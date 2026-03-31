mod read;
mod write;

pub use read::read;
pub use write::write;

/// Magic bytes at the start of every geograph binary file.
pub const MAGIC: &[u8; 4] = b"OMRP";

/// Current file format version.
pub const VERSION: u8 = 1;

/// Errors that can occur during serialisation or deserialisation.
#[derive(Debug)]
pub enum IoError {
    Io(std::io::Error),
    /// File does not start with the expected magic bytes.
    InvalidMagic,
    /// File was written by a newer or incompatible version.
    UnsupportedVersion(u8),
    /// File contents are structurally invalid.
    InvalidData(String),
}

impl From<std::io::Error> for IoError {
    #[inline]
    fn from(e: std::io::Error) -> Self { IoError::Io(e) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::dcel::VertexId;
    use crate::region::test_helpers::make_two_unit_region;

    use super::{read, write, IoError};

    fn round_trip(r: &crate::region::Region) -> crate::region::Region {
        let mut buf = Vec::new();
        write(r, &mut buf).expect("write failed");
        read(&mut buf.as_slice()).expect("read failed")
    }

    // -----------------------------------------------------------------------
    // Round-trip correctness
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_preserves_unit_count() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        assert_eq!(r2.num_units(), r.num_units());
    }

    #[test]
    fn round_trip_preserves_vertex_count() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        assert_eq!(r2.dcel.num_vertices(), r.dcel.num_vertices());
    }

    #[test]
    fn round_trip_preserves_half_edge_count() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        assert_eq!(r2.dcel.num_half_edges(), r.dcel.num_half_edges());
    }

    #[test]
    fn round_trip_preserves_rook_adjacency() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            assert_eq!(r.neighbors(uid), r2.neighbors(uid),
                "rook neighbors of {uid} changed");
        }
    }

    #[test]
    fn round_trip_preserves_queen_adjacency() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            assert_eq!(r.touching().neighbors(uid), r2.touching().neighbors(uid),
                "queen neighbors of {uid} changed");
        }
    }

    #[test]
    fn round_trip_preserves_area_cache() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            assert_eq!(r.area(uid), r2.area(uid));
        }
    }

    #[test]
    fn round_trip_preserves_perimeter_cache() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            assert_eq!(r.perimeter(uid), r2.perimeter(uid));
        }
    }

    #[test]
    fn round_trip_preserves_edge_lengths() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        assert_eq!(r.edge_length, r2.edge_length);
    }

    #[test]
    fn round_trip_preserves_face_to_unit() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        assert_eq!(r.face_to_unit, r2.face_to_unit);
    }

    #[test]
    fn round_trip_derived_is_exterior_is_consistent() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            assert_eq!(r.is_exterior(uid), r2.is_exterior(uid));
        }
    }

    #[test]
    fn round_trip_derived_exterior_boundary_length_is_consistent() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for uid in r.unit_ids() {
            let orig = r.exterior_boundary_length(uid);
            let rt   = r2.exterior_boundary_length(uid);
            assert!((orig - rt).abs() < 1e-9,
                "exterior_boundary_length mismatch for {uid}: {orig} vs {rt}");
        }
    }

    #[test]
    fn round_trip_vertex_coordinates_preserved() {
        let r  = make_two_unit_region();
        let r2 = round_trip(&r);
        for v in 0..r.dcel.num_vertices() {
            let c1 = r .dcel.vertex(VertexId(v as u32)).coords;
            let c2 = r2.dcel.vertex(VertexId(v as u32)).coords;
            assert!((c1.x - c2.x).abs() < 1e-7 && (c1.y - c2.y).abs() < 1e-7,
                "vertex {v}: ({:.9},{:.9}) → ({:.9},{:.9})", c1.x, c1.y, c2.x, c2.y);
        }
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn invalid_magic_returns_error() {
        let mut buf = b"WXYZ\x01\x00\x00\x00".to_vec();
        buf.extend(std::iter::repeat(0u8).take(64));
        match read(&mut buf.as_slice()) {
            Err(IoError::InvalidMagic) => {}
            Err(e) => panic!("expected InvalidMagic, got {:?}", e),
            Ok(_)  => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn unsupported_version_returns_error() {
        let mut buf = b"OMRP".to_vec();
        buf.push(99); // version = 99
        buf.extend([0u8; 3]); // reserved
        buf.extend([0u8; 16]); // counts (all zero)
        buf.extend(0u32.to_le_bytes()); // rook offsets[0]
        buf.extend(0u32.to_le_bytes()); // queen offsets[0]
        match read(&mut buf.as_slice()) {
            Err(IoError::UnsupportedVersion(99)) => {}
            Err(e) => panic!("expected UnsupportedVersion(99), got {:?}", e),
            Ok(_)  => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn truncated_file_returns_io_error() {
        let buf = b"OMRP\x01\x00\x00\x00"; // magic + version only
        match read(&mut buf.as_slice()) {
            Err(IoError::Io(_)) => {}
            Err(e) => panic!("expected Io error, got {:?}", e),
            Ok(_)  => panic!("expected error, got Ok"),
        }
    }
}
