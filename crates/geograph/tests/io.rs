// Integration tests for geograph binary serialisation:
//   io::write, io::read, round-trip fidelity

use geo::{Coord, LineString, MultiPolygon, Polygon};
use geograph::Region;

fn rect_poly(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
    Polygon::new(
        LineString(vec![
            Coord { x: x0, y: y0 },
            Coord { x: x1, y: y0 },
            Coord { x: x1, y: y1 },
            Coord { x: x0, y: y1 },
            Coord { x: x0, y: y0 },
        ]),
        vec![],
    )
}

fn three_squares() -> Region {
    let geoms = vec![
        MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
        MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
    ];
    Region::new(geoms, 1e-7).expect("construction failed")
}

fn round_trip(r: &Region) -> Region {
    let mut buf = Vec::new();
    geograph::io::write(r, &mut buf).expect("write failed");
    geograph::io::read(&mut buf.as_slice()).expect("read failed")
}

#[test]
fn round_trip_preserves_unit_count() {
    let r = three_squares();
    let r2 = round_trip(&r);
    assert_eq!(r2.num_units(), r.num_units());
}

#[test]
fn round_trip_preserves_adjacency() {
    let r = three_squares();
    let r2 = round_trip(&r);
    for uid in r.unit_ids() {
        assert_eq!(
            r.neighbors(uid),
            r2.neighbors(uid),
            "Rook neighbors of {uid} changed after round-trip"
        );
        assert_eq!(
            r.touching().neighbors(uid),
            r2.touching().neighbors(uid),
            "Queen neighbors of {uid} changed after round-trip"
        );
    }
}

#[test]
fn round_trip_preserves_geometry_cache() {
    let r = three_squares();
    let r2 = round_trip(&r);
    for uid in r.unit_ids() {
        let a_orig = r.area(uid);
        let a_rt = r2.area(uid);
        assert!(
            (a_orig - a_rt).abs() < 1.0,
            "area mismatch for {uid}: {a_orig} vs {a_rt}"
        );
        let p_orig = r.perimeter(uid);
        let p_rt = r2.perimeter(uid);
        assert!(
            (p_orig - p_rt).abs() < 1.0,
            "perimeter mismatch for {uid}: {p_orig} vs {p_rt}"
        );
        assert_eq!(
            r.is_exterior(uid),
            r2.is_exterior(uid),
            "is_exterior mismatch for {uid}"
        );
    }
}

#[test]
fn invalid_magic_returns_error() {
    let mut buf = b"WXYZ\x01\x00\x00\x00".to_vec();
    buf.extend(std::iter::repeat(0u8).take(64));
    match geograph::io::read(&mut buf.as_slice()) {
        Err(geograph::io::IoError::InvalidMagic) => {}
        Err(e) => panic!("expected InvalidMagic, got {:?}", e),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn unsupported_version_returns_error() {
    let mut buf = b"OMRP".to_vec();
    buf.push(99);
    buf.extend([0u8; 3]);
    buf.extend([0u8; 16]);
    buf.extend(0u32.to_le_bytes());
    buf.extend(0u32.to_le_bytes());
    match geograph::io::read(&mut buf.as_slice()) {
        Err(geograph::io::IoError::UnsupportedVersion(99)) => {}
        Err(e) => panic!("expected UnsupportedVersion(99), got {:?}", e),
        Ok(_) => panic!("expected error, got Ok"),
    }
}
