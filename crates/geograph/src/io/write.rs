use std::io::Write;

use crate::adj::AdjacencyMatrix;
use crate::dcel::{FaceId, HalfEdgeId, VertexId};
use crate::region::Region;

use super::{IoError, MAGIC, VERSION};

const NONE_U32: u32 = 0xFFFF_FFFF;
const COORD_SCALE: f64 = 1e7;

fn write_u32(w: &mut impl Write, v: u32) -> std::io::Result<()> { w.write_all(&v.to_le_bytes()) }
fn write_i32(w: &mut impl Write, v: i32) -> std::io::Result<()> { w.write_all(&v.to_le_bytes()) }
fn write_f64(w: &mut impl Write, v: f64) -> std::io::Result<()> { w.write_all(&v.to_bits().to_le_bytes()) }
fn encode_coord(v: f64) -> i32 { (v * COORD_SCALE).round() as i32 }

/// Serialise a [`Region`] to `writer` using the geograph binary format.
///
/// The counterpart to [`crate::io::read`]. Writes DCEL topology, adjacency
/// CSRs (neighbor pairs only, not weights), and pre-cached
/// `area`/`perimeter`/edge-lengths; other cached fields (centroids, bounds,
/// `is_exterior`, `exterior_boundary_length`, rook adjacency weights) are
/// recomputed on load.
///
/// See the [`crate::io`] module for the full file format.
///
/// # Errors
///
/// Returns [`IoError`] if an I/O error occurs while writing.
pub fn write(region: &Region, writer: &mut impl Write) -> Result<(), IoError> {
    let num_vertices   = region.dcel.num_vertices() as u32;
    let num_half_edges = region.dcel.num_half_edges() as u32;
    let num_faces      = region.dcel.num_faces() as u32;
    let num_units      = region.num_units() as u32;

    // ---- Header ----
    writer.write_all(MAGIC)?;
    writer.write_all(&[VERSION, 0, 0, 0])?; // version + 3 reserved bytes
    write_u32(writer, num_vertices)?;
    write_u32(writer, num_half_edges)?;
    write_u32(writer, num_faces)?;
    write_u32(writer, num_units)?;

    // ---- Vertices ----
    for v in 0..num_vertices as usize {
        let c = region.dcel.vertex(VertexId(v as u32)).coords;
        write_i32(writer, encode_coord(c.x))?;
        write_i32(writer, encode_coord(c.y))?;
    }

    // ---- HalfEdges (no twin field — derived as id ^ 1) ----
    for e in 0..num_half_edges as usize {
        let half_edge = region.dcel.half_edge(HalfEdgeId(e as u32));
        write_u32(writer, half_edge.origin.0)?;
        write_u32(writer, half_edge.next.0)?;
        write_u32(writer, half_edge.prev.0)?;
        write_u32(writer, half_edge.face.0)?;
    }

    // ---- Faces ----
    for f in 0..num_faces as usize {
        let he_opt = region.dcel.face(FaceId(f as u32)).half_edge;
        write_u32(writer, he_opt.map_or(NONE_U32, |h| h.0))?;
    }

    // ---- FaceToUnit ----
    for f in 0..num_faces as usize {
        let uid = region.face_to_unit[f];
        use crate::unit::UnitId;
        write_u32(writer, if uid == UnitId::EXTERIOR { NONE_U32 } else { uid.0 })?;
    }

    // ---- UnitCache (area, perimeter per unit) ----
    for u in 0..num_units as usize {
        write_f64(writer, region.area[u])?;
        write_f64(writer, region.perimeter[u])?;
    }

    // ---- EdgeLengths (one entry per undirected edge) ----
    for &l in &region.edge_length {
        write_f64(writer, l)?;
    }

    // ---- Rook adjacency CSR ----
    write_csr(writer, region, region.adjacency())?;

    // ---- Queen adjacency CSR ----
    write_csr(writer, region, region.touching())?;

    Ok(())
}

fn write_csr(writer: &mut impl Write, region: &Region, matrix: &AdjacencyMatrix) -> Result<(), IoError> {
    let mut offset = 0u32;
    write_u32(writer, offset)?;
    for uid in region.unit_ids() {
        offset += matrix.neighbors(uid).len() as u32;
        write_u32(writer, offset)?;
    }
    for uid in region.unit_ids() {
        for &nb in matrix.neighbors(uid) {
            write_u32(writer, nb.0)?;
        }
    }
    Ok(())
}
