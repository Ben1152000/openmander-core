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

/// Serialise `region` to `writer` using the geograph binary format.
///
/// See §8 (Serialisation) of DESIGN.md for the full file layout.
pub fn write(region: &Region, writer: &mut impl Write) -> Result<(), IoError> {
    let nv  = region.dcel.num_vertices()   as u32;
    let nhe = region.dcel.num_half_edges() as u32;
    let nf  = region.dcel.num_faces()      as u32;
    let nu  = region.num_units()           as u32;

    // ---- Header ----
    writer.write_all(MAGIC)?;
    writer.write_all(&[VERSION, 0, 0, 0])?; // version + 3 reserved bytes
    write_u32(writer, nv)?;
    write_u32(writer, nhe)?;
    write_u32(writer, nf)?;
    write_u32(writer, nu)?;

    // ---- Vertices ----
    for v in 0..nv as usize {
        let c = region.dcel.vertex(VertexId(v)).coords;
        write_i32(writer, encode_coord(c.x))?;
        write_i32(writer, encode_coord(c.y))?;
    }

    // ---- HalfEdges (no twin field — derived as id ^ 1) ----
    for h in 0..nhe as usize {
        let he = region.dcel.half_edge(HalfEdgeId(h));
        write_u32(writer, he.origin.0 as u32)?;
        write_u32(writer, he.next.0   as u32)?;
        write_u32(writer, he.prev.0   as u32)?;
        write_u32(writer, he.face.0   as u32)?;
    }

    // ---- Faces ----
    for f in 0..nf as usize {
        let he_opt = region.dcel.face(FaceId(f)).half_edge;
        write_u32(writer, he_opt.map_or(NONE_U32, |h| h.0 as u32))?;
    }

    // ---- FaceToUnit ----
    for f in 0..nf as usize {
        let uid = region.face_to_unit[f];
        use crate::unit::UnitId;
        write_u32(writer, if uid == UnitId::EXTERIOR { NONE_U32 } else { uid.0 })?;
    }

    // ---- UnitCache (area, perimeter per unit) ----
    for u in 0..nu as usize {
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
