use std::io::Read;

use geo::{Coord, Rect};

use crate::adj::AdjacencyMatrix;
use crate::dcel::{Dcel, Face, FaceId, HalfEdge, HalfEdgeId, Vertex, VertexId};
use crate::region::cache::{compute_bounds, compute_centroids, compute_exterior_boundary_length, compute_is_exterior};
use crate::region::Region;
use crate::rtree::SpatialIndex;
use crate::unit::UnitId;

use super::{IoError, MAGIC, VERSION};

const NONE_U32: u32 = 0xFFFF_FFFF;
const COORD_SCALE: f64 = 1e7;

fn read_u32(r: &mut impl Read) -> std::io::Result<u32> { let mut b = [0u8; 4]; r.read_exact(&mut b)?; Ok(u32::from_le_bytes(b)) }
fn read_i32(r: &mut impl Read) -> std::io::Result<i32> { let mut b = [0u8; 4]; r.read_exact(&mut b)?; Ok(i32::from_le_bytes(b)) }
fn read_f64(r: &mut impl Read) -> std::io::Result<f64> { let mut b = [0u8; 8]; r.read_exact(&mut b)?; Ok(f64::from_bits(u64::from_le_bytes(b))) }
fn decode_coord(v: i32) -> f64 { v as f64 / COORD_SCALE }

/// Deserialise a [`Region`] from `reader`.
///
/// The counterpart to [`crate::io::write`]. Reads DCEL topology, adjacency
/// CSRs, and pre-cached `area`/`perimeter`/edge-lengths from disk; recomputes
/// all other cached fields (centroids, bounds, `is_exterior`,
/// `exterior_boundary_length`, rook adjacency weights) from the DCEL on load.
///
/// See the [`crate::io`] module for the full file format.
///
/// # Errors
///
/// Returns [`IoError`] if the magic bytes are wrong, the version is
/// unsupported, or an I/O error occurs.
pub fn read(reader: &mut impl Read) -> Result<Region, IoError> {
    // ---- Header ----
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC { return Err(IoError::InvalidMagic) }
    let mut vr = [0u8; 4]; // version + 3 reserved
    reader.read_exact(&mut vr)?;
    if vr[0] != VERSION { return Err(IoError::UnsupportedVersion(vr[0])) }
    let num_vertices   = read_u32(reader)? as usize;
    let num_half_edges = read_u32(reader)? as usize;
    let num_faces      = read_u32(reader)? as usize;
    let num_units      = read_u32(reader)? as usize;

    if !num_half_edges.is_multiple_of(2) {
        return Err(IoError::InvalidData("num_half_edges must be even".into()));
    }

    // ---- Vertices ----
    let mut vertices: Vec<Vertex<Coord<f64>>> = Vec::with_capacity(num_vertices);
    for _ in 0..num_vertices {
        let lon = decode_coord(read_i32(reader)?);
        let lat = decode_coord(read_i32(reader)?);
        vertices.push(Vertex { coords: Coord { x: lon, y: lat }, half_edge: None });
    }

    // ---- HalfEdges ----
    let mut half_edges: Vec<HalfEdge> = Vec::with_capacity(num_half_edges);
    for e in 0..num_half_edges {
        let origin = read_u32(reader)?;
        let next   = read_u32(reader)?;
        let prev   = read_u32(reader)?;
        let face   = read_u32(reader)?;
        // twin is not stored; always h ^ 1 by construction.

        if (origin as usize) >= num_vertices || (next as usize) >= num_half_edges || (prev as usize) >= num_half_edges || (face as usize) >= num_faces {
            return Err(IoError::InvalidData(format!("half-edge {e}: index out of range")));
        }

        half_edges.push(HalfEdge {
            origin: VertexId(origin),
            next:   HalfEdgeId(next),
            prev:   HalfEdgeId(prev),
            face:   FaceId(face),
        });
    }

    // Back-fill vertex → half-edge pointers.
    for (e, half_edge) in half_edges.iter().enumerate() {
        let v = half_edge.origin.0 as usize;
        if vertices[v].half_edge.is_none() {
            vertices[v].half_edge = Some(HalfEdgeId(e as u32));
        }
    }

    // ---- Faces ----
    let mut faces: Vec<Face> = Vec::with_capacity(num_faces);
    for _ in 0..num_faces {
        let raw = read_u32(reader)?;
        let half_edge = if raw == NONE_U32 {
            None
        } else {
            if (raw as usize) >= num_half_edges {
                return Err(IoError::InvalidData("face half_edge out of range".into()));
            }
            Some(HalfEdgeId(raw))
        };
        faces.push(Face { half_edge });
    }

    // ---- FaceToUnit ----
    let mut face_to_unit: Vec<UnitId> = Vec::with_capacity(num_faces);
    for _ in 0..num_faces {
        let raw = read_u32(reader)?;
        face_to_unit.push(if raw == NONE_U32 {
            UnitId::EXTERIOR
        } else {
            if raw as usize >= num_units {
                return Err(IoError::InvalidData("face_to_unit index out of range".into()));
            }
            UnitId(raw)
        });
    }

    // ---- UnitCache ----
    let mut area      = Vec::with_capacity(num_units);
    let mut perimeter = Vec::with_capacity(num_units);
    for _ in 0..num_units {
        area.push(read_f64(reader)?);
        perimeter.push(read_f64(reader)?);
    }

    // ---- EdgeLengths ----
    let num_edges = num_half_edges / 2;
    let mut edge_length = Vec::with_capacity(num_edges);
    for _ in 0..num_edges {
        edge_length.push(read_f64(reader)?);
    }

    // ---- Adjacency CSR ----
    // Read the stored Rook CSR.  It may contain forced pairs (island bridges)
    // that are not present in the DCEL geometry; those must be preserved.
    let adjacent_stored = read_csr(reader, num_units)?;
    let touching = read_csr(reader, num_units)?;

    // ---- Rebuild DCEL ----
    let dcel = Dcel { vertices, half_edges, faces };

    // ---- Rebuild Rook adjacency: DCEL-derived weights + forced pairs from stored CSR ----
    // Build the natural adjacency from the DCEL (correct shared-boundary weights).
    let adjacent_natural = crate::region::adj::build_adjacent(&dcel, &face_to_unit, &edge_length, num_units);
    // Any pair in the stored CSR that is absent from the DCEL-derived matrix is a
    // forced (island-bridge) pair.  Add those with weight 0.0.
    let natural = &adjacent_natural;
    let forced_pairs: Vec<(UnitId, UnitId)> = (0..num_units as u32)
        .flat_map(|u| {
            let uid = UnitId(u);
            adjacent_stored.neighbors(uid).iter()
                .filter(move |&&v| !natural.contains(uid, v))
                .map(move |&v| (uid, v))
                .collect::<Vec<_>>()
        })
        .collect();
    let adjacent = adjacent_natural.with_extra_edges(&forced_pairs);

    // ---- Derive remaining cache fields ----
    let exterior_boundary_length =
        compute_exterior_boundary_length(&dcel, &face_to_unit, &edge_length, num_units);
    let centroid = compute_centroids(&dcel, &face_to_unit, num_units);
    let bounds = compute_bounds(&dcel, &face_to_unit, num_units);
    let bounds_all  = {
        let mut rect = bounds[0];
        for b in &bounds[1..] {
            rect = Rect::new(
                Coord { x: rect.min().x.min(b.min().x), y: rect.min().y.min(b.min().y) },
                Coord { x: rect.max().x.max(b.max().x), y: rect.max().y.max(b.max().y) },
            );
        }
        rect
    };
    let is_exterior = compute_is_exterior(&dcel, &face_to_unit, num_units);
    let geometries = crate::region::build::reconstruct_geometries(&dcel, &face_to_unit, num_units);
    let rtree = SpatialIndex::new(&bounds);
    let (unit_to_faces_offsets, unit_to_faces_data) = crate::region::build::compute_unit_to_faces(&face_to_unit, num_units);
    let face_inner_cycles = crate::region::build::compute_face_inner_cycles(&dcel);

    let region = Region {
        dcel,
        face_to_unit,
        geometries,
        area,
        perimeter,
        exterior_boundary_length,
        centroid,
        bounds,
        bounds_all,
        is_exterior,
        edge_length,
        adjacent,
        touching,
        rtree,
        unit_to_faces_offsets,
        unit_to_faces_data,
        face_inner_cycles,
    };

    {
        let mb = |b: usize| b as f64 / 1_048_576.0;
        let breakdown = region.heap_bytes_breakdown();
        let total: usize = breakdown.iter().map(|(_, b)| b).sum();
        eprintln!("[geograph::io] Region heap ({} units, {:.0}M half-edges):",
            region.num_units(), region.dcel.num_half_edges() as f64 / 1e6);
        for (label, bytes) in &breakdown {
            eprintln!("  {:<20} {:>6.1} MB", label, mb(*bytes));
        }
        eprintln!("  {:<20} {:>6.1} MB  (estimated)", "TOTAL", mb(total));
    }

    Ok(region)
}

// ---------------------------------------------------------------------------
// CSR helper
// ---------------------------------------------------------------------------

fn read_csr(reader: &mut impl Read, num_units: usize) -> Result<AdjacencyMatrix, IoError> {
    let mut offsets = Vec::with_capacity(num_units + 1);
    for _ in 0..=num_units {
        offsets.push(read_u32(reader)?);
    }
    let n_neighbors = *offsets.last().unwrap() as usize;
    // Read neighbor values guided by the stored offsets, then rebuild the CSR
    // via from_directed_pairs (sort + dedup).  The stored offsets are used only
    // to know how many neighbors to read per unit; the output CSR offsets are
    // recomputed from the pairs, which normalises any ordering differences.
    let mut pairs: Vec<(UnitId, UnitId)> = Vec::with_capacity(n_neighbors);
    for u in 0..num_units {
        let start = offsets[u]   as usize;
        let end   = offsets[u+1] as usize;
        for _ in start..end {
            let nb = read_u32(reader)?;
            pairs.push((UnitId(u as u32), UnitId(nb)));
        }
    }
    Ok(AdjacencyMatrix::from_directed_pairs(num_units, pairs))
}

