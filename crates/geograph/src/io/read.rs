use std::io::Read;

use geo::{Coord, LineString, MultiPolygon, Polygon, Rect};

use crate::adj::AdjacencyMatrix;
use crate::dcel::{Dcel, Face, FaceId, HalfEdge, HalfEdgeId, Vertex, VertexId};
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

/// Deserialise a `Region` from `reader`.
///
/// See §8 (Serialisation) of DESIGN.md for the full file layout.
pub fn read(reader: &mut impl Read) -> Result<Region, IoError> {
    // ---- Header ----
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC { return Err(IoError::InvalidMagic) }
    let mut vr = [0u8; 4]; // version + 3 reserved
    reader.read_exact(&mut vr)?;
    if vr[0] != VERSION { return Err(IoError::UnsupportedVersion(vr[0])) }
    let nv  = read_u32(reader)? as usize;
    let nhe = read_u32(reader)? as usize;
    let nf  = read_u32(reader)? as usize;
    let nu  = read_u32(reader)? as usize;

    if nhe % 2 != 0 {
        return Err(IoError::InvalidData("num_half_edges must be even".into()));
    }

    // ---- Vertices ----
    let mut vertices: Vec<Vertex<Coord<f64>>> = Vec::with_capacity(nv);
    for _ in 0..nv {
        let lon = decode_coord(read_i32(reader)?);
        let lat = decode_coord(read_i32(reader)?);
        vertices.push(Vertex { coords: Coord { x: lon, y: lat }, half_edge: None });
    }

    // ---- HalfEdges ----
    let mut half_edges: Vec<HalfEdge> = Vec::with_capacity(nhe);
    for h in 0..nhe {
        let origin = read_u32(reader)? as usize;
        let next   = read_u32(reader)? as usize;
        let prev   = read_u32(reader)? as usize;
        let face   = read_u32(reader)? as usize;
        let twin   = h ^ 1;

        if origin >= nv || next >= nhe || prev >= nhe || face >= nf {
            return Err(IoError::InvalidData(format!("half-edge {h}: index out of range")));
        }

        half_edges.push(HalfEdge {
            origin: VertexId(origin),
            twin:   HalfEdgeId(twin),
            next:   HalfEdgeId(next),
            prev:   HalfEdgeId(prev),
            face:   FaceId(face),
        });
    }

    // Back-fill vertex → half-edge pointers.
    for h in 0..nhe {
        let v = half_edges[h].origin.0;
        if vertices[v].half_edge.is_none() {
            vertices[v].half_edge = Some(HalfEdgeId(h));
        }
    }

    // ---- Faces ----
    let mut faces: Vec<Face> = Vec::with_capacity(nf);
    for _ in 0..nf {
        let raw = read_u32(reader)?;
        let he  = if raw == NONE_U32 {
            None
        } else {
            if raw as usize >= nhe {
                return Err(IoError::InvalidData("face half_edge out of range".into()));
            }
            Some(HalfEdgeId(raw as usize))
        };
        faces.push(Face { half_edge: he });
    }

    // ---- FaceToUnit ----
    let mut face_to_unit: Vec<UnitId> = Vec::with_capacity(nf);
    for _ in 0..nf {
        let raw = read_u32(reader)?;
        face_to_unit.push(if raw == NONE_U32 {
            UnitId::EXTERIOR
        } else {
            if raw as usize >= nu {
                return Err(IoError::InvalidData("face_to_unit index out of range".into()));
            }
            UnitId(raw)
        });
    }

    // ---- UnitCache ----
    let mut area      = Vec::with_capacity(nu);
    let mut perimeter = Vec::with_capacity(nu);
    for _ in 0..nu {
        area.push(read_f64(reader)?);
        perimeter.push(read_f64(reader)?);
    }

    // ---- EdgeLengths ----
    let n_edges = nhe / 2;
    let mut edge_length = Vec::with_capacity(n_edges);
    for _ in 0..n_edges {
        edge_length.push(read_f64(reader)?);
    }

    // ---- Adjacency CSR ----
    // Read the stored Rook CSR.  It may contain forced pairs (island bridges)
    // that are not present in the DCEL geometry; those must be preserved.
    let adjacent_stored = read_csr(reader, nu)?;
    let touching = read_csr(reader, nu)?;

    // ---- Rebuild DCEL ----
    let dcel = Dcel { vertices, half_edges, faces };

    // ---- Rebuild Rook adjacency: DCEL-derived weights + forced pairs from stored CSR ----
    // Build the natural adjacency from the DCEL (correct shared-boundary weights).
    let adjacent_natural = crate::region::adj::build_adjacent(&dcel, &face_to_unit, &edge_length, nu);
    // Any pair in the stored CSR that is absent from the DCEL-derived matrix is a
    // forced (island-bridge) pair.  Add those with weight 0.0.
    let forced_pairs: Vec<(UnitId, UnitId)> = (0..nu as u32)
        .flat_map(|u| {
            let uid = UnitId(u);
            adjacent_stored.neighbors(uid).iter()
                .filter(|&&v| !adjacent_natural.contains(uid, v))
                .map(move |&v| (uid, v))
                .collect::<Vec<_>>()
        })
        .collect();
    let adjacent = adjacent_natural.with_extra_edges(&forced_pairs);

    // ---- Derive remaining cache fields ----
    let exterior_boundary_length =
        compute_exterior_boundary_length(&dcel, &face_to_unit, &edge_length, nu);
    let centroid    = compute_centroids(&dcel, &face_to_unit, nu);
    let bounds      = compute_bounds(&dcel, &face_to_unit, nu);
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
    let is_exterior = compute_is_exterior(&dcel, &face_to_unit, nu);
    let geometries  = reconstruct_geometries(&dcel, &face_to_unit, nu);
    let rtree       = SpatialIndex::new(&bounds);
    let unit_to_faces = crate::region::build::compute_unit_to_faces(&face_to_unit, nu);

    Ok(Region {
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
        unit_to_faces,
    })
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

// ---------------------------------------------------------------------------
// Cache derivation helpers
// ---------------------------------------------------------------------------

fn compute_exterior_boundary_length(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    edge_length: &[f64],
    num_units: usize,
) -> Vec<f64> {
    let mut ext = vec![0.0f64; num_units];
    for h in 0..dcel.num_half_edges() {
        let he   = dcel.half_edge(HalfEdgeId(h));
        let unit = face_to_unit[he.face.0];
        if unit == UnitId::EXTERIOR { continue; }
        if face_to_unit[dcel.half_edge(he.twin).face.0] == UnitId::EXTERIOR {
            ext[unit.0 as usize] += edge_length[h / 2];
        }
    }
    ext
}

fn compute_centroids(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<Coord<f64>> {
    let mut sum_x = vec![0.0f64; num_units];
    let mut sum_y = vec![0.0f64; num_units];
    let mut count = vec![0u32; num_units];

    for h in 0..dcel.num_half_edges() {
        let he   = dcel.half_edge(HalfEdgeId(h));
        let unit = face_to_unit[he.face.0];
        if unit == UnitId::EXTERIOR { continue; }
        let c = dcel.vertex(he.origin).coords;
        let u = unit.0 as usize;
        sum_x[u] += c.x;
        sum_y[u] += c.y;
        count[u] += 1;
    }

    (0..num_units).map(|u| {
        if count[u] == 0 {
            Coord { x: 0.0, y: 0.0 }
        } else {
            Coord {
                x: sum_x[u] / count[u] as f64,
                y: sum_y[u] / count[u] as f64,
            }
        }
    }).collect()
}

fn compute_bounds(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<Rect<f64>> {
    let inf = f64::INFINITY;
    let mut min_x = vec![ inf; num_units];
    let mut min_y = vec![ inf; num_units];
    let mut max_x = vec![-inf; num_units];
    let mut max_y = vec![-inf; num_units];

    for h in 0..dcel.num_half_edges() {
        let he   = dcel.half_edge(HalfEdgeId(h));
        let unit = face_to_unit[he.face.0];
        if unit == UnitId::EXTERIOR { continue; }
        let c = dcel.vertex(he.origin).coords;
        let u = unit.0 as usize;
        if c.x < min_x[u] { min_x[u] = c.x; }
        if c.y < min_y[u] { min_y[u] = c.y; }
        if c.x > max_x[u] { max_x[u] = c.x; }
        if c.y > max_y[u] { max_y[u] = c.y; }
    }

    (0..num_units).map(|u| {
        let (mnx, mny) = if min_x[u].is_finite() { (min_x[u], min_y[u]) } else { (0.0, 0.0) };
        let (mxx, mxy) = if max_x[u].is_finite() { (max_x[u], max_y[u]) } else { (0.0, 0.0) };
        Rect::new(Coord { x: mnx, y: mny }, Coord { x: mxx, y: mxy })
    }).collect()
}

fn compute_is_exterior(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<bool> {
    let mut flags = vec![false; num_units];
    for h in 0..dcel.num_half_edges() {
        let he   = dcel.half_edge(HalfEdgeId(h));
        let unit = face_to_unit[he.face.0];
        if unit == UnitId::EXTERIOR { continue; }
        if face_to_unit[dcel.half_edge(he.twin).face.0] == UnitId::EXTERIOR {
            flags[unit.0 as usize] = true;
        }
    }
    flags
}

fn reconstruct_geometries(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<MultiPolygon<f64>> {
    let mut polys: Vec<Vec<Polygon<f64>>> = vec![Vec::new(); num_units];

    for f in 0..dcel.num_faces() {
        let unit = face_to_unit[f];
        if unit == UnitId::EXTERIOR { continue; }
        let start = match dcel.face(FaceId(f)).half_edge {
            Some(he) => he,
            None => continue,
        };
        let mut coords: Vec<Coord<f64>> = dcel
            .face_cycle(start)
            .map(|he| dcel.vertex(dcel.half_edge(he).origin).coords)
            .collect();
        if let Some(&first) = coords.first() { coords.push(first); }
        polys[unit.0 as usize].push(Polygon::new(LineString(coords), vec![]));
    }

    polys.into_iter().map(MultiPolygon).collect()
}
