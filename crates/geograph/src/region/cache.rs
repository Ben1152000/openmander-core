use geo::{Coord, Rect};

use crate::dcel::{Dcel, FaceId, HalfEdgeId};
use crate::unit::UnitId;

const M_PER_DEG: f64 = 111_320.0;

pub(crate) struct CacheData {
    pub(crate) edge_length:              Vec<f64>,
    pub(crate) area:                     Vec<f64>,
    pub(crate) perimeter:                Vec<f64>,
    pub(crate) exterior_boundary_length: Vec<f64>,
    pub(crate) centroid:                 Vec<Coord<f64>>,
    pub(crate) bounds:                   Vec<Rect<f64>>,
    pub(crate) bounds_all:               Rect<f64>,
    pub(crate) is_exterior:              Vec<bool>,
}

pub(crate) fn compute_caches(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
    t0: std::time::Instant,
) -> CacheData {
    let num_half_edges = dcel.num_half_edges();

    // 4a. Edge lengths (one per undirected edge, indexed by he.0 / 2).
    let num_edges = num_half_edges / 2;
    let edge_length: Vec<f64> = (0..num_edges)
        .map(|e| {
            let half_edge = dcel.half_edge(HalfEdgeId((e * 2) as u32));
            let c0 = dcel.vertex(half_edge.origin).coords;
            let c1 = dcel.vertex(dcel.dest(HalfEdgeId((e * 2) as u32))).coords;
            edge_length_m(c0, c1)
        })
        .collect();

    eprintln!("[region::new] 4a. edge lengths computed in {:.2?}", t0.elapsed());

    // 4b. Per-unit area (shoelace with cos(φ_mid) correction).
    let mut area: Vec<f64> = vec![0.0; num_units];
    for (f, &unit) in face_to_unit.iter().enumerate() {
        if unit == UnitId::EXTERIOR { continue; }
        let start = match dcel.face(FaceId(f as u32)).half_edge {
            Some(he) => he,
            None => continue,
        };
        let mut face_area = 0.0;
        for he in dcel.face_cycle(start) {
            let c0 = dcel.vertex(dcel.half_edge(he).origin).coords;
            let c1 = dcel.vertex(dcel.dest(he)).coords;
            let phi_mid = (c0.y + c1.y) / 2.0 * std::f64::consts::PI / 180.0;
            let shoelace = c0.x * c1.y - c1.x * c0.y;
            face_area += shoelace * phi_mid.cos();
        }
        face_area = face_area.abs() / 2.0 * M_PER_DEG * M_PER_DEG;
        area[unit.0 as usize] += face_area;
    }

    eprintln!("[region::new] 4b. area computed in {:.2?}", t0.elapsed());

    // Pass A: single scan over all half-edges accumulates perimeter,
    // exterior_boundary_length, and is_exterior together.
    let mut perimeter               = vec![0.0f64; num_units];
    let mut exterior_boundary_length = vec![0.0f64; num_units];
    let mut is_exterior             = vec![false; num_units];
    for e in 0..num_half_edges {
        let half_edge  = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        let twin_unit = face_to_unit[dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face.0 as usize];
        if twin_unit != unit {
            perimeter[unit.0 as usize] += edge_length[e / 2];
            if twin_unit == UnitId::EXTERIOR {
                exterior_boundary_length[unit.0 as usize] += edge_length[e / 2];
                is_exterior[unit.0 as usize] = true;
            }
        }
    }

    eprintln!("[region::new] 4c-4h. perimeter/ext-boundary/is_exterior computed in {:.2?}", t0.elapsed());

    // Pass B: single scan over all half-edges accumulates centroid sums and bounds.
    let mut sum_x: Vec<f64> = vec![0.0; num_units];
    let mut sum_y: Vec<f64> = vec![0.0; num_units];
    let mut count: Vec<u32> = vec![0; num_units];
    let inf = f64::INFINITY;
    let mut min_x = vec![ inf; num_units];
    let mut min_y = vec![ inf; num_units];
    let mut max_x = vec![-inf; num_units];
    let mut max_y = vec![-inf; num_units];
    for e in 0..num_half_edges {
        let half_edge = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        let c = dcel.vertex(half_edge.origin).coords;
        let u = unit.0 as usize;
        sum_x[u] += c.x;
        sum_y[u] += c.y;
        count[u] += 1;
        if c.x < min_x[u] { min_x[u] = c.x; }
        if c.y < min_y[u] { min_y[u] = c.y; }
        if c.x > max_x[u] { max_x[u] = c.x; }
        if c.y > max_y[u] { max_y[u] = c.y; }
    }
    let centroid: Vec<Coord<f64>> = (0..num_units).map(|u| {
        if count[u] == 0 {
            Coord { x: 0.0, y: 0.0 }
        } else {
            Coord {
                x: sum_x[u] / count[u] as f64,
                y: sum_y[u] / count[u] as f64,
            }
        }
    }).collect();
    let bounds: Vec<Rect<f64>> = (0..num_units).map(|u| {
        let (mnx, mny) = if min_x[u].is_finite() { (min_x[u], min_y[u]) } else { (0.0, 0.0) };
        let (mxx, mxy) = if max_x[u].is_finite() { (max_x[u], max_y[u]) } else { (0.0, 0.0) };
        Rect::new(Coord { x: mnx, y: mny }, Coord { x: mxx, y: mxy })
    }).collect();

    eprintln!("[region::new] 4e-4f. centroid/bounds computed in {:.2?}", t0.elapsed());

    // 4g. Region-wide bounding box.
    let bounds_all = {
        let mut rect = bounds[0];
        for b in &bounds[1..] {
            rect = Rect::new(
                Coord {
                    x: rect.min().x.min(b.min().x),
                    y: rect.min().y.min(b.min().y),
                },
                Coord {
                    x: rect.max().x.max(b.max().x),
                    y: rect.max().y.max(b.max().y),
                },
            );
        }
        rect
    };

    eprintln!("[region::new] 4g. bounds_all computed in {:.2?}", t0.elapsed());

    CacheData { edge_length, area, perimeter, exterior_boundary_length, centroid, bounds, bounds_all, is_exterior }
}

/// Edge length in metres using the per-edge cos(φ_mid) correction.
///
/// Formula: `√(Δlat² + (Δlon·cos(φ_mid))²) × 111_320`
#[inline]
fn edge_length_m(c0: Coord<f64>, c1: Coord<f64>) -> f64 {
    let dlat = c1.y - c0.y;
    let dlon = c1.x - c0.x;
    let phi_mid = (c0.y + c1.y) / 2.0 * std::f64::consts::PI / 180.0;
    let dx = dlon * phi_mid.cos();
    (dlat * dlat + dx * dx).sqrt() * M_PER_DEG
}

// ---------------------------------------------------------------------------
// Shared cache derivation helpers (also used by io::read)
// ---------------------------------------------------------------------------

pub(crate) fn compute_exterior_boundary_length(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    edge_length: &[f64],
    num_units: usize,
) -> Vec<f64> {
    let mut ext = vec![0.0f64; num_units];
    for e in 0..dcel.num_half_edges() {
        let half_edge = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        if face_to_unit[dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face.0 as usize] == UnitId::EXTERIOR {
            ext[unit.0 as usize] += edge_length[e / 2];
        }
    }
    ext
}

pub(crate) fn compute_centroids(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<Coord<f64>> {
    let mut sum_x: Vec<f64> = vec![0.0; num_units];
    let mut sum_y: Vec<f64> = vec![0.0; num_units];
    let mut count: Vec<u32> = vec![0; num_units];

    for e in 0..dcel.num_half_edges() {
        let half_edge = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        let coord = dcel.vertex(half_edge.origin).coords;
        let u = unit.0 as usize;
        sum_x[u] += coord.x;
        sum_y[u] += coord.y;
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

pub(crate) fn compute_bounds(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<Rect<f64>> {
    let inf = f64::INFINITY;
    let mut min_x = vec![ inf; num_units];
    let mut min_y = vec![ inf; num_units];
    let mut max_x = vec![-inf; num_units];
    let mut max_y = vec![-inf; num_units];

    for e in 0..dcel.num_half_edges() {
        let half_edge = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        let coord = dcel.vertex(half_edge.origin).coords;
        let u = unit.0 as usize;
        if coord.x < min_x[u] { min_x[u] = coord.x; }
        if coord.y < min_y[u] { min_y[u] = coord.y; }
        if coord.x > max_x[u] { max_x[u] = coord.x; }
        if coord.y > max_y[u] { max_y[u] = coord.y; }
    }

    (0..num_units).map(|u| {
        let (mnx, mny) = if min_x[u].is_finite() { (min_x[u], min_y[u]) } else { (0.0, 0.0) };
        let (mxx, mxy) = if max_x[u].is_finite() { (max_x[u], max_y[u]) } else { (0.0, 0.0) };
        Rect::new(Coord { x: mnx, y: mny }, Coord { x: mxx, y: mxy })
    }).collect()
}

pub(crate) fn compute_is_exterior(
    dcel: &Dcel<Coord<f64>>,
    face_to_unit: &[UnitId],
    num_units: usize,
) -> Vec<bool> {
    let mut flags = vec![false; num_units];
    for e in 0..dcel.num_half_edges() {
        let half_edge = dcel.half_edge(HalfEdgeId(e as u32));
        let unit = face_to_unit[half_edge.face.0 as usize];
        if unit == UnitId::EXTERIOR { continue; }
        if face_to_unit[dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face.0 as usize] == UnitId::EXTERIOR {
            flags[unit.0 as usize] = true;
        }
    }
    flags
}
