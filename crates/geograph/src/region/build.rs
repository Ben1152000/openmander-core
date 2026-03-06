use ahash::AHashMap;

use geo::{Coord, MultiPolygon, Rect};

use crate::dcel::{Dcel, FaceId, HalfEdgeId, VertexId, OUTER_FACE};
use crate::rtree::SpatialIndex;
use crate::snap::snap_vertices;
use crate::unit::UnitId;

use super::Region;
use super::adj::{build_adjacent, build_touching};

/// Errors that can occur when constructing or validating a `Region`.
#[derive(Debug)]
pub enum RegionError {
    /// One or more input geometries are invalid or empty.
    InvalidGeometry(String),
    /// A structural invariant was violated (see `Region::validate()`).
    ValidationError(String),
}

/// Metres per degree of latitude (WGS-84 mean).
const M_PER_DEG: f64 = 111_320.0;

impl Region {
    /// Build a `Region` from a vector of `MultiPolygon` geometries (one per
    /// unit, in the order that determines `UnitId` assignment).
    ///
    /// `snap_tol`: if `Some(tol)`, near-coincident shared-boundary vertices are
    /// snapped to a canonical position before DCEL construction (useful for
    /// data with small floating-point artefacts).  Pass `None` for
    /// topologically clean data such as TIGER/Line GeoParquet (the shared
    /// boundary coordinates are already exactly equal, so snapping is a no-op
    /// and skipping it avoids the O(E) overhead).
    pub fn new(geometries: Vec<MultiPolygon<f64>>, snap_tol: Option<f64>) -> Result<Self, RegionError> {
        if geometries.is_empty() {
            return Err(RegionError::InvalidGeometry("no geometries provided".into()));
        }

        let num_units = geometries.len();

        // -----------------------------------------------------------------
        // 1. Extract rings and (optionally) snap vertices
        // -----------------------------------------------------------------
        // rings[unit] = vec of rings (outer + holes), each ring = vec of coords.
        // Convention: outer rings are CCW, hole rings are CW (GeoJSON / geo crate).
        let mut rings: Vec<Vec<Vec<Coord<f64>>>> = Vec::with_capacity(num_units);
        // ring_info[unit][ring_idx] = (polygon_idx, is_outer)
        let mut ring_info: Vec<Vec<(usize, bool)>> = Vec::with_capacity(num_units);

        for mp in &geometries {
            let mut unit_rings = Vec::new();
            let mut unit_info = Vec::new();
            for (pi, poly) in mp.0.iter().enumerate() {
                let outer = poly.exterior().0.clone();
                if outer.len() < 4 {
                    return Err(RegionError::InvalidGeometry(
                        "polygon ring must have at least 4 coordinates (including closing)".into(),
                    ));
                }
                unit_rings.push(outer);
                unit_info.push((pi, true));
                for hole in poly.interiors() {
                    unit_rings.push(hole.0.clone());
                    unit_info.push((pi, false));
                }
            }
            rings.push(unit_rings);
            ring_info.push(unit_info);
        }

        // Normalize winding order: exterior rings → CCW (positive signed area),
        // hole rings → CW (negative signed area).  TIGER/Shapefile data and some
        // GeoParquet exports use ESRI convention (CW exterior), which inverts the
        // face assignments and produces incorrect face cycles.
        for (u, unit_rings) in rings.iter_mut().enumerate() {
            for (ri, ring) in unit_rings.iter_mut().enumerate() {
                let sa = ring_signed_area(ring);
                let is_outer = ring_info[u][ri].1;
                let needs_reverse = if is_outer { sa < 0.0 } else { sa > 0.0 };
                if needs_reverse {
                    ring.reverse();
                }
            }
        }

        if let Some(tol) = snap_tol {
            snap_vertices(&mut rings, tol);
        }

        // -----------------------------------------------------------------
        // 2. Build DCEL
        // -----------------------------------------------------------------
        let mut dcel: Dcel<Coord<f64>> = Dcel::new();

        // 2a. Deduplicate vertices by exact coordinate (post-snap).
        let mut vertex_map: AHashMap<CoordKey, VertexId> = AHashMap::new();
        // ring_vids[unit][ring_idx][pos] = VertexId
        let mut ring_vids: Vec<Vec<Vec<VertexId>>> = Vec::with_capacity(num_units);

        for unit_rings in &rings {
            let mut unit_vids = Vec::with_capacity(unit_rings.len());
            for ring in unit_rings {
                let mut vids = Vec::with_capacity(ring.len());
                for &c in ring {
                    let key = CoordKey::from(c);
                    let vid = *vertex_map
                        .entry(key)
                        .or_insert_with(|| dcel.add_vertex(c));
                    vids.push(vid);
                }
                unit_vids.push(vids);
            }
            ring_vids.push(unit_vids);
        }

        // 2b. Create faces: one DCEL face per outer ring of each unit.
        //     Hole rings share the face of whatever unit fills them (determined
        //     by edge matching), or become EXTERIOR gap faces.
        //
        //     face_owner[face_id] = UnitId that owns this face.
        //     For outer rings: the unit itself.
        //     For faces created later (gaps): EXTERIOR.
        //
        //     Strategy: only create faces for outer rings now.  Hole ring edges
        //     will match against some other unit's outer ring edges (or become
        //     OUTER_FACE edges if unmatched).

        // ring_face[unit][ring_idx] = FaceId (only meaningful for outer rings)
        let mut ring_face: Vec<Vec<Option<FaceId>>> = Vec::with_capacity(num_units);
        let mut face_to_unit_vec: Vec<UnitId> = vec![UnitId::EXTERIOR]; // slot 0 = OUTER_FACE

        for (u, infos) in ring_info.iter().enumerate() {
            let mut unit_faces = Vec::with_capacity(infos.len());
            for &(_pi, is_outer) in infos {
                if is_outer {
                    let fid = dcel.add_face();
                    face_to_unit_vec.push(UnitId(u as u32));
                    unit_faces.push(Some(fid));
                } else {
                    unit_faces.push(None); // hole — face determined later
                }
            }
            ring_face.push(unit_faces);
        }

        // 2c. Collect all directed edges from all rings.
        //     For each directed edge (a→b) from an outer ring, we know its face.
        //     For each directed edge from a hole ring, the face to its left is
        //     the interior of the hole — we'll figure that out via matching.
        //
        //     edge_table: (a, b) → (unit, ring_idx, face_on_left)
        //     We only insert edges from outer rings.  When matching, the twin's
        //     face comes from the table; unmatched outer edges get OUTER_FACE.

        // Map packed_edge(origin, dest) → FaceId for outer ring edges.
        let mut edge_face: AHashMap<u64, FaceId> = AHashMap::new();

        for (u, unit_rings) in ring_vids.iter().enumerate() {
            for (ri, vids) in unit_rings.iter().enumerate() {
                let face = match ring_face[u][ri] {
                    Some(f) => f,
                    None => continue, // skip hole rings in this pass
                };
                let n = vids.len();
                if n < 2 { continue; }
                // Skip the closing coordinate (last == first).
                let edge_count = if vids[0] == vids[n - 1] { n - 1 } else { n };
                for i in 0..edge_count {
                    let a = vids[i];
                    let b = vids[(i + 1) % n];
                    if a == b { continue; } // degenerate edge
                    edge_face.insert(pack_edge(a, b), face);
                }
            }
        }

        // Also collect hole ring edges — their face is the interior of the hole.
        // If another unit's outer ring provides the reverse edge, the hole
        // interior is that unit's face.  Otherwise, it's a gap (EXTERIOR).
        // We need these to find the correct face for the twin side of
        // boundary edges that border a hole.
        //
        // hole_edges: (a, b) → (unit_idx, ring_idx)  [for reference only]
        let mut hole_edge_face: AHashMap<u64, FaceId> = AHashMap::new();

        for (u, unit_rings) in ring_vids.iter().enumerate() {
            for (ri, vids) in unit_rings.iter().enumerate() {
                if ring_face[u][ri].is_some() { continue; } // skip outer rings
                let n = vids.len();
                if n < 2 { continue; }
                let edge_count = if vids[0] == vids[n - 1] { n - 1 } else { n };
                for i in 0..edge_count {
                    let a = vids[i];
                    let b = vids[(i + 1) % n];
                    if a == b { continue; }
                    // The face to the left of a CW hole ring edge is the hole
                    // interior.  Check if the reverse edge exists as an outer
                    // ring edge — if so, the hole interior is that outer ring's
                    // unit.
                    if edge_face.contains_key(&pack_edge(b, a)) {
                        // The reverse of this hole edge is an outer ring edge
                        // with face `face`.  The hole interior edge (a→b) has
                        // the hole's face on its left.  But we also know this
                        // edge's reverse (b→a) has `face` on its left.  So
                        // a→b is the twin of b→a: they share an edge.  We'll
                        // handle this naturally during edge creation.
                        //
                        // But for the hole's own directed edge (a→b), the face
                        // to its left is the enclosing unit's face (the unit
                        // that has the hole).  We find that from ring_face:
                        // The hole belongs to the same polygon as the outer ring
                        // of unit u.  The enclosing face is the outer ring's face.
                        let (_pi, _) = ring_info[u][ri];
                        // Find the outer ring face for this polygon of unit u.
                        let outer_face = ring_info[u].iter()
                            .enumerate()
                            .find(|&(_, &(pi2, is_outer))| pi2 == _pi && is_outer)
                            .and_then(|(ri2, _)| ring_face[u][ri2])
                            .unwrap_or(OUTER_FACE);
                        hole_edge_face.insert(pack_edge(a, b), outer_face);
                    } else {
                        // No matching outer ring edge — this hole borders EXTERIOR.
                        // The face to the left of the hole edge is some gap face.
                        // We'll create gap faces later during the gap detection pass.
                        // For now, mark with OUTER_FACE.
                        let (_pi, _) = ring_info[u][ri];
                        let outer_face = ring_info[u].iter()
                            .enumerate()
                            .find(|&(_, &(pi2, is_outer))| pi2 == _pi && is_outer)
                            .and_then(|(ri2, _)| ring_face[u][ri2])
                            .unwrap_or(OUTER_FACE);
                        hole_edge_face.insert(pack_edge(a, b), outer_face);
                    }
                }
            }
        }

        // 2d. Create half-edge pairs in the DCEL.
        //     For each undirected edge {a, b}, determine face_left (face of a→b)
        //     and face_right (face of b→a).
        //
        //     Collect all unique undirected edges from the edge tables.

        let mut seen_edges: AHashMap<u64, HalfEdgeId> = AHashMap::new();

        // Process outer ring edges.
        for (u, unit_rings) in ring_vids.iter().enumerate() {
            for (ri, vids) in unit_rings.iter().enumerate() {
                let n = vids.len();
                if n < 2 { continue; }
                let is_outer = ring_face[u][ri].is_some();
                let edge_count = if vids[0] == vids[n - 1] { n - 1 } else { n };
                for i in 0..edge_count {
                    let a = vids[i];
                    let b = vids[(i + 1) % n];
                    if a == b { continue; }

                    // Skip if this directed edge or its reverse already created.
                    if seen_edges.contains_key(&pack_edge(a, b)) || seen_edges.contains_key(&pack_edge(b, a)) {
                        continue;
                    }

                    // Determine faces on each side.
                    let face_ab = if is_outer {
                        ring_face[u][ri].unwrap()
                    } else {
                        *hole_edge_face.get(&pack_edge(a, b)).unwrap_or(&OUTER_FACE)
                    };

                    let face_ba = edge_face.get(&pack_edge(b, a)).copied()
                        .or_else(|| hole_edge_face.get(&pack_edge(b, a)).copied())
                        .unwrap_or(OUTER_FACE);

                    let (he_ab, _he_ba) = dcel.add_edge(a, b, face_ab, face_ba);
                    seen_edges.insert(pack_edge(a, b), he_ab);
                }
            }
        }

        // Process hole ring edges that haven't been created yet.
        for unit_rings in &ring_vids {
            for vids in unit_rings {
                let n = vids.len();
                if n < 2 { continue; }
                let edge_count = if vids[0] == vids[n - 1] { n - 1 } else { n };
                for i in 0..edge_count {
                    let a = vids[i];
                    let b = vids[(i + 1) % n];
                    if a == b { continue; }
                    if seen_edges.contains_key(&pack_edge(a, b)) || seen_edges.contains_key(&pack_edge(b, a)) {
                        continue;
                    }

                    let face_ab = hole_edge_face.get(&pack_edge(a, b)).copied()
                        .unwrap_or(OUTER_FACE);
                    let face_ba = edge_face.get(&pack_edge(b, a)).copied()
                        .or_else(|| hole_edge_face.get(&pack_edge(b, a)).copied())
                        .unwrap_or(OUTER_FACE);

                    let (he_ab, _he_ba) = dcel.add_edge(a, b, face_ab, face_ba);
                    seen_edges.insert(pack_edge(a, b), he_ab);
                }
            }
        }

        // 2e-pre: Assign face.half_edge for every outer-ring face to an outer-ring
        // half-edge BEFORE step 2f has a chance to assign a hole-ring half-edge.
        //
        // For a donut unit (outer ring + hole ring), the face has TWO disconnected
        // cycles: the CCW outer ring and the CW hole ring.  step 2f picks the first
        // half-edge by index, which can be a hole-ring edge if the enclave unit
        // (whose outer ring creates the hole twins) has a lower unit index.  This
        // pass guarantees face.half_edge always points into the outer ring cycle.
        for (u, unit_rings) in ring_vids.iter().enumerate() {
            for (ri, vids) in unit_rings.iter().enumerate() {
                let face = match ring_face[u][ri] {
                    Some(f) => f,
                    None => continue, // hole rings have no dedicated face
                };
                if dcel.face(face).half_edge.is_some() { continue; }
                let n = vids.len();
                if n < 2 { continue; }
                let edge_count = if vids[0] == vids[n - 1] { n - 1 } else { n };
                'find_outer: for i in 0..edge_count {
                    let a = vids[i];
                    let b = vids[(i + 1) % n];
                    if a == b { continue; }
                    // Try the forward direction (outer ring processed this unit first).
                    if let Some(&he) = seen_edges.get(&pack_edge(a, b)) {
                        if dcel.half_edge(he).face == face {
                            dcel.face_mut(face).half_edge = Some(he);
                            break 'find_outer;
                        }
                    }
                    // Try the reverse direction (another unit processed b→a first;
                    // the twin a→b belongs to this face).
                    if let Some(&he_ba) = seen_edges.get(&pack_edge(b, a)) {
                        let he = dcel.half_edge(he_ba).twin;
                        if dcel.half_edge(he).face == face {
                            dcel.face_mut(face).half_edge = Some(he);
                            break 'find_outer;
                        }
                    }
                }
            }
        }

        // 2e. Set next/prev links using the CCW rotation rule at each vertex.
        //
        //     At vertex v with outgoing half-edges h_0, h_1, ..., h_{k-1}
        //     sorted CCW by angle to destination:
        //       h_i.twin.next = h_{(i-1) mod k}
        //       h_{(i-1) mod k}.prev = h_i.twin
        //
        //     This correctly links all face cycles (both interior and outer).

        // Collect outgoing half-edges per vertex.
        let num_vertices = dcel.num_vertices();
        let num_half_edges = dcel.num_half_edges();
        let mut outgoing: Vec<Vec<HalfEdgeId>> = vec![Vec::new(); num_vertices];

        for h in 0..num_half_edges {
            let origin = dcel.half_edge(HalfEdgeId(h)).origin.0;
            outgoing[origin].push(HalfEdgeId(h));
        }

        // Sort outgoing half-edges CCW at each vertex by angle to destination.
        for v in 0..num_vertices {
            let vc = dcel.vertex(VertexId(v)).coords;
            outgoing[v].sort_by(|&a, &b| {
                let da = dcel.vertex(dcel.dest(a)).coords;
                let db = dcel.vertex(dcel.dest(b)).coords;
                let angle_a = (da.y - vc.y).atan2(da.x - vc.x);
                let angle_b = (db.y - vc.y).atan2(db.x - vc.x);
                angle_a.partial_cmp(&angle_b).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Apply rotation rule: h_i.twin.next = h_{(i-1) mod k}
        for v in 0..num_vertices {
            let k = outgoing[v].len();
            if k == 0 { continue; }
            for i in 0..k {
                let h_i = outgoing[v][i];
                let h_prev = outgoing[v][(i + k - 1) % k];
                let twin_i = dcel.half_edge(h_i).twin;
                dcel.set_next(twin_i, h_prev);
            }
        }

        // 2f. Set face.half_edge pointers.
        for h in 0..num_half_edges {
            let face = dcel.half_edge(HalfEdgeId(h)).face;
            if dcel.face(face).half_edge.is_none() {
                dcel.face_mut(face).half_edge = Some(HalfEdgeId(h));
            }
        }

        // -----------------------------------------------------------------
        // 3. Gap detection: identify bounded OUTER_FACE cycles as gap faces
        // -----------------------------------------------------------------
        //     After linking, OUTER_FACE may contain multiple cycles:
        //     - The outer boundary of the region (unbounded, largest)
        //     - Interior gap cycles (bounded holes with no assigned unit)
        //
        //     Find all cycles on OUTER_FACE. The one with the most negative
        //     signed area (largest CW polygon) is the true outer boundary;
        //     all others become new EXTERIOR-assigned faces.

        let mut visited_he = vec![false; num_half_edges];
        let mut outer_cycles: Vec<(Vec<HalfEdgeId>, f64)> = Vec::new();

        for h in 0..num_half_edges {
            if visited_he[h] { continue; }
            if dcel.half_edge(HalfEdgeId(h)).face != OUTER_FACE { continue; }

            let mut cycle = Vec::new();
            let mut cur = HalfEdgeId(h);
            loop {
                if visited_he[cur.0] { break; }
                visited_he[cur.0] = true;
                cycle.push(cur);
                cur = dcel.half_edge(cur).next;
                if cur == HalfEdgeId(h) { break; }
            }

            // Compute signed area of this cycle (shoelace, in degrees²).
            let signed_area = signed_area_deg(&dcel, &cycle);
            outer_cycles.push((cycle, signed_area));
        }

        if outer_cycles.len() > 1 {
            // Find the true outer boundary (most negative signed area = largest CW cycle).
            let outer_idx = outer_cycles.iter()
                .enumerate()
                .min_by(|(_, (_, a)), (_, (_, b))| a.partial_cmp(b).unwrap())
                .map(|(i, _)| i)
                .unwrap();

            // All other cycles are gap faces.
            for (idx, (cycle, _)) in outer_cycles.iter().enumerate() {
                if idx == outer_idx { continue; }
                let gap_face = dcel.add_face();
                face_to_unit_vec.push(UnitId::EXTERIOR);
                for &he in cycle {
                    dcel.half_edge_mut(he).face = gap_face;
                }
                dcel.face_mut(gap_face).half_edge = Some(cycle[0]);
            }
        }

        let face_to_unit = face_to_unit_vec;

        // -----------------------------------------------------------------
        // 4. Cache pre-computation
        // -----------------------------------------------------------------

        // 4a. Edge lengths (one per undirected edge, indexed by he.0 / 2).
        let n_edges = num_half_edges / 2;
        let mut edge_length = vec![0.0f64; n_edges];
        for e in 0..n_edges {
            let he = dcel.half_edge(HalfEdgeId(e * 2));
            let c0 = dcel.vertex(he.origin).coords;
            let c1 = dcel.vertex(dcel.dest(HalfEdgeId(e * 2))).coords;
            edge_length[e] = edge_length_m(c0, c1);
        }

        // 4b. Per-unit area (shoelace with cos(φ_mid) correction).
        let mut area = vec![0.0f64; num_units];
        for f in 0..dcel.num_faces() {
            let unit = face_to_unit[f];
            if unit == UnitId::EXTERIOR { continue; }
            let start = match dcel.face(FaceId(f)).half_edge {
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

        // 4c. Per-unit perimeter (sum of edge lengths for all boundary edges).
        let mut perimeter = vec![0.0f64; num_units];
        for f in 0..dcel.num_faces() {
            let unit = face_to_unit[f];
            if unit == UnitId::EXTERIOR { continue; }
            let start = match dcel.face(FaceId(f)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in dcel.face_cycle(start) {
                let twin_face = dcel.half_edge(dcel.half_edge(he).twin).face;
                let twin_unit = face_to_unit[twin_face.0];
                if twin_unit != unit {
                    perimeter[unit.0 as usize] += edge_length[he.0 / 2];
                }
            }
        }

        // 4d. Exterior boundary length.
        let mut exterior_boundary_length = vec![0.0f64; num_units];
        for h in 0..num_half_edges {
            let he = dcel.half_edge(HalfEdgeId(h));
            let unit = face_to_unit[he.face.0];
            if unit == UnitId::EXTERIOR { continue; }
            if face_to_unit[dcel.half_edge(he.twin).face.0] == UnitId::EXTERIOR {
                exterior_boundary_length[unit.0 as usize] += edge_length[h / 2];
            }
        }

        // 4e. Centroid (vertex-average of each unit's half-edge origins).
        let mut sum_x = vec![0.0f64; num_units];
        let mut sum_y = vec![0.0f64; num_units];
        let mut count = vec![0u32; num_units];
        for h in 0..num_half_edges {
            let he = dcel.half_edge(HalfEdgeId(h));
            let unit = face_to_unit[he.face.0];
            if unit == UnitId::EXTERIOR { continue; }
            let c = dcel.vertex(he.origin).coords;
            let u = unit.0 as usize;
            sum_x[u] += c.x;
            sum_y[u] += c.y;
            count[u] += 1;
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

        // 4f. Bounds (axis-aligned bounding box per unit).
        let inf = f64::INFINITY;
        let mut min_x = vec![ inf; num_units];
        let mut min_y = vec![ inf; num_units];
        let mut max_x = vec![-inf; num_units];
        let mut max_y = vec![-inf; num_units];
        for h in 0..num_half_edges {
            let he = dcel.half_edge(HalfEdgeId(h));
            let unit = face_to_unit[he.face.0];
            if unit == UnitId::EXTERIOR { continue; }
            let c = dcel.vertex(he.origin).coords;
            let u = unit.0 as usize;
            if c.x < min_x[u] { min_x[u] = c.x; }
            if c.y < min_y[u] { min_y[u] = c.y; }
            if c.x > max_x[u] { max_x[u] = c.x; }
            if c.y > max_y[u] { max_y[u] = c.y; }
        }
        let bounds: Vec<Rect<f64>> = (0..num_units).map(|u| {
            let (mnx, mny) = if min_x[u].is_finite() { (min_x[u], min_y[u]) } else { (0.0, 0.0) };
            let (mxx, mxy) = if max_x[u].is_finite() { (max_x[u], max_y[u]) } else { (0.0, 0.0) };
            Rect::new(Coord { x: mnx, y: mny }, Coord { x: mxx, y: mxy })
        }).collect();

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

        // 4h. is_exterior flag.
        let mut is_exterior = vec![false; num_units];
        for h in 0..num_half_edges {
            let he = dcel.half_edge(HalfEdgeId(h));
            let unit = face_to_unit[he.face.0];
            if unit == UnitId::EXTERIOR { continue; }
            if face_to_unit[dcel.half_edge(he.twin).face.0] == UnitId::EXTERIOR {
                is_exterior[unit.0 as usize] = true;
            }
        }

        // -----------------------------------------------------------------
        // 5. Build adjacency matrices
        // -----------------------------------------------------------------
        let adjacent = build_adjacent(&dcel, &face_to_unit, &edge_length, num_units);
        let touching = build_touching(&dcel, &face_to_unit, num_units);
        let rtree = SpatialIndex::new(&bounds);

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
        };

        #[cfg(debug_assertions)]
        region.validate().map_err(|e| RegionError::InvalidGeometry(
            format!("post-construction validation failed: {e:?}")
        ))?;

        Ok(region)
    }

    /// Deserialise a `Region` from a GeoJSON string.
    ///
    /// Each feature in the collection becomes one unit; `UnitId` is assigned
    /// in feature order.  Only `Polygon` and `MultiPolygon` geometry types are
    /// supported.
    pub fn from_geojson(_data: &str, _snap_tol: f64) -> Result<Self, RegionError> {
        // GeoJSON parsing requires the `geojson` crate which is not currently
        // in our dependencies.  This is left as a stub until that dependency
        // is added.
        todo!("from_geojson requires the `geojson` crate dependency")
    }

    /// Build a `Region` from a shapefile.
    ///
    /// Each shape record becomes one unit; `UnitId` is assigned in record
    /// order.  Only `Polygon` geometry types are supported.
    pub fn from_shapefile(_path: &std::path::Path, _snap_tol: f64) -> Result<Self, RegionError> {
        // Shapefile parsing requires the `shapefile` crate which is not
        // currently in our dependencies.  This is left as a stub until that
        // dependency is added.
        todo!("from_shapefile requires the `shapefile` crate dependency")
    }
}

// ---------------------------------------------------------------------------
// Coordinate key for exact deduplication (post-snap)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct CoordKey {
    x_bits: u64,
    y_bits: u64,
}

impl From<Coord<f64>> for CoordKey {
    #[inline]
    fn from(c: Coord<f64>) -> Self {
        Self {
            x_bits: c.x.to_bits(),
            y_bits: c.y.to_bits(),
        }
    }
}

// ---------------------------------------------------------------------------
// Metric helpers
// ---------------------------------------------------------------------------

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

/// Pack two `VertexId`s into a single `u64` for use as a HashMap key.
/// Requires vertex count < 2^32 (guaranteed for any realistic dataset).
#[inline]
fn pack_edge(a: VertexId, b: VertexId) -> u64 {
    (a.0 as u64) << 32 | b.0 as u64
}

/// Signed area of a coordinate ring (shoelace, degrees²).
/// Positive = CCW, Negative = CW.
fn ring_signed_area(coords: &[Coord<f64>]) -> f64 {
    let n = coords.len();
    if n < 3 { return 0.0; }
    let edge_count = if coords[0] == coords[n - 1] { n - 1 } else { n };
    let mut area = 0.0f64;
    for i in 0..edge_count {
        let a = coords[i];
        let b = coords[(i + 1) % n];
        area += a.x * b.y - b.x * a.y;
    }
    area / 2.0
}

/// Signed area of a face cycle in degrees² (no metric correction).
/// Negative for CW cycles, positive for CCW.
fn signed_area_deg(dcel: &Dcel<Coord<f64>>, cycle: &[HalfEdgeId]) -> f64 {
    let mut area = 0.0;
    for &he in cycle {
        let c0 = dcel.vertex(dcel.half_edge(he).origin).coords;
        let c1 = dcel.vertex(dcel.dest(he)).coords;
        area += c0.x * c1.y - c1.x * c0.y;
    }
    area / 2.0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use geo::{Coord, LineString, MultiPolygon, Polygon};

    use crate::unit::UnitId;

    use super::*;

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

    fn two_squares() -> Vec<MultiPolygon<f64>> {
        vec![
            MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
            MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
        ]
    }

    // -----------------------------------------------------------------------
    // Basic construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_two_squares_succeeds() {
        Region::new(two_squares(), None).expect("construction should succeed");
    }

    #[test]
    fn new_returns_correct_unit_count() {
        let r = Region::new(two_squares(), None).unwrap();
        assert_eq!(r.num_units(), 2);
    }

    #[test]
    fn new_units_are_adjacent() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.are_adjacent(UnitId(0), UnitId(1)));
    }

    #[test]
    fn new_units_not_self_adjacent() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(!r.are_adjacent(UnitId(0), UnitId(0)));
    }

    #[test]
    fn new_both_units_are_exterior() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.is_exterior(UnitId(0)));
        assert!(r.is_exterior(UnitId(1)));
    }

    #[test]
    fn new_area_is_positive() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.area(UnitId(0)) > 0.0);
        assert!(r.area(UnitId(1)) > 0.0);
    }

    #[test]
    fn new_perimeter_is_positive() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.perimeter(UnitId(0)) > 0.0);
        assert!(r.perimeter(UnitId(1)) > 0.0);
    }

    #[test]
    fn new_bounds_are_correct() {
        let r = Region::new(two_squares(), None).unwrap();
        let b0 = r.bounds(UnitId(0));
        assert!((b0.min().x - 0.0).abs() < 1e-9);
        assert!((b0.min().y - 0.0).abs() < 1e-9);
        assert!((b0.max().x - 1.0).abs() < 1e-9);
        assert!((b0.max().y - 1.0).abs() < 1e-9);
    }

    #[test]
    fn new_centroid_is_inside_bounds() {
        let r = Region::new(two_squares(), None).unwrap();
        for uid in r.unit_ids() {
            let c = r.centroid(uid);
            let b = r.bounds(uid);
            assert!(c.x >= b.min().x && c.x <= b.max().x);
            assert!(c.y >= b.min().y && c.y <= b.max().y);
        }
    }

    #[test]
    fn new_shared_boundary_positive() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.shared_boundary_length(UnitId(0), UnitId(1)) > 0.0);
    }

    #[test]
    fn new_queen_is_superset_of_rook() {
        let r = Region::new(two_squares(), None).unwrap();
        let rook = r.adjacency();
        let queen = r.touching();
        for uid in r.unit_ids() {
            for &nb in rook.neighbors(uid) {
                assert!(queen.contains(uid, nb));
            }
        }
    }

    #[test]
    fn new_exterior_boundary_positive() {
        let r = Region::new(two_squares(), None).unwrap();
        assert!(r.exterior_boundary_length(UnitId(0)) > 0.0);
        assert!(r.exterior_boundary_length(UnitId(1)) > 0.0);
    }

    // -----------------------------------------------------------------------
    // Three squares (L-shaped arrangement)
    // -----------------------------------------------------------------------

    #[test]
    fn new_three_squares() {
        let geoms = vec![
            MultiPolygon(vec![rect_poly(0.0, 0.0, 1.0, 1.0)]),
            MultiPolygon(vec![rect_poly(1.0, 0.0, 2.0, 1.0)]),
            MultiPolygon(vec![rect_poly(0.0, 1.0, 1.0, 2.0)]),
        ];
        let r = Region::new(geoms, None).unwrap();
        assert_eq!(r.num_units(), 3);
        // u0 adjacent to u1 (right) and u2 (above)
        assert!(r.are_adjacent(UnitId(0), UnitId(1)));
        assert!(r.are_adjacent(UnitId(0), UnitId(2)));
        // u1 and u2 share only a corner → not Rook adjacent
        assert!(!r.are_adjacent(UnitId(1), UnitId(2)));
        // But they should be Queen adjacent (corner touch)
        assert!(r.touching().contains(UnitId(1), UnitId(2)));
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn new_empty_returns_error() {
        match Region::new(vec![], None) {
            Err(RegionError::InvalidGeometry(_)) => {}
            other => panic!("expected InvalidGeometry, got {:?}", other.err()),
        }
    }
}
