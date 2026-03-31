use std::collections::HashSet;

use geo::{Coord, LineString, MultiLineString, MultiPolygon, Polygon};

use crate::dcel::HalfEdgeId;
use crate::unit::UnitId;

use super::{Interiors, Region, Ring};

impl Region {
    /// Exterior boundary of the subset as a `MultiLineString`.
    /// Equivalent to the outline of the merged shape without polygon union.
    ///
    /// Each `LineString` in the result is a closed cycle (first == last).
    /// Outer boundaries are CCW; hole boundaries are CW.
    pub fn boundary_of(&self, units: impl IntoIterator<Item = UnitId>) -> MultiLineString<f64> {
        let set: HashSet<UnitId> = units.into_iter().collect();
        let is_boundary = self.boundary_mask(&set);
        let cycles = self.trace_cycles(&is_boundary);
        MultiLineString(cycles.into_iter().map(|(ring, _)| LineString(ring)).collect())
    }

    /// Geometric union of all unit polygons in `units`.
    ///
    /// Uses the DCEL boundary walk to extract boundary cycles, classifies them
    /// as outer rings (CCW, positive signed area) or holes (CW, negative signed
    /// area), and matches holes to their enclosing outer ring.
    pub fn union_of(&self, units: impl IntoIterator<Item = UnitId>) -> MultiPolygon<f64> {
        let set: HashSet<UnitId> = units.into_iter().collect();
        let is_boundary = self.boundary_mask(&set);
        let cycles = self.trace_cycles(&is_boundary);
        cycles_to_multipolygon(cycles)
    }

    /// Faster variant of [`Region::union_of`] for use when the caller knows which units
    /// are on the district boundary (frontier).
    ///
    /// Instead of scanning all DCEL half-edges, this only examines the faces
    /// belonging to `frontier_units` — units that share an edge with a
    /// different district or the region exterior.
    ///
    /// # Contract
    ///
    /// `is_in_district(u)` must return `true` iff unit `u` belongs to the
    /// same district as the frontier units.  It is **never** called with
    /// `UnitId::EXTERIOR`; the caller must not pass exterior units in
    /// `frontier_units` either.
    pub fn union_of_frontier(
        &self,
        frontier_units: impl IntoIterator<Item = UnitId>,
        is_in_district: impl Fn(UnitId) -> bool,
    ) -> MultiPolygon<f64> {
        // Collect boundary half-edges by walking only frontier unit faces.
        let mut boundary: Vec<u32> = Vec::new();
        for unit in frontier_units {
            for &face_id in self.unit_faces(unit) {
                // Primary cycle (outer ring).
                let start = match self.dcel.face(face_id).half_edge {
                    Some(he) => he,
                    None => continue,
                };
                let mut cur = start;
                loop {
                    let half_edge = self.dcel.half_edge(cur);
                    let twin_unit = self.face_to_unit[self.dcel.half_edge(cur.twin()).face.0 as usize];
                    if twin_unit == UnitId::EXTERIOR || !is_in_district(twin_unit) {
                        boundary.push(cur.0);
                    }
                    cur = half_edge.next;
                    if cur == start { break; }
                }
                // Inner ring cycles (holes in donut-shaped units).
                for &inner_start in self.face_inner_cycle_starts(face_id) {
                    let mut cur = inner_start;
                    loop {
                        let half_edge = self.dcel.half_edge(cur);
                        let twin_unit = self.face_to_unit[self.dcel.half_edge(cur.twin()).face.0 as usize];
                        if twin_unit == UnitId::EXTERIOR || !is_in_district(twin_unit) {
                            boundary.push(cur.0);
                        }
                        cur = half_edge.next;
                        if cur == inner_start { break; }
                    }
                }
            }
        }

        let num_half_edges = self.dcel.num_half_edges();
        let mut in_boundary = vec![false; num_half_edges];
        for &h in &boundary { in_boundary[h as usize] = true; }
        let mut visited = vec![false; num_half_edges];
        let mut cycles: Vec<(Ring, f64)> = Vec::new();

        for &start_h in &boundary {
            if visited[start_h as usize] { continue; }

            let mut coords = Vec::new();
            let mut signed_area = 0.0;
            let mut cur = HalfEdgeId(start_h);
            loop {
                visited[cur.0 as usize] = true;
                let c0 = self.dcel.vertex(self.dcel.half_edge(cur).origin).coords;
                coords.push(c0);

                let c1 = self.dcel.vertex(self.dcel.dest(cur)).coords;
                signed_area += c0.x * c1.y - c1.x * c0.y;

                let mut next = self.dcel.half_edge(cur).next;
                while !in_boundary[next.0 as usize] {
                    next = self.dcel.half_edge(next.twin()).next;
                }
                cur = next;

                if cur.0 == start_h { break; }
            }
            signed_area /= 2.0;
            if let Some(&first) = coords.first() { coords.push(first); }
            cycles.push((coords, signed_area));
        }

        cycles_to_multipolygon(cycles)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build a boolean mask over all half-edges: `true` iff the half-edge's
    /// face is in `set` and its twin's face is outside `set`.
    fn boundary_mask(&self, set: &HashSet<UnitId>) -> Vec<bool> {
        let num_half_edges = self.dcel.num_half_edges();
        (0..num_half_edges).map(|e| {
            let half_edge = self.dcel.half_edge(HalfEdgeId(e as u32));
            let unit = self.face_to_unit[half_edge.face.0 as usize];
            if !set.contains(&unit) { return false; }
            let twin_face = self.dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face;
            !set.contains(&self.face_to_unit[twin_face.0 as usize])
        }).collect()
    }

    /// Trace all boundary cycles given an `is_boundary` half-edge mask.
    ///
    /// Returns `Vec<(coords, signed_area)>` where each entry is a closed ring
    /// (first coordinate repeated as last) and its signed shoelace area in
    /// degree²/2 (positive = CCW outer ring, negative = CW hole).
    fn trace_cycles(&self, is_boundary: &[bool]) -> Vec<(Ring, f64)> {
        let num_half_edges = is_boundary.len();
        let mut visited = vec![false; num_half_edges];
        let mut cycles: Vec<(Ring, f64)> = Vec::new();

        for e in 0..num_half_edges {
            if !is_boundary[e] || visited[e] { continue; }

            let mut coords = Vec::new();
            let mut signed_area = 0.0;
            let mut cur = HalfEdgeId(e as u32);
            loop {
                visited[cur.0 as usize] = true;
                let c0 = self.dcel.vertex(self.dcel.half_edge(cur).origin).coords;
                coords.push(c0);
                let c1 = self.dcel.vertex(self.dcel.dest(cur)).coords;
                signed_area += c0.x * c1.y - c1.x * c0.y;

                let mut next = self.dcel.half_edge(cur).next;
                while !is_boundary[next.0 as usize] {
                    next = self.dcel.half_edge(next.twin()).next;
                }
                cur = next;

                if cur == HalfEdgeId(e as u32) { break; }
            }
            signed_area /= 2.0;
            if let Some(&first) = coords.first() { coords.push(first); }
            cycles.push((coords, signed_area));
        }

        cycles
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Partition cycles into outer rings and holes, match holes to their enclosing
/// outer ring, and assemble a `MultiPolygon`.
fn cycles_to_multipolygon(cycles: Vec<(Ring, f64)>) -> MultiPolygon<f64> {
    let mut outers: Vec<(Ring, Interiors)> = Vec::new();
    let mut holes: Vec<Ring> = Vec::new();

    for (coords, area) in cycles {
        if area > 0.0 {
            outers.push((coords, Vec::new()));
        } else {
            holes.push(coords);
        }
    }

    // Match each hole to its smallest enclosing outer ring using point-in-ring
    // test.  Taking the smallest (by absolute signed area) correctly handles
    // nested polygons where multiple outer rings may enclose the same hole.
    for hole in holes {
        let pt = hole[0];
        let mut best = 0;
        let mut best_area = f64::INFINITY;
        for (i, (outer, _)) in outers.iter().enumerate() {
            if point_in_ring(pt, outer) {
                // Compute the absolute signed area of this outer ring.
                let area = outer.windows(2)
                    .map(|w| w[0].x * w[1].y - w[1].x * w[0].y)
                    .sum::<f64>()
                    .abs();
                if area < best_area {
                    best_area = area;
                    best = i;
                }
            }
        }
        outers[best].1.push(LineString(hole));
    }

    MultiPolygon(outers.into_iter()
        .map(|(ring, holes)| Polygon::new(LineString(ring), holes))
        .collect())
}

/// Ray-casting point-in-polygon test.  Returns `true` if `pt` is strictly
/// inside `ring` (a closed sequence of coordinates, first == last).
fn point_in_ring(pt: Coord<f64>, ring: &Ring) -> bool {
    let mut inside = false;
    let n = ring.len();
    if n < 4 { return false; } // degenerate
    let mut j = n - 2; // skip closing vertex (same as first)
    for i in 0..n - 1 {
        let a = ring[i];
        let b = ring[j];
        if (a.y > pt.y) != (b.y > pt.y) {
            let x_intersect = a.x + (pt.y - a.y) * (b.x - a.x) / (b.y - a.y);
            if pt.x < x_intersect {
                inside = !inside;
            }
        }
        j = i;
    }
    inside
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::unit::UnitId;
    use crate::region::test_helpers::make_two_unit_region;

    // -----------------------------------------------------------------------
    // boundary_of
    // -----------------------------------------------------------------------

    #[test]
    fn boundary_of_single_unit_has_one_ring() {
        let r = make_two_unit_region();
        let b = r.boundary_of([UnitId(0)]);
        assert_eq!(b.0.len(), 1);
    }

    #[test]
    fn boundary_of_single_unit_ring_is_closed() {
        let r = make_two_unit_region();
        let b = r.boundary_of([UnitId(0)]);
        let pts = &b.0[0].0;
        assert_eq!(pts.first(), pts.last());
    }

    #[test]
    fn boundary_of_single_unit_has_five_coords() {
        // 4 boundary edges → 4 vertices + closing = 5 coords
        let r = make_two_unit_region();
        let b = r.boundary_of([UnitId(0)]);
        assert_eq!(b.0[0].0.len(), 5);
    }

    #[test]
    fn boundary_of_both_units_has_one_ring() {
        let r = make_two_unit_region();
        let b = r.boundary_of(r.unit_ids());
        assert_eq!(b.0.len(), 1);
    }

    #[test]
    fn boundary_of_both_units_excludes_shared_edge() {
        // Merged boundary: 6 outer edges → 6 vertices + closing = 7 coords
        let r = make_two_unit_region();
        let b = r.boundary_of(r.unit_ids());
        assert_eq!(b.0[0].0.len(), 7);
    }

    // -----------------------------------------------------------------------
    // union_of
    // -----------------------------------------------------------------------

    #[test]
    fn union_of_single_unit_has_one_polygon() {
        let r = make_two_unit_region();
        let mp = r.union_of([UnitId(0)]);
        assert_eq!(mp.0.len(), 1);
    }

    #[test]
    fn union_of_single_unit_exterior_has_five_coords() {
        let r = make_two_unit_region();
        let mp = r.union_of([UnitId(0)]);
        assert_eq!(mp.0[0].exterior().0.len(), 5);
    }

    #[test]
    fn union_of_both_units_has_one_polygon() {
        let r = make_two_unit_region();
        let mp = r.union_of(r.unit_ids());
        assert_eq!(mp.0.len(), 1);
    }

    #[test]
    fn union_of_both_units_exterior_has_seven_coords() {
        let r = make_two_unit_region();
        let mp = r.union_of(r.unit_ids());
        assert_eq!(mp.0[0].exterior().0.len(), 7);
    }

    #[test]
    fn union_of_both_units_has_no_holes() {
        let r = make_two_unit_region();
        let mp = r.union_of(r.unit_ids());
        assert!(mp.0[0].interiors().is_empty());
    }
}
