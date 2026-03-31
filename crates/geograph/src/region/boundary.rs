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
        let num_half_edges = self.dcel.num_half_edges();

        // Mark boundary half-edges: face in set, twin face outside set.
        let is_boundary: Vec<bool> = (0..num_half_edges).map(|e| {
            let half_edge = self.dcel.half_edge(HalfEdgeId(e as u32));
            let unit = self.face_to_unit[half_edge.face.0 as usize];
            if !set.contains(&unit) { return false; }
            let twin_face = self.dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face;
            !set.contains(&self.face_to_unit[twin_face.0 as usize])
        }).collect();

        // Trace boundary cycles.
        let mut visited = vec![false; num_half_edges];
        let mut lines = Vec::new();

        for e in 0..num_half_edges {
            if !is_boundary[e] || visited[e] { continue; }

            let mut coords = Vec::new();
            let mut cur = HalfEdgeId(e as u32);
            loop {
                visited[cur.0 as usize] = true;
                coords.push(self.dcel.vertex(self.dcel.half_edge(cur).origin).coords);

                // Find next boundary edge: from dest(cur), scan CCW around the
                // vertex until we find the next half-edge that is also boundary.
                let mut next = self.dcel.half_edge(cur).next;
                while !is_boundary[next.0 as usize] {
                    next = self.dcel.half_edge(next.twin()).next;
                }
                cur = next;

                if cur == HalfEdgeId(e as u32) { break; }
            }

            // Close the ring.
            if let Some(&first) = coords.first() {
                coords.push(first);
            }
            lines.push(LineString(coords));
        }

        MultiLineString(lines)
    }

    /// Geometric union of all unit polygons in `units`.
    ///
    /// Uses the DCEL boundary walk to extract boundary cycles, classifies them
    /// as outer rings (CCW, positive signed area) or holes (CW, negative signed
    /// area), and matches holes to their enclosing outer ring.
    pub fn union_of(&self, units: impl IntoIterator<Item = UnitId>) -> MultiPolygon<f64> {
        let set: HashSet<UnitId> = units.into_iter().collect();
        let num_half_edges = self.dcel.num_half_edges();

        // Mark boundary half-edges: face in set, twin face outside set.
        let is_boundary: Vec<bool> = (0..num_half_edges).map(|e| {
            let half_edge = self.dcel.half_edge(HalfEdgeId(e as u32));
            let unit = self.face_to_unit[half_edge.face.0 as usize];
            if !set.contains(&unit) { return false; }
            let twin_face = self.dcel.half_edge(HalfEdgeId(e as u32 ^ 1)).face;
            !set.contains(&self.face_to_unit[twin_face.0 as usize])
        }).collect();

        // Trace boundary cycles and compute signed area for each.
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

                // Accumulate signed area (shoelace in degrees).
                let c1 = self.dcel.vertex(self.dcel.dest(cur)).coords;
                signed_area += c0.x * c1.y - c1.x * c0.y;

                // Find next boundary edge.
                let mut next = self.dcel.half_edge(cur).next;
                while !is_boundary[next.0 as usize] {
                    next = self.dcel.half_edge(next.twin()).next;
                }
                cur = next;

                if cur == HalfEdgeId(e as u32) { break; }
            }
            signed_area /= 2.0;

            // Close the ring.
            if let Some(&first) = coords.first() {
                coords.push(first);
            }
            cycles.push((coords, signed_area));
        }

        // Partition into outer rings (positive area = CCW) and holes (negative = CW).
        let mut outers: Vec<(Ring, Interiors)> = Vec::new();
        let mut holes: Vec<Ring> = Vec::new();

        for (coords, area) in cycles {
            if area > 0.0 {
                outers.push((coords, Vec::new()));
            } else {
                holes.push(coords);
            }
        }

        // Match each hole to its enclosing outer ring using point-in-ring test.
        for hole in holes {
            // Use the first vertex of the hole as the test point.
            let pt = hole[0];
            let mut best = 0;
            for (i, (outer, _)) in outers.iter().enumerate() {
                if point_in_ring(pt, outer) {
                    best = i;
                    break;
                }
            }
            outers[best].1.push(LineString(hole));
        }

        // Build MultiPolygon.
        let polys: Vec<Polygon<f64>> = outers
            .into_iter()
            .map(|(ring, holes)| Polygon::new(LineString(ring), holes))
            .collect();

        MultiPolygon(polys)
    }

    /// Faster variant of [`Region::union_of`] for use when the caller knows which units
    /// are on the district boundary (frontier).
    ///
    /// Instead of scanning all DCEL half-edges, this only examines the faces
    /// belonging to `frontier_units` — units that share an edge with a
    /// different district or the region exterior. `is_in_district(u)` must
    /// return `true` iff unit `u` belongs to the same district; it is never
    /// called with `UnitId::EXTERIOR`.
    pub fn union_of_frontier(
        &self,
        frontier_units: impl IntoIterator<Item = UnitId>,
        is_in_district: impl Fn(UnitId) -> bool,
    ) -> MultiPolygon<f64> {
        // Collect boundary half-edges by walking only frontier unit faces.
        let mut boundary: Vec<u32> = Vec::new();
        for unit in frontier_units {
            for &face_id in &self.unit_to_faces[unit.0 as usize] {
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
                for &inner_start in &self.face_inner_cycles[face_id.0 as usize] {
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

        let boundary_set: HashSet<u32> = boundary.iter().copied().collect();
        let mut visited: HashSet<u32> = HashSet::new();
        let mut cycles: Vec<(Ring, f64)> = Vec::new();

        for &start_h in &boundary {
            if visited.contains(&start_h) { continue; }

            let mut coords = Vec::new();
            let mut signed_area = 0.0;
            let mut cur = HalfEdgeId(start_h);
            loop {
                visited.insert(cur.0);
                let c0 = self.dcel.vertex(self.dcel.half_edge(cur).origin).coords;
                coords.push(c0);

                let c1 = self.dcel.vertex(self.dcel.dest(cur)).coords;
                signed_area += c0.x * c1.y - c1.x * c0.y;

                let mut next = self.dcel.half_edge(cur).next;
                while !boundary_set.contains(&next.0) {
                    next = self.dcel.half_edge(next.twin()).next;
                }
                cur = next;

                if cur.0 == start_h { break; }
            }
            signed_area /= 2.0;
            if let Some(&first) = coords.first() { coords.push(first); }
            cycles.push((coords, signed_area));
        }

        // Same hole-matching logic as union_of.
        let mut outers: Vec<(Ring, Interiors)> = Vec::new();
        let mut holes: Vec<Ring> = Vec::new();
        for (coords, area) in cycles {
            if area > 0.0 { outers.push((coords, Vec::new())); }
            else          { holes.push(coords); }
        }
        for hole in holes {
            let pt = hole[0];
            let mut best = 0;
            for (i, (outer, _)) in outers.iter().enumerate() {
                if point_in_ring(pt, outer) { best = i; break; }
            }
            outers[best].1.push(LineString(hole));
        }
        let polys: Vec<Polygon<f64>> = outers.into_iter()
            .map(|(ring, holes)| Polygon::new(LineString(ring), holes))
            .collect();
        MultiPolygon(polys)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
