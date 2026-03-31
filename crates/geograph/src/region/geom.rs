use std::collections::HashSet;

use geo::{ConvexHull, Coord, Contains, LineString, MultiLineString, MultiPolygon, Polygon, Rect};
use rstar::AABB;

use crate::dcel::{FaceId, HalfEdgeId};
use crate::unit::UnitId;

use super::Region;

impl Region {
    // -----------------------------------------------------------------------
    // Single-unit geometry  (O(1), pre-cached)
    // -----------------------------------------------------------------------

    /// Area of `unit` in m².
    #[inline]
    pub fn area(&self, unit: UnitId) -> f64 {
        self.area[unit.0 as usize]
    }

    /// Total perimeter of `unit` in m (includes hole boundaries).
    #[inline]
    pub fn perimeter(&self, unit: UnitId) -> f64 {
        self.perimeter[unit.0 as usize]
    }

    /// Length of `unit`'s boundary that touches the region exterior, in m.
    #[inline]
    pub fn exterior_boundary_length(&self, unit: UnitId) -> f64 {
        self.exterior_boundary_length[unit.0 as usize]
    }

    /// Centroid of `unit` in lon/lat.
    #[inline]
    pub fn centroid(&self, unit: UnitId) -> Coord<f64> {
        self.centroid[unit.0 as usize]
    }

    /// Axis-aligned bounding box of `unit` in lon/lat.
    #[inline]
    pub fn bounds(&self, unit: UnitId) -> Rect<f64> {
        self.bounds[unit.0 as usize]
    }

    /// Returns `true` if `unit` has any boundary with the region exterior.
    #[inline]
    pub fn is_exterior(&self, unit: UnitId) -> bool {
        self.is_exterior[unit.0 as usize]
    }

    /// Boundary of `unit` as a `MultiLineString`.
    ///
    /// Each element is one ring of the unit's polygon (outer ring or hole).
    /// Rings are closed (first coordinate repeated as last).
    pub fn boundary(&self, unit: UnitId) -> MultiLineString<f64> {
        let mut lines = Vec::new();
        for f in 0..self.dcel.num_faces() {
            if self.face_to_unit[f] != unit { continue; }
            let start = match self.dcel.face(FaceId(f as u32)).half_edge {
                Some(he) => he,
                None => continue,
            };
            let mut coords: Vec<Coord<f64>> = self.dcel
                .face_cycle(start)
                .map(|he| self.dcel.vertex(self.dcel.half_edge(he).origin).coords)
                .collect();
            // Close the ring.
            if let Some(&first) = coords.first() {
                coords.push(first);
            }
            lines.push(LineString(coords));
        }
        MultiLineString(lines)
    }

    /// Convex hull of `unit` in lon/lat.
    #[inline]
    pub fn convex_hull(&self, unit: UnitId) -> Polygon<f64> {
        self.geometries[unit.0 as usize].convex_hull()
    }

    // -----------------------------------------------------------------------
    // Subset geometry  (O(k) unless noted)
    // -----------------------------------------------------------------------

    /// Sum of areas of all units in `units`, in m².
    pub fn area_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        units.into_iter().map(|u| self.area[u.0 as usize]).sum()
    }

    /// Total exterior perimeter of the subset `units`, in m.
    /// Shared internal edges are excluded.
    pub fn perimeter_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        let set: HashSet<UnitId> = units.into_iter().collect();
        let mut total = 0.0;
        for f in 0..self.dcel.num_faces() {
            let unit = self.face_to_unit[f];
            if !set.contains(&unit) { continue; }
            let start = match self.dcel.face(FaceId(f as u32)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(he.twin()).face;
                if !set.contains(&self.face_to_unit[twin_face.0 as usize]) {
                    total += self.edge_length[he.0 as usize / 2];
                }
            }
        }
        total
    }

    /// Total length of the subset boundary that touches the region exterior,
    /// in m.
    pub fn exterior_boundary_length_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        units.into_iter()
            .map(|u| self.exterior_boundary_length[u.0 as usize])
            .sum()
    }

    /// Smallest bounding box containing all units in `units`.
    ///
    /// # Panics
    /// Panics if `units` is empty.
    pub fn bounds_of(&self, units: impl IntoIterator<Item = UnitId>) -> Rect<f64> {
        let mut iter = units.into_iter();
        let first = iter.next().expect("bounds_of requires at least one unit");
        let mut rect = self.bounds[first.0 as usize];
        for u in iter {
            let b = self.bounds[u.0 as usize];
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
    }

    /// Bounding box of the entire region (all units).  O(1), pre-cached.
    #[inline]
    pub fn bounds_all(&self) -> Rect<f64> {
        self.bounds_all
    }

    /// Convex hull of the union of all unit geometries in `units`.
    ///
    /// **Performance note:** This currently clones all polygon data and runs
    /// Graham scan on every vertex — O(V log V) where V is total vertices
    /// across all input units.  For large subsets this is slow.  A future
    /// implementation should merge per-unit hulls incrementally (O(k · h)
    /// where h is the output hull size) instead of recomputing from scratch.
    pub fn convex_hull_of(&self, units: impl IntoIterator<Item = UnitId>) -> Polygon<f64> {
        let combined: MultiPolygon<f64> = MultiPolygon(
            units.into_iter()
                .flat_map(|u| self.geometries[u.0 as usize].0.iter().cloned())
                .collect(),
        );
        combined.convex_hull()
    }

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

    /// Polsby-Popper compactness score for the subset: `4π·area / perimeter²`.
    pub fn compactness_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        let units: Vec<UnitId> = units.into_iter().collect();
        let a = self.area_of(units.iter().copied());
        let p = self.perimeter_of(units.iter().copied());
        if p == 0.0 { return 0.0; }
        4.0 * std::f64::consts::PI * a / (p * p)
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
        let mut cycles: Vec<(Vec<Coord<f64>>, f64)> = Vec::new();

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
        #[allow(clippy::type_complexity)]
        let mut outers: Vec<(Vec<Coord<f64>>, Vec<LineString<f64>>)> = Vec::new();
        let mut holes: Vec<Vec<Coord<f64>>> = Vec::new();

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

    /// Faster variant of [`union_of`] for use when the caller knows which units
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
        let mut cycles: Vec<(Vec<Coord<f64>>, f64)> = Vec::new();

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
        #[allow(clippy::type_complexity)]
        let mut outers: Vec<(Vec<Coord<f64>>, Vec<LineString<f64>>)> = Vec::new();
        let mut holes: Vec<Vec<Coord<f64>>> = Vec::new();
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
        let polys: Vec<Polygon<f64>> = outers
            .into_iter()
            .map(|(ring, holes)| Polygon::new(LineString(ring), holes))
            .collect();
        MultiPolygon(polys)
    }

    // -----------------------------------------------------------------------
    // Edge metrics
    // -----------------------------------------------------------------------

    /// Length of the shared boundary between units `a` and `b`, in m.
    /// Returns `0.0` if the units are not Rook-adjacent.
    pub fn shared_boundary_length(&self, a: UnitId, b: UnitId) -> f64 {
        self.sum_edge_lengths_between(a, b)
    }

    /// Shared boundary length in metres at a CSR index in the rook adjacency matrix.
    ///
    /// This is an O(1) alternative to [`Region::shared_boundary_length`] for callers
    /// that are already iterating the rook adjacency matrix and have a CSR index at hand.
    /// Both return the same value.
    #[inline]
    pub fn shared_boundary_length_at(&self, csr_idx: usize) -> f64 {
        self.adjacent.weight_at(csr_idx)
    }

    /// Total length of the boundary between `units` and `other`, in m.
    pub fn boundary_length_with(
        &self,
        units: impl IntoIterator<Item = UnitId>,
        other: impl IntoIterator<Item = UnitId>,
    ) -> f64 {
        let other_set: HashSet<UnitId> = other.into_iter().collect();
        let unit_set:  HashSet<UnitId> = units.into_iter().collect();
        // Walk all faces in unit_set; sum edges whose twin face is in other_set.
        let mut total = 0.0;
        for f in 0..self.dcel.num_faces() {
            let unit = self.face_to_unit[f];
            if !unit_set.contains(&unit) { continue; }
            let start = match self.dcel.face(FaceId(f as u32)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(he.twin()).face;
                if other_set.contains(&self.face_to_unit[twin_face.0 as usize]) {
                    total += self.edge_length[he.0 as usize / 2];
                }
            }
        }
        total
    }

    // -----------------------------------------------------------------------
    // Spatial queries
    // -----------------------------------------------------------------------

    /// Return the `UnitId` of the unit that contains `point` (lon/lat), or
    /// `None` if the point falls outside all units.
    ///
    /// Uses an R-tree for O(log n) candidate lookup, then exact
    /// point-in-polygon tests on candidates.
    pub fn unit_at(&self, point: Coord<f64>) -> Option<UnitId> {
        let geo_point = geo::Point::from(point);
        self.rtree.query_point([point.x, point.y])
            .find(|&uid| self.geometries[uid.0 as usize].contains(&geo_point))
    }

    /// Return all `UnitId`s whose bounding box intersects `envelope`.
    ///
    /// This is a coarse filter — returned units may not actually intersect
    /// the envelope geometrically.  Use for candidate generation.
    pub fn units_in_envelope(&self, envelope: Rect<f64>) -> Vec<UnitId> {
        let aabb = AABB::from_corners(
            [envelope.min().x, envelope.min().y],
            [envelope.max().x, envelope.max().y],
        );
        self.rtree.query(aabb).collect()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Sum `edge_length[he/2]` for each half-edge of `unit_a`'s faces whose
    /// twin belongs to `unit_b`.
    fn sum_edge_lengths_between(&self, unit_a: UnitId, unit_b: UnitId) -> f64 {
        let mut total = 0.0;
        for f in 0..self.dcel.num_faces() {
            if self.face_to_unit[f] != unit_a { continue; }
            let start = match self.dcel.face(FaceId(f as u32)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(he.twin()).face;
                if self.face_to_unit[twin_face.0 as usize] == unit_b {
                    total += self.edge_length[he.0 as usize / 2];
                }
            }
        }
        total
    }

}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Ray-casting point-in-polygon test.  Returns `true` if `pt` is strictly
/// inside `ring` (a closed sequence of coordinates, first == last).
fn point_in_ring(pt: Coord<f64>, ring: &[Coord<f64>]) -> bool {
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
    use geo::{Coord, Rect};

    use crate::unit::UnitId;
    use crate::region::test_helpers::make_two_unit_region;

    // -----------------------------------------------------------------------
    // Single-unit O(1) accessors
    // -----------------------------------------------------------------------

    #[test]
    fn area_returns_cached_value() {
        let r = make_two_unit_region();
        assert_eq!(r.area(UnitId(0)), 10.0);
        assert_eq!(r.area(UnitId(1)), 20.0);
    }

    #[test]
    fn perimeter_returns_cached_value() {
        let r = make_two_unit_region();
        assert_eq!(r.perimeter(UnitId(0)), 4.0);
        assert_eq!(r.perimeter(UnitId(1)), 4.0);
    }

    #[test]
    fn exterior_boundary_length_returns_cached_value() {
        let r = make_two_unit_region();
        assert_eq!(r.exterior_boundary_length(UnitId(0)), 3.0);
        assert_eq!(r.exterior_boundary_length(UnitId(1)), 3.0);
    }

    #[test]
    fn centroid_returns_cached_value() {
        let r = make_two_unit_region();
        assert_eq!(r.centroid(UnitId(0)), Coord { x: 0.5, y: 0.5 });
        assert_eq!(r.centroid(UnitId(1)), Coord { x: 1.5, y: 0.5 });
    }

    #[test]
    fn bounds_returns_cached_value() {
        let r = make_two_unit_region();
        assert_eq!(
            r.bounds(UnitId(0)),
            Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 1.0, y: 1.0 })
        );
    }

    #[test]
    fn is_exterior_returns_cached_flag() {
        let r = make_two_unit_region();
        assert!(r.is_exterior(UnitId(0)));
        assert!(r.is_exterior(UnitId(1)));
    }

    // -----------------------------------------------------------------------
    // boundary
    // -----------------------------------------------------------------------

    #[test]
    fn boundary_single_unit_has_one_ring() {
        let r = make_two_unit_region();
        let b = r.boundary(UnitId(0));
        assert_eq!(b.0.len(), 1, "left unit has one face → one ring");
    }

    #[test]
    fn boundary_ring_is_closed() {
        let r = make_two_unit_region();
        for uid in r.unit_ids() {
            for ring in &r.boundary(uid).0 {
                let pts = &ring.0;
                assert_eq!(pts.first(), pts.last(), "ring for {uid} is not closed");
            }
        }
    }

    #[test]
    fn boundary_ring_length_for_rectangle() {
        // Each unit is a rectangle (4 corners) → 5 coords (including closing)
        let r = make_two_unit_region();
        let b = r.boundary(UnitId(0));
        assert_eq!(b.0[0].0.len(), 5);
    }

    // -----------------------------------------------------------------------
    // area_of
    // -----------------------------------------------------------------------

    #[test]
    fn area_of_single_unit() {
        let r = make_two_unit_region();
        assert_eq!(r.area_of([UnitId(0)]), 10.0);
        assert_eq!(r.area_of([UnitId(1)]), 20.0);
    }

    #[test]
    fn area_of_all_units_is_sum() {
        let r = make_two_unit_region();
        assert_eq!(r.area_of(r.unit_ids()), 30.0);
    }

    #[test]
    fn area_of_empty_is_zero() {
        let r = make_two_unit_region();
        assert_eq!(r.area_of([]), 0.0);
    }

    // -----------------------------------------------------------------------
    // perimeter_of  (DCEL walk — uses edge_length = 1.0 for all edges)
    // -----------------------------------------------------------------------

    #[test]
    fn perimeter_of_single_unit_is_four_edges() {
        // Each unit-0 face (left rectangle) has 4 edges, each length 1.0.
        let r = make_two_unit_region();
        assert_eq!(r.perimeter_of([UnitId(0)]), 4.0);
    }

    #[test]
    fn perimeter_of_both_units_excludes_shared_edge() {
        // 6 outer edges × 1.0 (shared middle edge excluded on both sides)
        let r = make_two_unit_region();
        assert_eq!(r.perimeter_of(r.unit_ids()), 6.0);
    }

    // -----------------------------------------------------------------------
    // exterior_boundary_length_of
    // -----------------------------------------------------------------------

    #[test]
    fn exterior_boundary_length_of_sums_cached_values() {
        let r = make_two_unit_region();
        assert_eq!(r.exterior_boundary_length_of(r.unit_ids()), 6.0);
    }

    // -----------------------------------------------------------------------
    // bounds_of
    // -----------------------------------------------------------------------

    #[test]
    fn bounds_of_two_units_is_union() {
        let r = make_two_unit_region();
        let b = r.bounds_of(r.unit_ids());
        assert_eq!(b.min(), Coord { x: 0.0, y: 0.0 });
        assert_eq!(b.max(), Coord { x: 2.0, y: 1.0 });
    }

    #[test]
    fn bounds_of_single_unit_matches_bounds() {
        let r = make_two_unit_region();
        assert_eq!(r.bounds_of([UnitId(1)]), r.bounds(UnitId(1)));
    }

    // -----------------------------------------------------------------------
    // compactness_of
    // -----------------------------------------------------------------------

    #[test]
    fn compactness_of_uses_dcel_perimeter() {
        // area=10, perimeter_of (DCEL) = 4.0  →  4π·10 / 16 = 5π/2
        let r = make_two_unit_region();
        let expected = 4.0 * std::f64::consts::PI * 10.0 / (4.0_f64.powi(2));
        assert!((r.compactness_of([UnitId(0)]) - expected).abs() < 1e-12);
    }

    #[test]
    fn compactness_of_zero_perimeter_is_zero() {
        // A degenerate unit with zero perimeter should not divide by zero.
        let mut r = make_two_unit_region();
        // Force perimeter cache to zero (artificial; tests the guard only).
        r.edge_length = vec![0.0; 7];
        // perimeter_of walks DCEL and sums edge_length — will be 0
        assert_eq!(r.compactness_of([UnitId(0)]), 0.0);
    }

    // -----------------------------------------------------------------------
    // shared_boundary_length
    // -----------------------------------------------------------------------

    #[test]
    fn shared_boundary_between_adjacent_units() {
        // Units 0 and 1 share exactly 1 edge (the middle edge, length 1.0).
        let r = make_two_unit_region();
        assert_eq!(r.shared_boundary_length(UnitId(0), UnitId(1)), 1.0);
        assert_eq!(r.shared_boundary_length(UnitId(1), UnitId(0)), 1.0);
    }

    #[test]
    fn shared_boundary_with_self_is_zero() {
        let r = make_two_unit_region();
        assert_eq!(r.shared_boundary_length(UnitId(0), UnitId(0)), 0.0);
    }

    // -----------------------------------------------------------------------
    // boundary_length_with
    // -----------------------------------------------------------------------

    #[test]
    fn boundary_length_with_matching_shared_edge() {
        let r = make_two_unit_region();
        assert_eq!(
            r.boundary_length_with([UnitId(0)], [UnitId(1)]),
            1.0
        );
    }

    #[test]
    fn boundary_length_with_disjoint_sets_is_zero() {
        let r = make_two_unit_region();
        // Both units in both sets — their shared edge is interior, not a
        // boundary between the two groups.
        assert_eq!(
            r.boundary_length_with([UnitId(0)], [UnitId(0)]),
            0.0
        );
    }

    // -----------------------------------------------------------------------
    // convex_hull / convex_hull_of
    // -----------------------------------------------------------------------

    #[test]
    fn convex_hull_of_rectangle_is_rectangle() {
        let r = make_two_unit_region();
        let hull = r.convex_hull(UnitId(0));
        // Unit 0 is a rectangle → hull exterior has 5 coords (4 corners + close)
        assert_eq!(hull.exterior().0.len(), 5);
    }

    #[test]
    fn convex_hull_of_both_units_encloses_all() {
        let r = make_two_unit_region();
        let hull = r.convex_hull_of(r.unit_ids());
        // Merged 2×1 rectangle → hull exterior has 5 coords
        assert_eq!(hull.exterior().0.len(), 5);
        // Check bounds: hull should span (0,0) to (2,1)
        let xs: Vec<f64> = hull.exterior().0.iter().map(|c| c.x).collect();
        let ys: Vec<f64> = hull.exterior().0.iter().map(|c| c.y).collect();
        assert_eq!(*xs.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(), 0.0);
        assert_eq!(*xs.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(), 2.0);
        assert_eq!(*ys.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(), 0.0);
        assert_eq!(*ys.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(), 1.0);
    }

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

    // -----------------------------------------------------------------------
    // unit_at / units_in_envelope
    // -----------------------------------------------------------------------

    #[test]
    fn unit_at_center_of_left_unit() {
        let r = make_two_unit_region();
        assert_eq!(r.unit_at(Coord { x: 0.5, y: 0.5 }), Some(UnitId(0)));
    }

    #[test]
    fn unit_at_center_of_right_unit() {
        let r = make_two_unit_region();
        assert_eq!(r.unit_at(Coord { x: 1.5, y: 0.5 }), Some(UnitId(1)));
    }

    #[test]
    fn unit_at_outside_returns_none() {
        let r = make_two_unit_region();
        assert_eq!(r.unit_at(Coord { x: 5.0, y: 5.0 }), None);
    }

    #[test]
    fn units_in_envelope_covers_both() {
        let r = make_two_unit_region();
        let envelope = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 1.0 });
        let mut result = r.units_in_envelope(envelope);
        result.sort();
        assert_eq!(result, vec![UnitId(0), UnitId(1)]);
    }

    #[test]
    fn units_in_envelope_covers_one() {
        let r = make_two_unit_region();
        let envelope = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 0.5, y: 0.5 });
        let result = r.units_in_envelope(envelope);
        assert_eq!(result, vec![UnitId(0)]);
    }

    #[test]
    fn units_in_envelope_outside_is_empty() {
        let r = make_two_unit_region();
        let envelope = Rect::new(Coord { x: 5.0, y: 5.0 }, Coord { x: 6.0, y: 6.0 });
        let result = r.units_in_envelope(envelope);
        assert!(result.is_empty());
    }
}
