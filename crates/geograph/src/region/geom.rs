use std::collections::HashSet;

use geo::{Coord, LineString, MultiLineString, MultiPolygon, Rect};

use crate::dcel::FaceId;
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
            let start = match self.dcel.face(FaceId(f)).half_edge {
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
            let start = match self.dcel.face(FaceId(f)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(self.dcel.half_edge(he).twin).face;
                if !set.contains(&self.face_to_unit[twin_face.0]) {
                    total += self.edge_length[he.0 / 2];
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

    /// Exterior boundary of the subset as a `MultiLineString`.
    /// Equivalent to the outline of the merged shape without polygon union.
    pub fn boundary_of(&self, _units: impl IntoIterator<Item = UnitId>) -> MultiLineString<f64> {
        todo!("boundary_of: boundary cycle tracing not yet implemented")
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
    pub fn union_of(&self, _units: impl IntoIterator<Item = UnitId>) -> MultiPolygon<f64> {
        todo!("union_of: polygon boolean union not yet implemented")
    }

    // -----------------------------------------------------------------------
    // Edge metrics
    // -----------------------------------------------------------------------

    /// Length of the shared boundary between units `a` and `b`, in m.
    /// Returns `0.0` if the units are not Rook-adjacent.
    pub fn shared_boundary_length(&self, a: UnitId, b: UnitId) -> f64 {
        self.sum_edge_lengths_between(a, b)
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
            let start = match self.dcel.face(FaceId(f)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(self.dcel.half_edge(he).twin).face;
                if other_set.contains(&self.face_to_unit[twin_face.0]) {
                    total += self.edge_length[he.0 / 2];
                }
            }
        }
        total
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
            let start = match self.dcel.face(FaceId(f)).half_edge {
                Some(he) => he,
                None => continue,
            };
            for he in self.dcel.face_cycle(start) {
                let twin_face = self.dcel.half_edge(self.dcel.half_edge(he).twin).face;
                if self.face_to_unit[twin_face.0] == unit_b {
                    total += self.edge_length[he.0 / 2];
                }
            }
        }
        total
    }

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
}
