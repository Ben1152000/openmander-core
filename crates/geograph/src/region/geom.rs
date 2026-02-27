use geo::{Coord, MultiLineString, MultiPolygon, Rect};

use crate::unit::UnitId;

use super::Region;

impl Region {
    // -----------------------------------------------------------------------
    // Single-unit geometry  (O(1), pre-cached)
    // -----------------------------------------------------------------------

    /// Area of `unit` in m².
    pub fn area(&self, unit: UnitId) -> f64 {
        todo!()
    }

    /// Total perimeter of `unit` in m (includes hole boundaries).
    pub fn perimeter(&self, unit: UnitId) -> f64 {
        todo!()
    }

    /// Length of `unit`'s boundary that touches the region exterior, in m.
    pub fn exterior_boundary_length(&self, unit: UnitId) -> f64 {
        todo!()
    }

    /// Centroid of `unit` in lon/lat.
    pub fn centroid(&self, unit: UnitId) -> Coord<f64> {
        todo!()
    }

    /// Axis-aligned bounding box of `unit` in lon/lat.
    pub fn bounds(&self, unit: UnitId) -> Rect<f64> {
        todo!()
    }

    /// Returns `true` if `unit` has any boundary with the region exterior.
    pub fn is_exterior(&self, unit: UnitId) -> bool {
        todo!()
    }

    /// Boundary of `unit` as a `MultiLineString` (one ring per face).
    pub fn boundary(&self, unit: UnitId) -> MultiLineString<f64> {
        todo!()
    }

    // -----------------------------------------------------------------------
    // Subset geometry  (O(k) unless noted)
    // -----------------------------------------------------------------------

    /// Sum of areas of all units in `units`, in m².
    pub fn area_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        todo!()
    }

    /// Total exterior perimeter of the subset `units`, in m.
    /// Shared internal edges are excluded.
    pub fn perimeter_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        todo!()
    }

    /// Total length of the subset boundary that touches the region exterior,
    /// in m.
    pub fn exterior_boundary_length_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        todo!()
    }

    /// Smallest bounding box containing all units in `units`.
    pub fn bounds_of(&self, units: impl IntoIterator<Item = UnitId>) -> Rect<f64> {
        todo!()
    }

    /// Exterior boundary of the subset as a `MultiLineString`.
    /// Equivalent to the outline of the merged shape without polygon union.
    pub fn boundary_of(&self, units: impl IntoIterator<Item = UnitId>) -> MultiLineString<f64> {
        todo!()
    }

    /// Polsby-Popper compactness score for the subset: `4π·area / perimeter²`.
    pub fn compactness_of(&self, units: impl IntoIterator<Item = UnitId>) -> f64 {
        todo!()
    }

    /// Geometric union of all unit polygons in `units`.
    ///
    /// Cost depends on polygon complexity of the subset; prefer `boundary_of`
    /// when only the outline is needed.
    pub fn union_of(&self, units: impl IntoIterator<Item = UnitId>) -> MultiPolygon<f64> {
        todo!()
    }

    // -----------------------------------------------------------------------
    // Edge metrics
    // -----------------------------------------------------------------------

    /// Length of the shared boundary between units `a` and `b`, in m.
    /// Returns `0.0` if the units are not Rook-adjacent.
    pub fn shared_boundary_length(&self, a: UnitId, b: UnitId) -> f64 {
        todo!()
    }

    /// Total length of the boundary between `units` and `other`, in m.
    pub fn boundary_length_with(&self,
        units: impl IntoIterator<Item = UnitId>,
        other: impl IntoIterator<Item = UnitId>,
    ) -> f64 {
        todo!()
    }
}
