use crate::unit::UnitId;

use super::Region;

impl Region {
    /// Returns `true` if all units in `units` form a single connected component
    /// under Rook adjacency.
    pub fn is_contiguous(&self, units: impl IntoIterator<Item = UnitId>) -> bool {
        todo!()
    }

    /// Partition `units` into maximal connected components under Rook
    /// adjacency.
    pub fn connected_components(&self, units: impl IntoIterator<Item = UnitId>) -> Vec<Vec<UnitId>> {
        todo!()
    }

    /// Returns `true` if the complement of `units` contains any component
    /// entirely surrounded by `units` (i.e. not adjacent to the exterior).
    pub fn has_holes(&self, units: impl IntoIterator<Item = UnitId>) -> bool {
        todo!()
    }

    /// Returns each connected component of the complement of `units` that is
    /// entirely surrounded by `units` (not adjacent to the exterior).
    pub fn enclaves(&self, units: impl IntoIterator<Item = UnitId>) -> Vec<Vec<UnitId>> {
        todo!()
    }
}
