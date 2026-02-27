use crate::adj::AdjacencyMatrix;
use crate::unit::UnitId;

use super::Region;

impl Region {
    /// Returns `true` if `a` and `b` share a positive-length boundary segment
    /// (Rook adjacency).
    pub fn are_adjacent(&self, a: UnitId, b: UnitId) -> bool {
        todo!()
    }

    /// Sorted slice of Rook-adjacent units for `unit`.
    pub fn neighbors(&self, unit: UnitId) -> &[UnitId] {
        todo!()
    }

    /// The Rook (shared-edge) adjacency matrix.  Built lazily on first call.
    pub fn adjacency(&self) -> &AdjacencyMatrix {
        todo!()
    }

    /// The Queen (shared-point) adjacency matrix.  Built lazily on first call.
    /// Rook adjacency ⊆ Queen adjacency.
    pub fn touching(&self) -> &AdjacencyMatrix {
        todo!()
    }
}
