use crate::unit::UnitId;

/// A read-only CSR (Compressed Sparse Row) adjacency matrix over units.
///
/// `offsets[u]..offsets[u+1]` indexes into `neighbors` to give the sorted
/// list of units adjacent to unit `u`.  Supports O(log deg) membership tests
/// via binary search.
///
/// Two matrices are maintained on a `Region`: one for Rook adjacency (shared
/// edge) and one for Queen adjacency (shared point, a superset of Rook).
pub struct AdjacencyMatrix {
    /// CSR row offsets; length = `num_units + 1`.
    offsets: Vec<u32>,
    /// Flattened neighbor lists; sorted within each row.
    neighbors: Vec<u32>,
}

impl AdjacencyMatrix {
    /// Number of units covered by this matrix.
    pub fn num_units(&self) -> usize {
        todo!()
    }

    /// Sorted slice of units adjacent to `unit`.
    pub fn neighbors(&self, unit: UnitId) -> &[UnitId] {
        todo!()
    }

    /// Returns `true` if `other` is adjacent to `unit` (binary search).
    pub fn contains(&self, unit: UnitId, other: UnitId) -> bool {
        todo!()
    }
}
