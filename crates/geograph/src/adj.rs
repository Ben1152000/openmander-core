use crate::unit::UnitId;

/// A read-only CSR (Compressed Sparse Row) adjacency matrix over units.
///
/// `offsets[u]..offsets[u+1]` indexes into `neighbors` to give the sorted
/// list of units adjacent to unit `u`.  Supports O(log deg) membership tests
/// via binary search.
pub struct AdjacencyMatrix {
    /// CSR row offsets; length = `num_units + 1`.
    offsets: Vec<u32>,
    /// Flattened neighbor lists; sorted within each row.
    neighbors: Vec<UnitId>,
}

impl AdjacencyMatrix {
    /// Build a CSR adjacency matrix from a list of directed `(row, col)` pairs.
    ///
    /// `num_units` sets the number of rows.  Pairs where either element equals
    /// `UnitId::EXTERIOR` are silently dropped; the remaining pairs are sorted
    /// and deduplicated before building the CSR.
    pub(crate) fn from_directed_pairs(
        num_units: usize,
        mut pairs: Vec<(UnitId, UnitId)>,
    ) -> Self {
        // Drop any pair involving EXTERIOR (cannot be a CSR row/column index).
        pairs.retain(|&(u, v)| u != UnitId::EXTERIOR && v != UnitId::EXTERIOR);
        pairs.sort_unstable();
        pairs.dedup();

        let mut offsets   = vec![0u32; num_units + 1];
        let mut neighbors: Vec<UnitId> = Vec::with_capacity(pairs.len());

        // Count neighbors per row.
        for &(u, _) in &pairs {
            offsets[u.0 as usize + 1] += 1;
        }
        // Prefix-sum.
        for i in 1..=num_units {
            offsets[i] += offsets[i - 1];
        }
        // Fill neighbor lists (pairs are already sorted by (row, col)).
        for &(_, v) in &pairs {
            neighbors.push(v);
        }
        Self { offsets, neighbors }
    }

    /// Number of units covered by this matrix.
    #[inline]
    pub fn num_units(&self) -> usize { self.offsets.len() - 1 }

    /// Sorted slice of units adjacent to `unit`.
    #[inline]
    pub fn neighbors(&self, unit: UnitId) -> &[UnitId] {
        let start = self.offsets[unit.0 as usize] as usize;
        let end   = self.offsets[unit.0 as usize + 1] as usize;
        &self.neighbors[start..end]
    }

    /// Returns `true` if `other` is adjacent to `unit` (binary search).
    #[inline]
    pub fn contains(&self, unit: UnitId, other: UnitId) -> bool {
        self.neighbors(unit).binary_search(&other).is_ok()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    /// Build an `AdjacencyMatrix` from a slice of neighbor lists.
    /// Each inner list must already be **sorted** and contain raw `u32` unit
    /// indices (no `UnitId::EXTERIOR`).  Panics if the total count overflows
    /// `u32`.
    fn make(lists: &[&[u32]]) -> AdjacencyMatrix {
        let mut offsets = Vec::with_capacity(lists.len() + 1);
        let mut neighbors: Vec<UnitId> = Vec::new();
        offsets.push(0u32);
        for list in lists {
            neighbors.extend(list.iter().map(|&x| UnitId(x)));
            offsets.push(neighbors.len() as u32);
        }
        AdjacencyMatrix { offsets, neighbors }
    }

    // -----------------------------------------------------------------------
    // num_units
    // -----------------------------------------------------------------------

    #[test]
    fn num_units_empty_matrix() {
        assert_eq!(make(&[]).num_units(), 0);
    }

    #[test]
    fn num_units_single_isolated() {
        assert_eq!(make(&[&[]]).num_units(), 1);
    }

    #[test]
    fn num_units_matches_row_count() {
        // 5 rows regardless of edge count
        let m = make(&[&[1], &[0, 2], &[1, 3], &[2, 4], &[3]]);
        assert_eq!(m.num_units(), 5);
    }

    // -----------------------------------------------------------------------
    // neighbors
    // -----------------------------------------------------------------------

    #[test]
    fn neighbors_of_isolated_unit_is_empty() {
        let m = make(&[&[], &[], &[]]);
        assert!(m.neighbors(UnitId(0)).is_empty());
        assert!(m.neighbors(UnitId(1)).is_empty());
        assert!(m.neighbors(UnitId(2)).is_empty());
    }

    #[test]
    fn neighbors_path_graph() {
        // 0 — 1 — 2
        let m = make(&[&[1], &[0, 2], &[1]]);
        assert_eq!(m.neighbors(UnitId(0)), &[UnitId(1)]);
        assert_eq!(m.neighbors(UnitId(1)), &[UnitId(0), UnitId(2)]);
        assert_eq!(m.neighbors(UnitId(2)), &[UnitId(1)]);
    }

    #[test]
    fn neighbors_first_and_last_unit() {
        // Cycle: 0 — 1 — 2 — 3 — 0
        let m = make(&[&[1, 3], &[0, 2], &[1, 3], &[0, 2]]);
        assert_eq!(m.neighbors(UnitId(0)), &[UnitId(1), UnitId(3)]);
        assert_eq!(m.neighbors(UnitId(3)), &[UnitId(0), UnitId(2)]);
    }

    #[test]
    fn neighbors_are_sorted() {
        // Complete graph K4: each row must be strictly ascending.
        let m = make(&[&[1, 2, 3], &[0, 2, 3], &[0, 1, 3], &[0, 1, 2]]);
        for u in 0..4u32 {
            let ns = m.neighbors(UnitId(u));
            for w in ns.windows(2) {
                assert!(w[0] < w[1], "row {u} is not sorted");
            }
        }
    }

    #[test]
    fn neighbors_rows_are_independent() {
        // Two disconnected triangles: 0-1-2 and 3-4-5.
        // Unit 0's neighbors {1,2} and unit 3's neighbors {4,5} are disjoint,
        // verifying that each row returns data only for that unit.
        let m = make(&[
            &[1, 2], &[0, 2], &[0, 1],   // triangle 0-1-2
            &[4, 5], &[3, 5], &[3, 4],   // triangle 3-4-5
        ]);
        let n0 = m.neighbors(UnitId(0));
        let n3 = m.neighbors(UnitId(3));
        for x in n0 {
            assert!(!n3.contains(x));
        }
    }

    // -----------------------------------------------------------------------
    // contains
    // -----------------------------------------------------------------------

    #[test]
    fn contains_true_for_known_neighbor() {
        let m = make(&[&[1], &[0, 2], &[1]]);
        assert!(m.contains(UnitId(0), UnitId(1)));
        assert!(m.contains(UnitId(1), UnitId(0)));
        assert!(m.contains(UnitId(1), UnitId(2)));
        assert!(m.contains(UnitId(2), UnitId(1)));
    }

    #[test]
    fn contains_false_for_non_neighbor() {
        // 0 and 2 are two hops apart in the path graph.
        let m = make(&[&[1], &[0, 2], &[1]]);
        assert!(!m.contains(UnitId(0), UnitId(2)));
        assert!(!m.contains(UnitId(2), UnitId(0)));
    }

    #[test]
    fn contains_false_for_isolated_unit() {
        let m = make(&[&[], &[]]);
        assert!(!m.contains(UnitId(0), UnitId(1)));
        assert!(!m.contains(UnitId(1), UnitId(0)));
    }

    #[test]
    fn contains_no_self_loops() {
        let m = make(&[&[1, 2], &[0, 2], &[0, 1]]);
        for u in 0..3u32 {
            assert!(!m.contains(UnitId(u), UnitId(u)));
        }
    }

    #[test]
    fn contains_is_symmetric() {
        // For every edge (u, v) stored, (v, u) must also be stored.
        let m = make(&[&[1, 2, 3], &[0, 2, 3], &[0, 1, 3], &[0, 1, 2]]);
        for u in 0..4u32 {
            let row: Vec<UnitId> = m.neighbors(UnitId(u)).to_vec();
            for v in row {
                assert!(m.contains(v, UnitId(u)),
                    "symmetry broken: {u} -> {} but not the reverse", v.0);
            }
        }
    }

    #[test]
    fn contains_complete_graph() {
        // K4: every pair is adjacent.
        let m = make(&[&[1, 2, 3], &[0, 2, 3], &[0, 1, 3], &[0, 1, 2]]);
        for u in 0..4u32 {
            for v in 0..4u32 {
                if u == v {
                    assert!(!m.contains(UnitId(u), UnitId(v)));
                } else {
                    assert!(m.contains(UnitId(u), UnitId(v)));
                }
            }
        }
    }

    /// Star graph with 20 leaves exercises the binary-search path through
    /// a long neighbor list (hub row has 20 entries).
    #[test]
    fn contains_large_neighbor_list() {
        // Hub = UnitId(0), leaves = UnitId(1)..UnitId(20)
        let hub_row: Vec<u32> = (1..=20).collect();
        let leaf_row = [0u32];
        let mut lists: Vec<Vec<u32>> = vec![hub_row];
        for _ in 0..20 {
            lists.push(leaf_row.to_vec());
        }
        let refs: Vec<&[u32]> = lists.iter().map(Vec::as_slice).collect();
        let m = make(&refs);

        for leaf in 1u32..=20 {
            assert!(m.contains(UnitId(0), UnitId(leaf)));
            assert!(m.contains(UnitId(leaf), UnitId(0)));
        }
        // Hub is not its own neighbor; leaves are not adjacent to each other.
        assert!(!m.contains(UnitId(0), UnitId(0)));
        assert!(!m.contains(UnitId(1), UnitId(2)));
    }
}
