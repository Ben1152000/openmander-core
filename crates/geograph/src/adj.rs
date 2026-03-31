use crate::unit::UnitId;

/// A read-only CSR (Compressed Sparse Row) adjacency matrix over units.
///
/// `offsets[u]..offsets[u+1]` indexes into `neighbors` to give the sorted
/// list of units adjacent to unit `u`.  Supports O(log deg) membership tests
/// via binary search.
#[derive(Clone)]
pub struct AdjacencyMatrix {
    /// CSR row offsets; length = `num_units + 1`.
    offsets: Vec<u32>,
    /// Flattened neighbor lists; sorted within each row.
    neighbors: Vec<UnitId>,
    /// Optional per-edge weights, aligned to `neighbors`.
    /// When present, `weights[i]` is the weight of the directed edge
    /// at position `i` in the flattened neighbor array.
    weights: Option<Vec<f64>>,
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
        Self { offsets, neighbors, weights: None }
    }

    /// Build a CSR adjacency matrix from a list of directed `(row, col, weight)`
    /// triples.
    ///
    /// `num_units` sets the number of rows.  Triples where either unit equals
    /// `UnitId::EXTERIOR` are silently dropped.  Duplicate `(row, col)` pairs
    /// have their weights **summed**.  The resulting CSR rows are sorted by
    /// column (neighbor `UnitId`).
    pub(crate) fn from_directed_pairs_weighted(
        num_units: usize,
        mut triples: Vec<(UnitId, UnitId, f64)>,
    ) -> Self {
        // Drop any triple involving EXTERIOR.
        triples.retain(|&(u, v, _)| u != UnitId::EXTERIOR && v != UnitId::EXTERIOR);
        // Sort by (row, col) so we can merge duplicates.
        triples.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));

        // Merge duplicates: sum weights for identical (row, col).
        let mut merged: Vec<(UnitId, UnitId, f64)> = Vec::with_capacity(triples.len());
        for (u, v, w) in triples {
            if let Some(last) = merged.last_mut()
                && last.0 == u && last.1 == v {
                    last.2 += w;
                    continue;
                }
            merged.push((u, v, w));
        }

        let mut offsets = vec![0u32; num_units + 1];
        let mut neighbors: Vec<UnitId> = Vec::with_capacity(merged.len());
        let mut weights: Vec<f64> = Vec::with_capacity(merged.len());

        // Count neighbors per row.
        for &(u, _, _) in &merged {
            offsets[u.0 as usize + 1] += 1;
        }
        // Prefix-sum.
        for i in 1..=num_units {
            offsets[i] += offsets[i - 1];
        }
        // Fill neighbor and weight lists (already sorted by (row, col)).
        for &(_, v, w) in &merged {
            neighbors.push(v);
            weights.push(w);
        }
        Self { offsets, neighbors, weights: Some(weights) }
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

    // -----------------------------------------------------------------------
    // CSR edge indexing
    // -----------------------------------------------------------------------

    /// Total number of directed edges (entries in the neighbor array).
    #[inline]
    pub fn num_directed_edges(&self) -> usize { self.neighbors.len() }

    /// CSR start offset for `unit`'s neighbor list.
    ///
    /// The directed edges of `unit` occupy indices
    /// `offset(unit)..offset(unit) + degree(unit)` in the flat neighbor array.
    #[inline]
    pub fn offset(&self, unit: UnitId) -> usize {
        self.offsets[unit.0 as usize] as usize
    }

    /// Number of neighbors (degree) of `unit`.
    #[inline]
    pub fn degree(&self, unit: UnitId) -> usize {
        let u = unit.0 as usize;
        (self.offsets[u + 1] - self.offsets[u]) as usize
    }

    /// Recover the `(source, target)` pair for a flat directed-edge index.
    ///
    /// Returns `None` if `edge_idx` is out of range.
    pub fn edge_at(&self, edge_idx: usize) -> Option<(UnitId, UnitId)> {
        if edge_idx >= self.neighbors.len() { return None; }
        // Binary search offsets to find the source unit.
        let nu = self.num_units();
        let mut lo = 0;
        let mut hi = nu;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if (self.offsets[mid + 1] as usize) <= edge_idx {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some((UnitId(lo as u32), self.neighbors[edge_idx]))
    }

    /// Target unit at the given flat directed-edge index.
    ///
    /// This is faster than `edge_at()` when the source unit is already known.
    #[inline]
    pub fn target_at(&self, edge_idx: usize) -> UnitId {
        self.neighbors[edge_idx]
    }

    // -----------------------------------------------------------------------
    // Edge weights
    // -----------------------------------------------------------------------

    /// Returns `true` if this matrix has per-edge weights.
    #[inline]
    pub fn has_weights(&self) -> bool { self.weights.is_some() }

    /// Weight at the given flat directed-edge index.
    ///
    /// Returns `0.0` if no weights are stored.
    #[inline]
    pub fn weight_at(&self, edge_idx: usize) -> f64 {
        self.weights.as_ref().map_or(0.0, |w| w[edge_idx])
    }

    /// Return a new `AdjacencyMatrix` with all existing edges plus the given
    /// undirected pairs (both directions added) with weight `0.0`.
    ///
    /// Pairs where either unit is `UnitId::EXTERIOR` are silently dropped.
    /// If a forced pair already exists, its weight is unchanged (0.0 is added
    /// to the existing weight via the duplicate-merge in `from_directed_pairs_weighted`).
    pub(crate) fn with_extra_edges(self, extra: &[(UnitId, UnitId)]) -> Self {
        if extra.is_empty() { return self; }

        let nu = self.num_units();
        let mut triples: Vec<(UnitId, UnitId, f64)> =
            Vec::with_capacity(self.neighbors.len() + extra.len() * 2);

        // Preserve all existing edges with their current weights.
        for u in 0..nu {
            let uid = UnitId(u as u32);
            let start = self.offsets[u] as usize;
            let end   = self.offsets[u + 1] as usize;
            for i in start..end {
                let w = self.weights.as_ref().map_or(0.0, |ws| ws[i]);
                triples.push((uid, self.neighbors[i], w));
            }
        }

        // Add forced pairs in both directions with weight 0.0.
        for &(a, b) in extra {
            if a == UnitId::EXTERIOR || b == UnitId::EXTERIOR { continue; }
            triples.push((a, b, 0.0));
            triples.push((b, a, 0.0));
        }

        Self::from_directed_pairs_weighted(nu, triples)
    }

    /// Approximate heap bytes consumed by this matrix.
    pub(crate) fn heap_bytes(&self) -> usize {
        self.offsets.capacity()   * std::mem::size_of::<u32>()
        + self.neighbors.capacity() * std::mem::size_of::<UnitId>()
        + self.weights.as_ref().map_or(0, |w| w.capacity() * std::mem::size_of::<f64>())
    }

    /// Slice of weights for `unit`'s neighbors, aligned to `neighbors(unit)`.
    ///
    /// Returns an empty slice if no weights are stored.
    #[inline]
    pub fn weights_of(&self, unit: UnitId) -> &[f64] {
        match &self.weights {
            Some(w) => {
                let start = self.offsets[unit.0 as usize] as usize;
                let end   = self.offsets[unit.0 as usize + 1] as usize;
                &w[start..end]
            }
            None => &[],
        }
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
        AdjacencyMatrix { offsets, neighbors, weights: None }
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

    // -----------------------------------------------------------------------
    // CSR edge indexing
    // -----------------------------------------------------------------------

    #[test]
    fn num_directed_edges_matches_total_neighbors() {
        let m = make(&[&[1, 2], &[0, 2], &[0, 1]]);
        assert_eq!(m.num_directed_edges(), 6); // K3: 3 units × 2 neighbors each
    }

    #[test]
    fn offset_and_degree_are_consistent() {
        let m = make(&[&[1], &[0, 2], &[1]]);
        assert_eq!(m.offset(UnitId(0)), 0);
        assert_eq!(m.degree(UnitId(0)), 1);
        assert_eq!(m.offset(UnitId(1)), 1);
        assert_eq!(m.degree(UnitId(1)), 2);
        assert_eq!(m.offset(UnitId(2)), 3);
        assert_eq!(m.degree(UnitId(2)), 1);
    }

    #[test]
    fn edge_at_recovers_source_and_target() {
        // Path: 0—1—2
        let m = make(&[&[1], &[0, 2], &[1]]);
        // Flat layout: [1, 0, 2, 1]
        assert_eq!(m.edge_at(0), Some((UnitId(0), UnitId(1))));
        assert_eq!(m.edge_at(1), Some((UnitId(1), UnitId(0))));
        assert_eq!(m.edge_at(2), Some((UnitId(1), UnitId(2))));
        assert_eq!(m.edge_at(3), Some((UnitId(2), UnitId(1))));
        assert_eq!(m.edge_at(4), None);
    }

    #[test]
    fn target_at_matches_neighbors() {
        let m = make(&[&[1, 2], &[0, 2], &[0, 1]]);
        for u in 0..3u32 {
            let uid = UnitId(u);
            for i in 0..m.degree(uid) {
                let idx = m.offset(uid) + i;
                assert_eq!(m.target_at(idx), m.neighbors(uid)[i]);
            }
        }
    }

    #[test]
    fn edge_at_out_of_range_returns_none() {
        let m = make(&[&[1], &[0]]);
        assert_eq!(m.edge_at(2), None);
        assert_eq!(m.edge_at(100), None);
    }

    // -----------------------------------------------------------------------
    // from_directed_pairs_weighted
    // -----------------------------------------------------------------------

    #[test]
    fn weighted_basic_construction() {
        let triples = vec![
            (UnitId(0), UnitId(1), 2.5),
            (UnitId(1), UnitId(0), 2.5),
        ];
        let m = AdjacencyMatrix::from_directed_pairs_weighted(2, triples);
        assert_eq!(m.num_units(), 2);
        assert!(m.has_weights());
        assert_eq!(m.neighbors(UnitId(0)), &[UnitId(1)]);
        assert_eq!(m.weight_at(0), 2.5);
        assert_eq!(m.weights_of(UnitId(0)), &[2.5]);
    }

    #[test]
    fn weighted_sums_duplicates() {
        let triples = vec![
            (UnitId(0), UnitId(1), 1.0),
            (UnitId(0), UnitId(1), 3.0),
            (UnitId(1), UnitId(0), 4.0),
        ];
        let m = AdjacencyMatrix::from_directed_pairs_weighted(2, triples);
        assert_eq!(m.weights_of(UnitId(0)), &[4.0]); // 1.0 + 3.0
        assert_eq!(m.weights_of(UnitId(1)), &[4.0]);
    }

    #[test]
    fn weighted_drops_exterior() {
        let triples = vec![
            (UnitId(0), UnitId::EXTERIOR, 1.0),
            (UnitId::EXTERIOR, UnitId(0), 1.0),
            (UnitId(0), UnitId(1), 5.0),
            (UnitId(1), UnitId(0), 5.0),
        ];
        let m = AdjacencyMatrix::from_directed_pairs_weighted(2, triples);
        assert_eq!(m.num_directed_edges(), 2);
        assert_eq!(m.weight_at(0), 5.0);
    }

    #[test]
    fn weighted_aligned_with_neighbors() {
        // Path: 0—1—2
        let triples = vec![
            (UnitId(0), UnitId(1), 1.0),
            (UnitId(1), UnitId(0), 1.0),
            (UnitId(1), UnitId(2), 2.0),
            (UnitId(2), UnitId(1), 2.0),
        ];
        let m = AdjacencyMatrix::from_directed_pairs_weighted(3, triples);
        // Node 1's neighbors sorted: [0, 2]
        assert_eq!(m.neighbors(UnitId(1)), &[UnitId(0), UnitId(2)]);
        assert_eq!(m.weights_of(UnitId(1)), &[1.0, 2.0]);
    }

    // -----------------------------------------------------------------------
    // weight accessors on unweighted matrix
    // -----------------------------------------------------------------------

    #[test]
    fn unweighted_has_no_weights() {
        let m = make(&[&[1], &[0]]);
        assert!(!m.has_weights());
        assert_eq!(m.weight_at(0), 0.0);
        assert!(m.weights_of(UnitId(0)).is_empty());
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
