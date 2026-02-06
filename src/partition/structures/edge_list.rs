/// A per-part bucketed set for directed half-edges, with O(1) insert/remove/find.
///
/// Intended use (OpenMander):
/// - Each undirected adjacency edge (u, v) has two directed half-edges: (u→v) and (v→u)
/// - A directed half-edge belongs to exactly one *part* (district) iff it lies on that part's frontier.
/// - Membership is updated incrementally during node flips (O(deg(u))).
///
/// Representation:
/// - `parts[p]` stores the list of half-edges currently assigned to part p
/// - `loc[he] = Some((p, i))` means half-edge `he` is stored at `parts[p][i]`
///
/// Removal is swap-remove, so per-part order is not preserved.
#[derive(Debug, Clone)]
pub(crate) struct FrontierEdgeList {
    parts: Vec<Vec<usize>>,
    loc: Vec<Option<(usize, usize)>>, // loc[he] = Some((part, pos))
}

impl FrontierEdgeList {
    /// Create an empty frontier list with `num_parts` parts and `num_edges` edges in the graph.
    pub(crate) fn new(num_parts: usize, num_edges: usize) -> Self {
        // Heuristic pre-allocation; same idea as your MultiSet.
        let cap = (num_edges * 2 / num_parts.max(1)).isqrt().saturating_add(1);
        Self {
            parts: (0..num_parts).map(|_| Vec::with_capacity(cap)).collect(),
            loc: vec![None; num_edges * 2],
        }
    }

    #[inline] pub(crate) fn num_parts(&self) -> usize { self.parts.len() }

    #[inline] pub(crate) fn num_directed_edges(&self) -> usize { self.loc.len() }

    #[inline] fn check_part(&self, part: usize) { debug_assert!(part < self.parts.len(), "part id out of range") }

    #[inline] fn check_directed_edge(&self, edge: usize) { debug_assert!(edge < self.loc.len(), "edge id out of range") }

    /// Returns the part that `edge` is currently assigned to, or `None` if absent.
    #[inline]
    pub(crate) fn find(&self, edge: usize) -> Option<usize> {
        self.check_directed_edge(edge);
        self.loc[edge].map(|(part, _)| part)
    }

    /// Returns true if `edge` is in any part.
    #[inline] pub(crate) fn contains(&self, edge: usize) -> bool { self.find(edge).is_some() }

    /// Read-only view of half-edges in `part`.
    #[inline]
    pub(crate) fn get(&self, part: usize) -> &[usize] {
        self.check_part(part);
        &self.parts[part]
    }

    /// Returns true if `part` currently has no frontier half-edges.
    #[inline]
    pub(crate) fn is_empty_part(&self, part: usize) -> bool {
        self.get(part).is_empty()
    }

    /// Iterator over each part as a slice.
    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &[usize]> + '_ {
        self.parts.iter().map(|v| v.as_slice())
    }

    /// Iterator over all half-edges present (across all parts).
    #[inline]
    pub(crate) fn iter_all(&self) -> impl Iterator<Item = usize> + '_ {
        self.parts.iter().flat_map(|v| v.iter().copied())
    }

    /// Remove all half-edges from all parts (O(total_size)).
    pub(crate) fn clear(&mut self) {
        for part in &mut self.parts { part.clear() }
        self.loc.fill(None);
    }

    /// Rebuild from an iterator of (half_edge, part) pairs.
    /// Half-edges not mentioned end up absent.
    ///
    /// Panics in debug if ids are out of range or a half-edge appears more than once.
    pub(crate) fn rebuild_from<I>(&mut self, iter: I)
    where I: IntoIterator<Item = (usize, usize)>,
    {
        self.clear();
        for (edge, part) in iter {
            self.check_directed_edge(edge);
            self.check_part(part);
            debug_assert!(self.loc[edge].is_none(), "half-edge listed multiple times in rebuild");
            self.insert_unchecked(edge, part);
        }
    }

    /// Insert `edge` into `part`. If `edge` is already present in a different part, it is moved.
    pub(crate) fn insert(&mut self, edge: usize, part: usize) {
        self.check_directed_edge(edge);
        self.check_part(part);

        match self.loc[edge] {
            Some((cur, _)) if cur == part => { /* already correct */ }
            Some(_) => {
                self.remove(edge);
                self.insert_unchecked(edge, part);
            }
            None => self.insert_unchecked(edge, part),
        }
    }

    /// Remove `edge` from whichever part it is in (no-op if absent).
    pub(crate) fn remove(&mut self, edge: usize) {
        self.check_directed_edge(edge);
        if let Some((part, pos)) = self.loc[edge] {
            let bucket = &mut self.parts[part];
            let last = bucket.pop().expect("loc said present, but bucket empty");
            if pos < bucket.len() {
                bucket[pos] = last;
                self.loc[last] = Some((part, pos));
            }
            self.loc[edge] = None;
        }
    }

    #[inline]
    fn insert_unchecked(&mut self, edge: usize, part: usize) {
        // Caller must have validated `he` and `part` and ensured `he` is absent.
        let bucket = &mut self.parts[part];
        self.loc[edge] = Some((part, bucket.len()));
        bucket.push(edge);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty_parts() {
        let fel = FrontierEdgeList::new(4, 10);
        assert_eq!(fel.num_parts(), 4);
        assert_eq!(fel.num_directed_edges(), 20); // 10 edges * 2 directions
        for p in 0..4 {
            assert!(fel.is_empty_part(p));
        }
    }

    #[test]
    fn insert_and_find() {
        let mut fel = FrontierEdgeList::new(3, 5);

        fel.insert(0, 1);
        fel.insert(2, 1);
        fel.insert(4, 2);

        assert_eq!(fel.find(0), Some(1));
        assert_eq!(fel.find(2), Some(1));
        assert_eq!(fel.find(4), Some(2));
        assert_eq!(fel.find(1), None);
        assert_eq!(fel.find(3), None);

        assert!(fel.contains(0));
        assert!(!fel.contains(1));
    }

    #[test]
    fn insert_moves_between_parts() {
        let mut fel = FrontierEdgeList::new(3, 5);

        fel.insert(0, 1);
        assert_eq!(fel.find(0), Some(1));
        assert_eq!(fel.get(1).len(), 1);

        // Move to different part
        fel.insert(0, 2);
        assert_eq!(fel.find(0), Some(2));
        assert_eq!(fel.get(1).len(), 0);
        assert_eq!(fel.get(2).len(), 1);
    }

    #[test]
    fn remove_works() {
        let mut fel = FrontierEdgeList::new(3, 5);

        fel.insert(0, 1);
        fel.insert(2, 1);
        fel.insert(4, 1);

        assert_eq!(fel.get(1).len(), 3);

        fel.remove(2);
        assert_eq!(fel.find(2), None);
        assert_eq!(fel.get(1).len(), 2);

        // Removing non-existent edge is a no-op
        fel.remove(2);
        assert_eq!(fel.get(1).len(), 2);
    }

    #[test]
    fn clear_empties_all() {
        let mut fel = FrontierEdgeList::new(3, 5);

        fel.insert(0, 0);
        fel.insert(2, 1);
        fel.insert(4, 2);

        fel.clear();

        for p in 0..3 {
            assert!(fel.is_empty_part(p));
        }
        assert!(!fel.contains(0));
        assert!(!fel.contains(2));
        assert!(!fel.contains(4));
    }

    #[test]
    fn rebuild_from_iterator() {
        let mut fel = FrontierEdgeList::new(3, 5);

        // Add some initial data
        fel.insert(0, 0);
        fel.insert(1, 1);

        // Rebuild with new data
        fel.rebuild_from(vec![(2, 0), (4, 1), (6, 2)]);

        assert_eq!(fel.find(0), None); // Old data cleared
        assert_eq!(fel.find(1), None);
        assert_eq!(fel.find(2), Some(0));
        assert_eq!(fel.find(4), Some(1));
        assert_eq!(fel.find(6), Some(2));
    }

    #[test]
    fn get_returns_correct_edges() {
        let mut fel = FrontierEdgeList::new(3, 10);

        fel.insert(0, 1);
        fel.insert(2, 1);
        fel.insert(4, 1);
        fel.insert(6, 2);

        let part1_edges: std::collections::HashSet<usize> = fel.get(1).iter().copied().collect();
        assert_eq!(part1_edges, [0, 2, 4].into_iter().collect());

        let part2_edges: std::collections::HashSet<usize> = fel.get(2).iter().copied().collect();
        assert_eq!(part2_edges, [6].into_iter().collect());
    }

    #[test]
    fn iter_all_returns_all_edges() {
        let mut fel = FrontierEdgeList::new(3, 10);

        fel.insert(0, 0);
        fel.insert(2, 1);
        fel.insert(4, 2);

        let all_edges: std::collections::HashSet<usize> = fel.iter_all().collect();
        assert_eq!(all_edges, [0, 2, 4].into_iter().collect());
    }

    #[test]
    fn swap_remove_maintains_consistency() {
        let mut fel = FrontierEdgeList::new(2, 10);

        // Insert several edges
        fel.insert(0, 0);
        fel.insert(2, 0);
        fel.insert(4, 0);
        fel.insert(6, 0);

        // Remove from middle (triggers swap-remove)
        fel.remove(2);

        // All remaining edges should still be findable
        assert_eq!(fel.find(0), Some(0));
        assert_eq!(fel.find(2), None);
        assert_eq!(fel.find(4), Some(0));
        assert_eq!(fel.find(6), Some(0));

        // Part should have exactly 3 edges
        assert_eq!(fel.get(0).len(), 3);

        // All edges in part should be valid
        for &edge in fel.get(0) {
            assert_eq!(fel.find(edge), Some(0));
        }
    }
}
