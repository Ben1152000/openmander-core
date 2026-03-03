use std::collections::HashSet;

use crate::graph::WeightedGraph;

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

    /// Walk the frontier edges of `part` in CCW boundary order, using the
    /// planar embedding encoded in the graph's CCW-sorted adjacency lists.
    ///
    /// Returns a vector of boundary cycles. Each cycle is a sequence of
    /// `(inside_node, outside_node)` directed frontier edges tracing one
    /// connected component of the boundary counter-clockwise.
    ///
    /// The algorithm at each step: starting from frontier edge `(u, v)` with
    /// `u` inside the part, find `v` in `u`'s CCW adjacency list and advance
    /// to the next neighbor. If that neighbor is outside the part, it is the
    /// next frontier edge. If inside, transition to that neighbor and repeat.
    ///
    /// Requires that the graph's adjacency lists were sorted in CCW angular
    /// order during pack construction (see `sort_adjacencies_ccw`).
    pub(crate) fn walk_boundary(
        &self,
        part: usize,
        graph: &WeightedGraph,
        assignments: &[usize],
    ) -> Vec<Vec<(usize, usize)>> {
        let edge_indices = self.get(part);
        if edge_indices.is_empty() {
            return vec![];
        }

        // Collect all frontier edges as (source, target) for this part.
        // Build a set for O(1) visited checks.
        let mut all_frontier: Vec<(usize, usize)> = Vec::with_capacity(edge_indices.len());
        for &edge_idx in edge_indices {
            if let Some(pair) = graph.edge_endpoints(edge_idx) {
                all_frontier.push(pair);
            }
        }

        let frontier_set: HashSet<(usize, usize)> = all_frontier.iter().copied().collect();
        let mut visited: HashSet<(usize, usize)> = HashSet::with_capacity(all_frontier.len());
        let mut cycles: Vec<Vec<(usize, usize)>> = Vec::new();

        for &start in &all_frontier {
            if visited.contains(&start) {
                continue;
            }

            let mut cycle: Vec<(usize, usize)> = Vec::new();
            let (mut u, mut v) = start;

            loop {
                debug_assert_eq!(assignments[u], part);
                debug_assert!(v == u || assignments[v] != part);

                cycle.push((u, v));
                visited.insert((u, v));

                // Find the next frontier edge by walking CCW from v in u's
                // adjacency list, transitioning through interior nodes.
                let (next_u, next_v) = Self::next_frontier_edge(
                    u, v, part, graph, assignments,
                );

                u = next_u;
                v = next_v;

                if (u, v) == start {
                    break;
                }

                // Safety check: if we hit an edge we already visited (but not
                // the start), the walk diverged — break to avoid an infinite loop.
                if visited.contains(&(u, v)) {
                    break;
                }
            }

            // Only keep cycles where every edge is actually a frontier edge
            // of this part (sanity check).
            if cycle.iter().all(|e| frontier_set.contains(e)) {
                cycles.push(cycle);
            }
        }

        cycles
    }

    /// Given frontier edge `(u, v)` with `u` in `part`, find the next frontier
    /// edge in the CCW boundary walk.
    ///
    /// Starting at `u`, advance CCW past `v` in `u`'s adjacency list. If the
    /// next neighbor is outside the part, that's the next frontier edge.
    /// If inside, transition to that neighbor and repeat.
    fn next_frontier_edge(
        u: usize,
        v: usize,
        part: usize,
        graph: &WeightedGraph,
        assignments: &[usize],
    ) -> (usize, usize) {
        let mut cur = u;
        let mut prev = v;

        loop {
            let deg = graph.degree(cur);
            debug_assert!(deg > 0);

            // Find prev's position in cur's adjacency list.
            let mut pos = 0;
            for (i, nbr) in graph.edges(cur).enumerate() {
                if nbr == prev {
                    pos = i;
                    break;
                }
            }

            // Advance one step CCW (next position, wrapping).
            let next_pos = (pos + 1) % deg;
            let w = graph.edge(cur, next_pos).unwrap();

            if w == cur || assignments[w] != part {
                // w is outside the part (or a self-edge exterior sentinel)
                // — found the next frontier edge.
                return (cur, w);
            } else {
                // w is inside — transition to w and continue walking.
                prev = cur;
                cur = w;
            }
        }
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

    /// Build a 3x3 grid graph with CCW-ordered adjacency lists.
    ///
    /// Node layout (column, row):
    ///   6(0,2)  7(1,2)  8(2,2)
    ///   3(0,1)  4(1,1)  5(2,1)
    ///   0(0,0)  1(1,0)  2(2,0)
    ///
    /// CCW adjacency order is by atan2(dy, dx) from node centroid to
    /// the shared-boundary point (here identical to neighbor centroid
    /// for a regular grid).
    fn make_grid_3x3() -> WeightedGraph {
        use crate::graph::WeightMatrix;
        // For a regular grid, CCW order from east going counter-clockwise:
        //   east(0), north(π/2), west(π), south(-π/2)
        // Sorted by atan2: south(-π/2), east(0), north(π/2), west(π)
        //
        // Neighbors for each node (only rook-adjacent), sorted by angle:
        let adj: Vec<Vec<u32>> = vec![
            /* 0 (0,0) */ vec![1, 3],            // east, north
            /* 1 (1,0) */ vec![2, 0, 4],         // S=-π/2 is absent; east=2(0), west=0(π), north=4(π/2) → sorted: 2(0), 4(π/2), 0(π)
            /* 2 (2,0) */ vec![5, 1],            // north=5(π/2), west=1(π) → sorted: 5(π/2), 1(π)
            /* 3 (0,1) */ vec![0, 4, 6],         // south=0(-π/2), east=4(0), north=6(π/2) → sorted: 0(-π/2), 4(0), 6(π/2)
            /* 4 (1,1) */ vec![1, 5, 7, 3],      // south=1(-π/2), east=5(0), north=7(π/2), west=3(π)
            /* 5 (2,1) */ vec![2, 8, 4],         // south=2(-π/2), north=8(π/2), west=4(π)
            /* 6 (0,2) */ vec![3, 7],            // south=3(-π/2), east=7(0)
            /* 7 (1,2) */ vec![6, 4, 8],         // west=6(π), south=4(-π/2), east=8(0) → sorted: 4(-π/2), 8(0), 6(π)
            /* 8 (2,2) */ vec![7, 5],            // west=7(π), south=5(-π/2) → sorted: 5(-π/2), 7(π)
        ];
        let weights: Vec<Vec<f64>> = adj.iter().map(|a| vec![1.0; a.len()]).collect();
        WeightedGraph::new(9, &adj, &weights, WeightMatrix::empty(9), &[])
    }

    #[test]
    fn walk_boundary_single_node_district() {
        let graph = make_grid_3x3();
        // District = {4} (center node), everything else is part 0
        let assignments = [0, 0, 0, 0, 1, 0, 0, 0, 0];

        // Build frontier edges: directed edges from 4 to its neighbors
        // Node 4's adj list: [1, 5, 7, 3], offsets[4] = 2+3+2+3 = 10
        // Actually let's compute offsets properly:
        //   0: 2 edges → offsets[0]=0
        //   1: 3 edges → offsets[1]=2
        //   2: 2 edges → offsets[2]=5
        //   3: 3 edges → offsets[3]=7
        //   4: 4 edges → offsets[4]=10
        //   5: 3 edges → offsets[5]=14
        //   6: 2 edges → offsets[6]=17
        //   7: 3 edges → offsets[7]=19
        //   8: 2 edges → offsets[8]=22
        // Node 4's edges are at CSR indices 10, 11, 12, 13
        // 4→1 at idx 10, 4→5 at idx 11, 4→7 at idx 12, 4→3 at idx 13
        let num_undirected = graph.edge_count() / 2;
        let mut fel = FrontierEdgeList::new(2, num_undirected);
        for local_idx in 0..graph.degree(4) {
            let edge_idx = graph.offset(4) + local_idx;
            fel.insert(edge_idx, 1); // part 1's frontier
        }

        let cycles = fel.walk_boundary(1, &graph, &assignments);
        assert_eq!(cycles.len(), 1, "single-node district should have exactly one boundary cycle");

        let cycle = &cycles[0];
        assert_eq!(cycle.len(), 4, "center node has 4 frontier edges");

        // All edges should be from node 4
        assert!(cycle.iter().all(|&(u, _)| u == 4));

        // The targets should be all neighbors of 4 in CCW order: [1, 5, 7, 3]
        let targets: Vec<usize> = cycle.iter().map(|&(_, v)| v).collect();
        // Find where 1 starts in the cycle (the walk can start from any frontier edge)
        // Find the starting position and verify CCW order
        let expected_ccw = [1, 5, 7, 3]; // CCW order of node 4's neighbors
        // The cycle should be a rotation of this CCW order
        let first_in_expected = expected_ccw.iter().position(|&t| t == targets[0]).unwrap();
        for i in 0..4 {
            assert_eq!(targets[i], expected_ccw[(first_in_expected + i) % 4],
                "boundary walk should follow CCW order");
        }
    }

    #[test]
    fn walk_boundary_two_node_district() {
        let graph = make_grid_3x3();
        // District = {3, 4} (left-center + center), everything else is part 0
        let assignments = [0, 0, 0, 1, 1, 0, 0, 0, 0];

        // Frontier edges for part 1:
        // From node 3 (adj [0, 4, 6]): 3→0 (outside), 3→4 (inside, skip), 3→6 (outside)
        //   CSR: node 3 starts at offset 7. 3→0 at 7, 3→4 at 8, 3→6 at 9
        //   Frontier: idx 7 (3→0), idx 9 (3→6)
        // From node 4 (adj [1, 5, 7, 3]): 4→1 (out), 4→5 (out), 4→7 (out), 4→3 (in, skip)
        //   CSR: node 4 starts at offset 10. 4→1 at 10, 4→5 at 11, 4→7 at 12, 4→3 at 13
        //   Frontier: idx 10 (4→1), idx 11 (4→5), idx 12 (4→7)
        let num_undirected = graph.edge_count() / 2;
        let mut fel = FrontierEdgeList::new(2, num_undirected);
        // Insert frontier edges from node 3
        for local_idx in 0..graph.degree(3) {
            let nbr = graph.edge(3, local_idx).unwrap();
            if assignments[nbr] != 1 {
                fel.insert(graph.offset(3) + local_idx, 1);
            }
        }
        // Insert frontier edges from node 4
        for local_idx in 0..graph.degree(4) {
            let nbr = graph.edge(4, local_idx).unwrap();
            if assignments[nbr] != 1 {
                fel.insert(graph.offset(4) + local_idx, 1);
            }
        }

        let cycles = fel.walk_boundary(1, &graph, &assignments);
        assert_eq!(cycles.len(), 1, "connected two-node district should have one boundary cycle");

        let cycle = &cycles[0];
        assert_eq!(cycle.len(), 5, "district {{3,4}} has 5 frontier edges");

        // The boundary should contain edges from both nodes 3 and 4
        let sources: std::collections::HashSet<usize> = cycle.iter().map(|&(u, _)| u).collect();
        assert!(sources.contains(&3));
        assert!(sources.contains(&4));

        // Verify the cycle is a valid rotation of the expected CCW boundary
        let expected_edges: Vec<(usize, usize)> = vec![
            (4, 1), (4, 5), (4, 7), (3, 6), (3, 0),
        ];
        // Find where the cycle starts relative to expected
        if let Some(offset) = cycle.iter().position(|e| *e == expected_edges[0]) {
            for i in 0..5 {
                assert_eq!(cycle[(offset + i) % 5], expected_edges[i],
                    "boundary edge {} should match expected CCW order", i);
            }
        } else {
            // If it starts from a different edge, just check it's a valid rotation
            let cycle_set: std::collections::HashSet<(usize, usize)> = cycle.iter().copied().collect();
            let expected_set: std::collections::HashSet<(usize, usize)> = expected_edges.iter().copied().collect();
            assert_eq!(cycle_set, expected_set, "boundary should contain exactly the expected frontier edges");
        }
    }

    #[test]
    fn walk_boundary_empty_frontier() {
        let graph = make_grid_3x3();
        // All nodes in part 0 — no frontier edges for part 0 against itself
        let assignments = [0; 9];
        let num_undirected = graph.edge_count() / 2;
        let fel = FrontierEdgeList::new(2, num_undirected);

        let cycles = fel.walk_boundary(0, &graph, &assignments);
        assert!(cycles.is_empty());
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
