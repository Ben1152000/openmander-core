use rand::{distributions::{weighted::WeightedIndex, Distribution}, seq::{SliceRandom, IteratorRandom}, Rng};

use crate::partition::Partition;

#[allow(unused)]
impl Partition {
    /// Select a random node from the map.
    pub(crate) fn random_node<R: Rng + ?Sized>(&self, rng: &mut R) -> usize {
        rng.gen_range(0..self.graph().node_count())
    }

    /// Select a random node from a given part.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_node_from_part<R: Rng + ?Sized>(&self, part: u32, rng: &mut R) -> Option<usize> {
        self.parts.get(part as usize).choose(rng).copied()
    }

    /// Select a random unassigned node from the map.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_unassigned_node<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<usize> {
        self.random_node_from_part(0, rng)
    }

    /// Select a random unassigned node from the map that is on a part boundary.
    pub(crate) fn random_unassigned_boundary_node<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<usize> {
        let set = self.frontiers.get(0);
        if set.is_empty() { None } else { Some(set[rng.gen_range(0..set.len())]) }
    }

    /// Select a random neighbor of a given node.
    pub(crate) fn random_edge<R: Rng + ?Sized>(&self, node: usize, rng: &mut R) -> Option<usize> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        Some(self.graph().edge(node, rng.gen_range(0..self.graph().degree(node))).unwrap())
    }

    /// Select a random neighbor of a given node that is in the same part.
    pub(crate) fn random_same_part_edge<R: Rng + ?Sized>(&self, node: usize, rng: &mut R) -> Option<usize> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        let part = self.assignment(node);
        let same_part_neighbors = self.graph().edges(node)
            .filter(|&v| self.assignment(v) == part)
            .collect::<Vec<_>>();
        if same_part_neighbors.is_empty() { None }
        else { same_part_neighbors.choose(rng).copied() }
    }

    /// Select a random neighboring part of a given node.
    pub(crate) fn random_neighboring_part<R: Rng + ?Sized>(&self, node: usize, rng: &mut R) -> Option<u32> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        self.graph().edges(node)
            .map(|v| self.assignment(v))
            .filter(|&p| p != self.assignment(node))
            .choose(rng)
    }

    /// Select a random part, weighted by frontier size.
    pub(crate) fn random_part_weighted_by_frontier<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<u32> {
        let weights = self.frontiers.iter()
            .map(|set| set.len().saturating_sub(1))
            .collect::<Vec<_>>();
        let dist = WeightedIndex::new(&weights).ok()?; // None if all weights are zero
        Some(dist.sample(rng) as u32)
    }

    /// Randomly assign all nodes to contiguous parts.
    pub(crate) fn random_seed_fill(&mut self) {
        let mut rng = rand::thread_rng();
        self.clear_assignments();

        // Seed parts with random starting nodes.
        for part in 1..self.num_parts() {
            self.move_node(self.random_unassigned_node(&mut rng).unwrap(), part, false);
        }

        // Expand parts until all nodes are assigned.
        while let Some(u) = self.random_unassigned_boundary_node(&mut rng) {
            self.move_node(u, self.random_neighboring_part(u, &mut rng).unwrap(), false);
        }
    }

    /// Assign all nodes to districts by building a minimum spanning tree weighted to prefer
    /// intra-county edges, then greedily cutting subtrees from the leaves inward.
    ///
    /// `series`     — weight series used to size districts (e.g. `"T_20_CENS_Total"`).
    /// `county_ids` — per-node county index (dense, 0-based). Edges crossing county boundaries
    ///                receive a weight penalty of 1.0, making them less likely to appear in the MST.
    pub(crate) fn random_minimize_county_splits(&mut self, series: &str, vtd_ids: &[u32], county_ids: &[u32]) {
        use crate::partition::structures::SpanningTree;

        self.clear_assignments();

        let n = self.num_nodes();
        let num_districts = (self.num_parts() - 1) as usize; // excludes unassigned part 0
        let mut rng = rand::thread_rng();

        // Steps 1 & 2: random edge weights with penalties for boundary-crossing edges.
        // +1 for precinct (VTD) crossings, +2 for county crossings (cumulative).
        let mut weighted_adj = vec![Vec::new(); n];
        for u in 0..n {
            for v in self.graph().edges(u) {
                let vtd_penalty    = if vtd_ids[u]    != vtd_ids[v]    { 1.0 } else { 0.0 };
                let county_penalty = if county_ids[u]  != county_ids[v] { 2.0 } else { 0.0 };
                weighted_adj[u].push((v, rng.gen_range(0.0..1.0) + vtd_penalty + county_penalty));
            }
        }

        // Step 3: build MST over the entire graph.
        let tree = SpanningTree::minimum_spanning_tree((0..n).collect(), n, &weighted_adj);

        let total  = self.region_total(series);
        let target = total / num_districts as f64;

        // Step 4a: bottom-up accumulation — compute subtree populations once.
        let mut subtree_pop: Vec<f64> = (0..n)
            .map(|u| self.unit_weights().get_as_f64(series, u).unwrap_or(0.0))
            .collect();
        for u in tree.non_root_nodes_bottom_up() {
            let p = tree.parent_of(u).unwrap();
            subtree_pop[p] += subtree_pop[u];
        }

        let mut assignments = vec![0u32; n];

        // Step 4b: cut one district at a time.
        // Each iteration finds the subtree whose population is closest to `target` without
        // exceeding it, assigns it, then walks up the tree subtracting the cut population
        // from each ancestor — O(depth) work per cut instead of a full re-traversal.
        for district in 1..num_districts as u32 {
            // Best candidate: unassigned non-root node with subtree_pop ≤ target, maximised.
            let best = tree.non_root_nodes_bottom_up()
                .filter(|&u| assignments[u] == 0 && subtree_pop[u] <= target)
                .max_by(|&a, &b| {
                    subtree_pop[a].partial_cmp(&subtree_pop[b]).unwrap_or(std::cmp::Ordering::Equal)
                });

            let u = match best {
                Some(u) => u,
                None => break, // remaining population fits in one district
            };

            // Assign the subtree; skip nodes already assigned by earlier cuts.
            for &node in tree.subtree_nodes(u).unwrap() {
                if assignments[node] == 0 { assignments[node] = district; }
            }

            // Propagate the cut upward: subtract `target` (not the actual subtree population)
            // from every ancestor so that underapproximations do not compound across cuts.
            let mut cur = u;
            while let Some(p) = tree.parent_of(cur) {
                subtree_pop[p] -= target;
                cur = p;
            }
        }

        // Step 5: everything still unassigned belongs to the final district.
        let last = num_districts as u32;
        for a in &mut assignments {
            if *a == 0 { *a = last; }
        }

        self.set_assignments(assignments);
    }
}
