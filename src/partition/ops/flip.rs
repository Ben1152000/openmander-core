use crate::partition::Partition;

impl Partition {
    /// Move a single node to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn move_node(&mut self, node: usize, part: u32, check: bool) {
        assert!(node < self.num_nodes(), "node {} out of range", node);
        assert!(part < self.num_parts(), "part {} out of range [0, {})", part, self.num_parts());

        let prev = self.assignment(node);
        if prev == part { return }

        // Ensure move will not break contiguity.
        if check { assert!(self.check_node_contiguity(node, part), "moving node {} would break contiguity of part {}", node, prev); }

        // Commit assignment.
        self.parts.move_to(node, part as usize);

        // Recompute frontier sets for `node` and its neighbors.
        if self.graph().edges(node).any(|v| self.assignment(v) != part) {
            self.frontiers.insert(node, self.assignment(node) as usize);
        } else {
            self.frontiers.remove(node);
        }

        for u in self.graph().edges(node).collect::<Vec<_>>() {
            if self.graph().edges(u).any(|v| self.assignment(v) != self.assignment(u)) {
                self.frontiers.insert(u, self.assignment(u) as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // Update aggregated integer totals (subtract from old, add to new).
        self.update_part_totals_for_node_move(node, prev, part);
    }

    /// Move a connected subgraph to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn move_subgraph(&mut self, nodes: &[usize], part: u32, check: bool) {
        assert!(part < self.num_parts(), "part {} out of range [0, {})", part, self.num_parts());
        if nodes.is_empty() { return }

        // Deduplicate and validate indices.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph().node_count()];
        for &u in nodes {
            assert!(u < self.graph().node_count(), "node {} out of range", u);
            if !in_subgraph[u] { in_subgraph[u] = true; subgraph.push(u); }
        }

        // Single node case: use move_node for efficiency and simplicity.
        if subgraph.len() == 1 { return self.move_node(subgraph[0], part, check);}

        // Check subgraph is connected AND removing it won't disconnect any source part.
        if check { assert!(self.check_subgraph_contiguity(&subgraph, part), "moving subgraph would break contiguity"); }

        let prev = self.assignment(subgraph[0]);
        assert!(subgraph.iter().all(|&u| self.assignment(u) == prev), "all nodes in subgraph must be in the same part");

        // Commit assignment.
        for &u in &subgraph {
            self.parts.move_to(u, part as usize);
        }

        let mut boundary = Vec::with_capacity(subgraph.len() * 2);
        let mut in_boundary = vec![false; self.graph().node_count()];
        for &u in &subgraph {
            if !in_boundary[u] { in_boundary[u] = true; boundary.push(u); }
            self.graph().edges(u).for_each(|v| {
                if !in_boundary[v] { in_boundary[v] = true; boundary.push(v); }
            });
        }

        // Recompute boundary flags and frontier sets only where necessary.
        for &u in &boundary {
            if self.graph().edges(u).any(|v| self.assignment(v) != self.assignment(u)) {
                self.frontiers.insert(u, self.assignment(u) as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // Batch-update per-part totals.
        self.update_part_totals_for_subgraph_move(&subgraph, prev, part);
    }

    /// Articulation-aware move: move `u` and (if needed) the minimal "dangling" component
    /// that would be cut off by removing `u`, so the source stays contiguous.
    pub(crate) fn move_node_with_articulation(&mut self, node: usize, part: u32) {
        assert!(part < self.num_parts(), "part must be in range [0, {})", self.num_parts());
        if self.assignment(node) == part { return }

        // Ensure that `node` is adjacent to the new part, if it exists.
        if !(self.part_is_empty(part) || self.graph().edges(node).any(|v| self.assignment(v) == part)) { return }

        // Find subgraph of all but largest "dangling" piece if removing `node` splits the district.
        let mut subgraph = self.cut_subgraph_within_part(node);
        if subgraph.len() == 0 { 
            self.move_node(node, part, true);
        } else {
            subgraph.push(node);
            self.move_subgraph(&subgraph, part, true);
        }
    }
}