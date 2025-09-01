use std::collections::{HashMap, VecDeque};

use crate::partition::WeightedGraphPartition;

impl WeightedGraphPartition {
    /// Check if a part is empty (has no assigned nodes).
    pub fn part_is_empty(&self, part: u32) -> bool {
        assert!(part < self.num_parts, "part must be in range [0, {})", self.num_parts);
        self.part_sizes[part as usize] == 0
    }

    /// Check if moving `node` to a new part does not break contiguity.
    pub fn check_node_contiguity(&self, node: usize, part: u32) -> bool {
        let prev = self.assignments[node];

        // Ensure that `node` is adjacent to the new part, if it exists.
        if !(self.part_is_empty(part) || self.graph.edges(node).any(|v| self.assignments[v] == part)) { return false }

        // Unassigned: moving it cannot break contiguity of a real district.
        if prev == 0 { return true }

        // Collect neighbors that are in the same part.
        let neighbors = self.graph.edges(node)
            .filter(|&v| self.assignments[v] == prev)
            .collect::<Vec<_>>();

        // If fewer than 2 same-part neighbors, removing `node` cannot disconnect the part.
        if neighbors.len() <= 1 { return true }

        // Track which same-part neighbors have been reached.
        let mut targets = vec![false; self.graph.len()];
        neighbors.iter().for_each(|&v| targets[v] = true );

        // BFS from one neighbor within `part`, forbidding `node`.
        let mut visited = vec![false; self.graph.len()];
        visited[node] = true;
        visited[neighbors[0]] = true;

        let mut remaining = neighbors.len() - 1;
        let mut queue = VecDeque::from([neighbors[0]]);
        while let Some(u) = queue.pop_front() {
            for v in self.graph.edges(u) {
                if v != node && !visited[v] && self.assignments[v] == prev {
                    visited[v] = true;
                    queue.push_back(v);

                    // Check for early exit: if all targets have been visited, contiguity is preserved.
                    if targets[v] { remaining -= 1; if remaining == 0 { return true } }
                }
            }
        }

        // If all same-part neighbors are reachable without `node`, contiguity is preserved.
        neighbors.iter().all(|&v| visited[v])
    }

    /// Check if a set of nodes forms a contiguous subgraph, and if moving them would violate contiguity.
    pub fn check_subgraph_contiguity(&self, nodes: &[usize], part: u32) -> bool {
        if nodes.is_empty() { return true }

        // Deduplicate and validate indices.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph.len()];
        for &u in nodes {
            assert!(u < self.graph.len(), "node {} out of range", u);
            if !in_subgraph[u] { in_subgraph[u] = true; subgraph.push(u); }
        }

        // Ensure that at least one node in the subgraph is adjacent to the new part.
        if !(self.part_is_empty(part) || subgraph.iter().any(|&u| self.graph.edges(u).any(|v| self.assignments[v] == part))) { return false }

        // Check if the subgraph itself is contiguous.
        let mut seen = 1 as usize;
        let mut visited = vec![false; self.graph.len()];
        let mut queue = VecDeque::from([subgraph[0]]);
        visited[subgraph[0]] = true;
        while let Some(u) = queue.pop_front() {
            for v in self.graph.edges(u) {
                if in_subgraph[v] && !visited[v] {
                    seen += 1;
                    visited[v] = true;
                    queue.push_back(v);
                }
            }
        }
        if seen != subgraph.len() { return false }

        // Collect unique non-zero parts appearing in the subgraph.
        let mut parts = subgraph.iter()
            .map(|&u| self.assignments[u])
            .filter(|&p| p != 0)
            .collect::<Vec<_>>();
        parts.sort_unstable();
        parts.dedup();

        'by_part: for part in parts {
            // Build boundary set in part: vertices in p adjacent to the subgraph.
            let mut boundary = Vec::new();
            let mut in_boundary = vec![false; self.graph.len()];
            for &u in subgraph.iter().filter(|&&u| self.assignments[u] == part) {
                for v in self.graph.edges(u).filter(|&v| !in_subgraph[v] && self.assignments[v] == part) {
                    if !in_boundary[v] { in_boundary[v] = true; boundary.push(v) }
                }
            }

            // If fewer than 2 boundary nodes, removal cannot disconnect the part.
            if boundary.len() <= 1 { continue }

            // BFS within part p, forbidding S, early exit once all targets seen.
            let mut visited = vec![false; self.graph.len()];
            visited[boundary[0]] = true;

            let mut remaining = boundary.len() - 1;
            let mut queue = VecDeque::from([boundary[0]]);

            while let Some(u) = queue.pop_front() {
                for v in self.graph.edges(u) {
                    if !in_subgraph[v] && !visited[v] && self.assignments[v] == part {
                        visited[v] = true;
                        queue.push_back(v);

                        // Check for early exit: if all targets have been visited, contiguity is preserved.
                        if in_boundary[v] { remaining -= 1; if remaining == 0 { continue 'by_part } }
                    }
                }
            }

            if remaining > 0 { return false }
        }

        true
    }

    /// Find all connected components (as node lists) inside district `part`.
    pub fn find_components(&self, part: u32) -> Vec<Vec<usize>> {
        let mut components = Vec::new();

        let mut visited = vec![false; self.graph.len()];
        for u in (0..self.graph.len()).filter(|&u| self.assignments[u] == part) {
            if !visited[u] {
                visited[u] = true;
                let mut component = Vec::new();
                let mut queue = VecDeque::from([u]);
                while let Some(v) = queue.pop_front() {
                    component.push(v);
                    for w in self.graph.edges(v) {
                        if self.assignments[w] == part && !visited[w] {
                            visited[w] = true;
                            queue.push_back(w);
                        }
                    }
                }
                components.push(component);
            }
        }
        components
    }

    /// Check if if every real district `(1..num_parts)` is contiguous.
    pub fn check_contiguity(&self) -> bool {
        (1..self.num_parts).all(|part| self.find_components(part).len() <= 1)
    }

    /// Enforce contiguity of all parts by reassigning nodes as needed.
    ///
    /// Greedily fix contiguity: for any district with multiple components,
    /// keep its largest component and move each smaller component to the
    /// best neighboring district (by summed shared-perimeter weight).
    /// Returns true if any changes were made.
    pub fn ensure_contiguity(&mut self) -> bool {
        let mut changed = false;

        for part in 1..self.num_parts {
            // Find connected components inside the part.
            let components = self.find_components(part);
            if components.len() <= 1 { continue }

            // Keep the largest component, expel the rest.
            let largest = components.iter().enumerate()
                .max_by_key(|(_, c)| c.len())
                .map(|(i, _)| i)
                .unwrap();

            for (i, component) in components.into_iter().enumerate() {
                if i == largest { continue }

                // If the component borders an unassigned node, unassign the component.
                if component.iter().any(|&u| self.graph.edges(u).any(|v| self.assignments[v] == 0)) {
                    self.move_subgraph(&component, part);
                    changed = true;
                    continue;
                }

                let mut in_component = vec![false; self.graph.len()];
                for &u in &component { in_component[u] = true; }

                // Score candidate destination districts by boundary shared-perimeter weight.
                let mut scores: HashMap<u32, f64> = HashMap::new();
                for &u in &component {
                    for (v, weight) in self.graph.edges_with_weights(u).filter(|&(v, _)| !in_component[v] && self.assignments[v] != part) {
                        *scores.entry(self.assignments[v]).or_insert(0.0) += weight;
                    }
                }

                // Find the part with the largest shared-perimeter.
                self.move_subgraph(&component, *scores.iter()
                    .max_by(|(_, a), (_, b)| a.total_cmp(b)).unwrap().0);

                changed = true;
            }
        }
        changed
    }
}
