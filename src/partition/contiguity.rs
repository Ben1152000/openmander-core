use std::collections::{HashMap, HashSet, VecDeque};

use crate::partition::Partition;

impl Partition {
    /// Check if a part is empty (has no assigned nodes).
    pub(crate) fn part_is_empty(&self, part: u32) -> bool {
        assert!(part < self.num_parts(), "part must be in range [0, {})", self.num_parts());
        self.part_sizes[part as usize] == 0
    }

    /// Check if a node borders a given part.
    pub(crate) fn node_borders_part(&self, node: usize, part: u32) -> bool {
        assert!(node < self.graph().len(), "node {} out of range", node);
        assert!(part < self.num_parts(), "part must be in range [0, {})", self.num_parts());

        self.graph().edges(node).any(|v| self.assignments[v] == part)
    }

    /// Check if part `a` borders part `b`.
    pub(crate) fn part_borders_part(&self, a: u32, b: u32) -> bool {
        assert!(a < self.num_parts() && b < self.num_parts() && a != b, 
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        let a_frontier = self.frontiers.get(a);
        a_frontier.iter().any(|&u| self.node_borders_part(u, b))
    }

    /// Check if moving `node` to a new part does not break contiguity.
    /// If `part` is 0 (unassigned), only checks if removing `node` breaks its current part.
    pub(crate) fn check_node_contiguity(&self, node: usize, part: u32) -> bool {
        assert!(node < self.graph().len(), "node {} out of range", node);
        assert!(part < self.num_parts(), "part must be in range [0, {})", self.num_parts());

        let prev = self.assignments[node];

        // No-op move: always fine.
        if part == prev { return true }

        // For a non-empty destination, require at least one adjacent node in that part.
        if part != 0 && !self.part_is_empty(part)
            && !self.graph().edges(node).any(|v| self.assignments[v] == part)
        { return false }

        // If currently unassigned, removing it can't break any real district.
        if prev == 0 { return true }

        // Collect neighbors in the same part as `node`.
        let neighbors = self.graph().edges(node)
            .filter(|&v| self.assignments[v] == prev)
            .collect::<Vec<_>>();

        // If fewer than 2 same-part neighbors, removing `node` cannot disconnect the part.
        if neighbors.len() <= 1 { return true }

        // Fast path: exactly two neighbors and they’re directly connected.
        if neighbors.len() == 2
            && self.graph().edges(neighbors[0]).any(|v| v == neighbors[1])
        { return true }

        // Get the number of same-part neighbors for each neighbor.
        let neighbor_degrees = neighbors.iter()
            .map(|&u| self.graph().edges(u).filter(|&v| self.assignments[v] == prev).count())
            .collect::<Vec<_>>();

        // Heuristic: Start the BFS with the neighbor with fewest same-part neighbors.
        let (start, &min_degree) = neighbor_degrees.iter().enumerate()
            .min_by_key(|&(_, d)| d)
            .unwrap();

        // Fast path: If any neighbor has only a single same-part neighbor, removing `node` would disconnect.
        if min_degree == 1 { return false }

        // Track which same-part neighbors have been reached.
        let mut is_neighbor = vec![false; self.graph().len()];
        for &u in &neighbors { is_neighbor[u] = true }

        // BFS from one neighbor within `part`, forbidding `node`.
        let mut visited = vec![false; self.graph().len()];
        visited[node] = true;
        visited[neighbors[start]] = true;

        let mut remaining = neighbors.len() - 1;
        let mut queue = VecDeque::from([neighbors[start]]);
        while let Some(u) = queue.pop_front() {
            for v in self.graph().edges(u).filter(|&v| v != node && self.assignments[v] == prev) {
                if !visited[v] {
                    visited[v] = true;
                    if is_neighbor[v] {
                        // Check for early exit: if all targets have been visited, contiguity is preserved.
                        remaining -= 1; if remaining == 0 { return true }
                        queue.push_front(v); // prioritize BFS from targets
                    } else {
                        queue.push_back(v);
                    }
                }
            }
        }

        // If all same-part neighbors are reachable without `node`, contiguity is preserved.
        remaining == 0
    }

    /// Check if a set of nodes forms a contiguous subgraph, and if moving them would violate contiguity.
    pub(crate) fn check_subgraph_contiguity(&self, nodes: &[usize], part: u32) -> bool {
        if nodes.is_empty() { return true }

        // Deduplicate and validate indices.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph().len()];
        for &u in nodes {
            assert!(u < self.graph().len(), "node {} out of range", u);
            if !in_subgraph[u] { in_subgraph[u] = true; subgraph.push(u); }
        }

        // Ensure that at least one node in the subgraph is adjacent to the new part.
        if !(part == 0 || self.part_is_empty(part) || subgraph.iter().any(|&u| self.graph().edges(u).any(|v| self.assignments[v] == part))) { return false }

        // Check if the subgraph itself is contiguous.
        let mut seen = 1 as usize;
        let mut visited = vec![false; self.graph().len()];
        let mut queue = VecDeque::from([subgraph[0]]);
        visited[subgraph[0]] = true;
        while let Some(u) = queue.pop_front() {
            for v in self.graph().edges(u) {
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
            let mut in_boundary = vec![false; self.graph().len()];
            for &u in subgraph.iter().filter(|&&u| self.assignments[u] == part) {
                for v in self.graph().edges(u).filter(|&v| !in_subgraph[v] && self.assignments[v] == part) {
                    if !in_boundary[v] { in_boundary[v] = true; boundary.push(v) }
                }
            }

            // If fewer than 2 boundary nodes, removal cannot disconnect the part.
            if boundary.len() <= 1 { continue }

            // BFS within part p, forbidding S, early exit once all targets seen.
            let mut visited = vec![false; self.graph().len()];
            visited[boundary[0]] = true;

            let mut remaining = boundary.len() - 1;
            let mut queue = VecDeque::from([boundary[0]]);

            while let Some(u) = queue.pop_front() {
                for v in self.graph().edges(u) {
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
    pub(crate) fn find_components(&self, part: u32) -> Vec<Vec<usize>> {
        let mut components = Vec::new();

        let mut visited = vec![false; self.graph().len()];
        for u in (0..self.graph().len()).filter(|&u| self.assignments[u] == part) {
            if !visited[u] {
                visited[u] = true;
                let mut component = Vec::new();
                let mut queue = VecDeque::from([u]);
                while let Some(v) = queue.pop_front() {
                    component.push(v);
                    for w in self.graph().edges(v) {
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
    pub(crate) fn check_contiguity(&self) -> bool {
        (1..self.num_parts()).all(|part| self.find_components(part).len() <= 1)
    }

    /// Enforce contiguity of all parts by reassigning nodes as needed.
    ///
    /// Greedily fix contiguity: for any district with multiple components,
    /// keep its largest component and move each smaller component to the
    /// best neighboring district (by summed shared-perimeter weight).
    /// Returns true if any changes were made.
    pub(crate) fn ensure_contiguity(&mut self) -> bool {
        let mut changed = false;

        for part in 1..self.num_parts() {
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
                if component.iter().any(|&u| self.graph().edges(u).any(|v| self.assignments[v] == 0)) {
                    self.move_subgraph(&component, part, false);
                    changed = true;
                    continue;
                }

                let mut in_component = vec![false; self.graph().len()];
                for &u in &component { in_component[u] = true; }

                // Score candidate destination districts by boundary shared-perimeter weight.
                let mut scores: HashMap<u32, f64> = HashMap::new();
                for &u in &component {
                    for (v, weight) in self.graph().edges_with_weights(u).filter(|&(v, _)| !in_component[v] && self.assignments[v] != part) {
                        *scores.entry(self.assignments[v]).or_insert(0.0) += weight;
                    }
                }

                // Find the part with the largest shared-perimeter.
                self.move_subgraph(
                    &component, 
                    *scores.iter()
                        .max_by(|(_, a), (_, b)| a.total_cmp(b)).unwrap().0, 
                    false
                );

                changed = true;
            }
        }
        changed
    }

    /// If removing `u` from its current part splits it, return the smaller component(s)
    /// of the *previous* part that become disconnected from the rest *without u*.
    pub(crate) fn cut_subgraph_within_part(&self, node: usize) -> Vec<usize> {
        assert!(node < self.graph().len(), "node {} out of range", node);

        let part = self.assignments[node];
        if part == 0 { return vec![] }

        // Collect same-part neighbors of u.
        let neighbors = self.graph().edges(node)
            .filter(|&v| self.assignments[v] == part)
            .collect::<Vec<_>>();

        // If fewer than 2 neighbors, node cannot be an articulation point.
        if neighbors.len() <= 1 { return vec![] }

        let mut components = neighbors.iter()
            .map(|&u| vec![u])
            .collect::<Vec<_>>();

        // Index of component that reached v, or usize::MAX if unvisited
        let mut in_component = vec![usize::MAX; self.graph().len()];
        in_component[node] = self.graph().len(); // mark removed
        for i in 0..neighbors.len() { in_component[neighbors[i]] = i }

        // One queue per component seed (each neighbor).
        let mut queues = neighbors.iter()
            .map(|&u| VecDeque::from([u]))
            .collect::<Vec<_>>();

        // Tiny union-find data structure over label indices to merge when frontiers meet.
        struct UnionFind { parent: Vec<usize>, reps: Vec<u8>, components: usize }

        impl UnionFind {
            fn new(n: usize) -> Self {
                Self { parent: (0..n).collect(), reps: vec![0; n], components: n }
            }

            fn find(&mut self, u: usize) -> usize {
                if self.parent[u] == u { u } else {
                    let rep = self.find(self.parent[u]);
                    self.parent[u] = rep;
                    rep
                }
            }

            fn union(&mut self, a: usize, b: usize) {
                let (mut a, mut b) = (self.find(a), self.find(b));
                if a == b { return }
                if self.reps[a] < self.reps[b] { std::mem::swap(&mut a, &mut b) }
                self.parent[b] = a;
                if self.reps[a] == self.reps[b] { self.reps[a] += 1 }
                self.components -= 1;
            }
        }

        let mut union_find = UnionFind::new(neighbors.len());

        // Round-robin expansion; stop when a single label-group remains active.
        loop {
            // Count active union-find roots with non-empty queues
            let active_roots = (0..neighbors.len())
                .filter(|&i| !queues[i].is_empty())
                .map(|i| union_find.find(i))
                .collect::<HashSet<_>>();

            // Check if all but one BFS has completed
            if active_roots.len() <= 1 { break }

            // If all components have merged, node is not an articulation point
            if union_find.components == 1 { return vec![] }

            for i in 0..neighbors.len() {
                if let Some(u) = queues[i].pop_front() {
                    for v in self.graph().edges(u).filter(|&v| v != node && self.assignments[v] == part) {
                        if in_component[v] == usize::MAX {
                            in_component[v] = i;
                            queues[i].push_back(v);
                            components[i].push(v);
                        } else if in_component[v] < neighbors.len() && in_component[v] != i {
                            // BFS encountered node from another component
                            union_find.union(i, in_component[v]);
                        }
                    }
                }
            }
        }

        // Determine main (still-active) root; if none, use largest by size.
        let active_roots = (0..neighbors.len())
            .filter(|&i| !queues[i].is_empty())
            .map(|i| union_find.find(i))
            .collect::<HashSet<_>>();

        let main_root = active_roots.iter().copied().next().unwrap_or({
            let mut size_by_root = vec![0; neighbors.len()];
            for i in 0..neighbors.len() {
                size_by_root[union_find.find(i)] += components[i].len();
            }
            (0..neighbors.len()).max_by_key(|&r| size_by_root[r]).unwrap()
        });

        // Collect nodes from all non-main components.
        components.into_iter().enumerate()
            .filter(|(i, _)| union_find.find(*i) != main_root)
            .flat_map(|(_, component)| component)
            .collect::<Vec<_>>()
    }
}
