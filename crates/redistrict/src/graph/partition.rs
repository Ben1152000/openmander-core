use std::{collections::{HashMap, VecDeque}, sync::Arc};

use ndarray::{Array1, Array2, Axis};

use crate::graph::{WeightMatrix, WeightedGraph};

/// Partition + caches for fast incremental updates.
#[derive(Debug)]
pub struct WeightedGraphPartition {
    pub num_parts: u32,
    pub graph: Arc<WeightedGraph>,
    pub assignments: Array1<u32>, // Current part assignment for each node, len = n
    pub boundary: Array1<bool>, // Whether each node is on a part boundary, len = n
    pub part_weights: WeightMatrix,
}

impl WeightedGraphPartition {
    /// Construct an empty partition from a map layer.
    pub fn new(
        num_parts: usize,
        graph: Arc<WeightedGraph>,
    ) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");

        let mut part_weights = WeightMatrix {
            series: graph.node_weights.series.clone(),
            i64: Array2::<i64>::zeros((num_parts, graph.node_weights.i64.ncols())),
            f64: Array2::<f64>::zeros((num_parts, graph.node_weights.f64.ncols())),
        };

        // initialize part 0 to contain the sum of all node weights
        part_weights.i64.row_mut(0).assign(&graph.node_weights.i64.sum_axis(Axis(0)));
        part_weights.f64.row_mut(0).assign(&graph.node_weights.f64.sum_axis(Axis(0)));

        Self {
            num_parts: num_parts as u32,
            assignments: Array1::<u32>::zeros(graph.len()),
            boundary: Array1::<bool>::from_elem(graph.len(), false),
            part_weights,
            graph,
        }
    }

    /// Generate assignments map from GeoId to district.
    pub fn set_assignments(&mut self, assignments: Vec<u32>) {
        assert!(assignments.len() == self.assignments.len(), "assignments.len() must equal number of nodes");
        assert!(assignments.iter().all(|&p| p < self.num_parts), "all assignments must be in range [0, {})", self.num_parts);

        // Copy assignments
        self.assignments.assign(&Array1::from(assignments));

        // Recompute boundary flags
        self.boundary.iter_mut().enumerate().for_each(|(node, flag)| {
            *flag = self.graph.edges(node)
                .any(|u| self.assignments[u] != self.assignments[node]);
        });

        // Recompute per-part totals
        self.part_weights = WeightMatrix {
            series: self.graph.node_weights.series.clone(),
            i64: Array2::<i64>::zeros((self.num_parts as usize, self.graph.node_weights.i64.ncols())),
            f64: Array2::<f64>::zeros((self.num_parts as usize, self.graph.node_weights.f64.ncols())),
        };

        self.part_weights.i64.axis_iter_mut(Axis(0)).enumerate().for_each(
            |(i, mut acc)| {
                for (node, &part) in self.assignments.iter().enumerate() {
                    if part as usize == i {
                        acc += &self.graph.node_weights.i64.row(node);
                    }
                }
            }
        );

        self.part_weights.f64.axis_iter_mut(Axis(0)).enumerate().for_each(
            |(i, mut acc)| {
                for (node, &part) in self.assignments.iter().enumerate() {
                    if part as usize == i {
                        acc += &self.graph.node_weights.f64.row(node);
                    }
                }
            }
        );
    }

    /// Randomly assign all nodes to contiguous districts, updating caches.
    pub fn randomize(&mut self) {
        // 1) Seed districts with random starting blocks
        // 2) Expand districts until all blocks are assigned
        // 3) Equalize populations in each district
        todo!()
    }

    /// Equalize all districts by given series, within a given tolerance.
    pub fn equalize(&mut self, series: &str, tol: u32) {
        todo!()
    }

    /// Move a single node to a different part, updating caches.
    pub fn move_node(&mut self, node: usize, part: u32) {
        assert!(node < self.assignments.len(), "node {} out of range", node);
        assert!(part < self.num_parts, "part {} out of range [0, {})", part, self.num_parts);

        let prev = self.assignments[node];
        if prev == part { return }

        // Update aggregated integer totals (subtract from old, add to new).
        let row_i = self.graph.node_weights.i64.row(node);
        self.part_weights.i64.row_mut(prev as usize).scaled_add(-1, &row_i);
        self.part_weights.i64.row_mut(part as usize).scaled_add(1, &row_i);

        let row_f = self.graph.node_weights.f64.row(node);
        self.part_weights.f64.row_mut(prev as usize).scaled_add(-1.0, &row_f);
        self.part_weights.f64.row_mut(part as usize).scaled_add(1.0, &row_f);

        // Commit assignment.
        self.assignments[node] = part;

        // Recompute boundary flag for `node`.
        self.boundary[node] = self.graph.edges(node)
            .any(|u| self.assignments[u] != part);

        // Recompute boundary flags for neighbors of `node`.
        self.graph.edges(node).for_each(|u| {
            self.boundary[u] = self.graph.edges(u)
                .any(|v| self.assignments[v] != self.assignments[u]);
        });
    }

    /// Move a connected subgraph to a different part, updating caches.
    pub fn move_subgraph(&mut self, nodes: Vec<usize>, part: u32) { todo!() }

    /// Check if removing `node` from its current part does not break contiguity.
    fn check_node_contiguity(&self, node: usize) -> bool {
        let part = self.assignments[node];

        // Unassigned: moving it cannot break contiguity of a real district.
        if part == 0 { return true }

        // Collect neighbors that are in the same part.
        let neighbors = self.graph.edges(node)
            .filter(|&v| self.assignments[v] == part)
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
                if v != node && !visited[v] && self.assignments[v] == part {
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
    fn check_subgraph_contiguity(&self, nodes: Vec<usize>) -> bool {
        if nodes.is_empty() { return true }

        // Create list of unique nodes in the subgraph.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph.len()];
        for u in nodes {
            if !in_subgraph[u] { in_subgraph[u] = true; subgraph.push(u); }
        }

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

        'parts: for part in parts {
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
                        if in_boundary[v] { remaining -= 1; if remaining == 0 { continue 'parts } }
                    }
                }
            }

            if remaining > 0 { return false }
        }

        true
    }

    /// Check if if every real district `(1..num_parts-1)` is contiguous.
    fn check_contiguity(&self) -> bool {
        (1..self.num_parts).all(|part| self.find_components(part).len() <= 1)
    }

    /// Find all connected components (as node lists) inside district `part`.
    fn find_components(&self, part: u32) -> Vec<Vec<usize>> {
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
                    self.move_subgraph(component, part);
                    changed = true;
                    continue;
                }

                let mut in_component = vec![false; self.graph.len()];
                for &u in &component { in_component[u] = true; }

                // Score candidate destination districts by boundary shared-perimeter weight.
                let mut scores: HashMap<u32, f64> = HashMap::new();
                for &u in &component {
                    for (v, weight) in self.graph.edge_weights(u).filter(|&(v, _)| !in_component[v] && self.assignments[v] != part) {
                        *scores.entry(self.assignments[v]).or_insert(0.0) += weight;
                    }
                }

                // Find the part with the largest shared-perimeter.
                self.move_subgraph(component, *scores.iter()
                    .max_by(|(_, a), (_, b)| a.total_cmp(b)).unwrap().0);

                changed = true;
            }
        }
        changed
    }

    /// Select a random block from the map.
    pub fn random_node(&self) -> usize {
        use rand::Rng;
        rand::rng().random_range(0..self.graph.len())
    }

    /// Select a random unassigned block from the map.
    fn random_unassigned_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.assignments.iter().enumerate()
            .filter_map(|(i, &part)| (part == 0).then_some(i))
            .choose(&mut rand::rng())
    }

    /// Select a random block from the map that is on a district boundary.
    fn random_boundary_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.boundary.iter().enumerate()
            .filter_map(|(i, &flag)| flag.then_some(i))
            .choose(&mut rand::rng())
    }

    /// Select a random unassigned block from the map that is on a district boundary.
    fn random_unassigned_boundary_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.assignments.iter().zip(self.boundary.iter()).enumerate()
            .filter_map(|(i, (&part, &flag))| (flag && part == 0).then_some(i))
            .choose(&mut rand::rng())
    }

}
