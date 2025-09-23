use std::sync::Arc;

use ndarray::Array1;

use crate::{graph::{Graph, WeightMatrix}, partition::FrontierSet};

/// A partition of a graph into contiguous parts (districts).
#[derive(Clone, Debug)]
pub(crate) struct Partition {
    num_parts: u32,                        // Fixed number of parts (including unassigned 0)
    graph: Arc<Graph>,                     // Fixed graph structure
    pub(super) assignments: Array1<u32>,   // Current part assignment for each node, len = n
    pub(super) boundary: Array1<bool>,     // Whether each node is on a part boundary, len = n
    pub(super) frontiers: FrontierSet,     // Nodes on the boundary of each part
    pub(super) part_sizes: Vec<usize>,     // Number of nodes in each part, len = num_parts
    pub(super) part_weights: WeightMatrix, // Aggregated weights for each part
}

impl Partition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
    pub(crate) fn new(num_parts: usize, graph: impl Into<Arc<Graph>>) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");

        let graph: Arc<Graph> = graph.into();

        let mut part_sizes = vec![0; num_parts];
        part_sizes[0] = graph.len();

        let mut part_weights = graph.node_weights().copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, graph.node_weights());

        Self {
            num_parts: num_parts as u32,
            assignments: Array1::<u32>::zeros(graph.len()),
            boundary: Array1::<bool>::from_elem(graph.len(), false),
            frontiers: FrontierSet::new(num_parts, graph.len()),
            part_sizes,
            part_weights,
            graph,
        }
    }

    /// Get the number of parts in this partition (including unassigned 0).
    #[inline] pub(crate) fn num_parts(&self) -> u32 { self.num_parts }

    /// Get a reference to the underlying graph.
    #[inline] pub(crate) fn graph(&self) -> &Graph { &self.graph }

    /// Get the part assignment of a given node.
    #[inline] pub(crate) fn assignment(&self, node: usize) -> u32 { self.assignments[node] }

    /// Get the set of boundary nodes for a given part.
    #[inline] pub(crate) fn frontier(&self, part: u32) -> &[usize] { self.frontiers.get(part) }

    /// Clear all assignments, setting every node to unassigned (0).
    pub(crate) fn clear_assignments(&mut self) {
        self.assignments.fill(0);
        self.boundary.fill(false);
        self.frontiers.clear();

        self.part_sizes.fill(0);
        self.part_sizes[0] = self.graph.len();

        self.part_weights.clear_all_rows();
        self.part_weights.set_row_to_sum_of(0, self.graph.node_weights());
    }

    /// Generate assignments map from GeoId to district.
    pub(crate) fn set_assignments(&mut self, assignments: Vec<u32>) {
        assert!(assignments.len() == self.assignments.len(), "assignments.len() must equal number of nodes");
        assert!(assignments.iter().all(|&p| p < self.num_parts), "all assignments must be in range [0, {})", self.num_parts);

        // Copy assignments.
        self.assignments.assign(&Array1::from(assignments));

        // Recompute boundary flags.
        self.boundary.iter_mut().enumerate().for_each(|(u, flag)| {
            *flag = self.graph.edges(u)
                .any(|v| self.assignments[v] != self.assignments[u]);
        });

        // Recompute frontiers.
        self.frontiers.rebuild(
            self.assignments.as_slice().unwrap(),
            self.boundary.as_slice().unwrap()
        );

        self.part_sizes.fill(0);
        for &part in &self.assignments { self.part_sizes[part as usize] += 1 }

        // Recompute per-part totals.
        self.part_weights = WeightMatrix::copy_of_size(self.graph.node_weights(), self.num_parts as usize);
        for (node, &part) in self.assignments.iter().enumerate() {
            self.part_weights.add_row_from(part as usize, self.graph.node_weights(), node);
        }
    }

    /// Move a single node to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn move_node(&mut self, node: usize, part: u32, check: bool) {
        assert!(node < self.assignments.len(), "node {} out of range", node);
        assert!(part < self.num_parts, "part {} out of range [0, {})", part, self.num_parts);

        let prev = self.assignments[node];
        if prev == part { return }

        // Ensure move will not break contiguity.
        if check { assert!(self.check_node_contiguity(node, part), "moving node {} would break contiguity of part {}", node, prev); }

        // Commit assignment.
        self.assignments[node] = part;

        // Recompute boundary flag for `node`.
        self.boundary[node] = self.graph.edges(node)
            .any(|u| self.assignments[u] != part);

        // Recompute boundary flags for neighbors of `node`.
        for u in self.graph.edges(node) {
            self.boundary[u] = self.graph.edges(u)
                .any(|v| self.assignments[v] != self.assignments[u]);
        }

        // Recompute frontier sets for `node` and its neighbors.
        for u in std::iter::once(node).chain(self.graph.edges(node)) {
            self.frontiers.refresh(u, self.assignments[u], self.boundary[u]);
        }

        // Update part sizes (subtract from old, add to new).
        self.part_sizes[prev as usize] -= 1;
        self.part_sizes[part as usize] += 1;

        // Update aggregated integer totals (subtract from old, add to new).
        self.part_weights.subtract_row_from(prev as usize, self.graph.node_weights(), node);
        self.part_weights.add_row_from(part as usize, self.graph.node_weights(), node);
    }

    /// Move a connected subgraph to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn move_subgraph(&mut self, nodes: &[usize], part: u32, check: bool) {
        assert!(part < self.num_parts, "part {} out of range [0, {})", part, self.num_parts);
        if nodes.is_empty() { return }

        // Deduplicate and validate indices.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph.len()];
        for &u in nodes {
            assert!(u < self.graph.len(), "node {} out of range", u);
            if !in_subgraph[u] { in_subgraph[u] = true; subgraph.push(u); }
        }

        // Single node case: use move_node for efficiency and simplicity.
        if subgraph.len() == 1 { return self.move_node(subgraph[0], part, check);}

        // Check subgraph is connected AND removing it won't disconnect any source part.
        if check { assert!(self.check_subgraph_contiguity(&subgraph, part), "moving subgraph would break contiguity"); }

        let prev = self.assignments[subgraph[0]];
        assert!(subgraph.iter().all(|&u| self.assignments[u] == prev), "all nodes in subgraph must be in the same part");

        // Commit assignment.
        for &u in &subgraph { self.assignments[u] = part }

        let mut boundary = Vec::with_capacity(subgraph.len() * 2);
        let mut in_boundary = vec![false; self.graph.len()];
        for &u in &subgraph {
            if !in_boundary[u] { in_boundary[u] = true; boundary.push(u); }
            self.graph.edges(u).for_each(|v| {
                if !in_boundary[v] { in_boundary[v] = true; boundary.push(v); }
            });
        }

        // Recompute boundary flags and frontier sets only where necessary.
        for &u in &boundary {
            self.boundary[u] = self.graph.edges(u)
                .any(|v| self.assignments[v] != self.assignments[u]);
            self.frontiers.refresh(u, self.assignments[u], self.boundary[u]);
        }

        self.part_sizes[prev as usize] -= subgraph.len();
        self.part_sizes[part as usize] += subgraph.len();

        // Batch-update per-part totals.
        self.part_weights.subtract_rows_from(prev as usize, self.graph.node_weights(), &subgraph);
        self.part_weights.add_rows_from(part as usize, self.graph.node_weights(), &subgraph);
    }

    /// Articulation-aware move: move `u` and (if needed) the minimal "dangling" component
    /// that would be cut off by removing `u`, so the source stays contiguous.
    pub(crate) fn move_node_with_articulation(&mut self, node: usize, part: u32) {
        assert!(part < self.num_parts, "part must be in range [0, {})", self.num_parts);
        if self.assignments[node] == part { return }

        // Ensure that `node` is adjacent to the new part, if it exists.
        if !(self.part_is_empty(part) || self.graph.edges(node).any(|v| self.assignments[v] == part)) { return }

        // Find subgraph of all but largest "dangling" piece if removing `node` splits the district.
        let mut subgraph = self.cut_subgraph_within_part(node);
        if subgraph.len() == 0 { 
            self.move_node(node, part, true);
        } else {
            subgraph.push(node);
            self.move_subgraph(&subgraph, part, true);
        }
    }

    /// Merge two parts into one, updating caches.
    /// Returns the index of the eliminated part (if merge is successful).
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn merge_parts(&mut self, a: u32, b: u32, check: bool) -> Option<u32> {
        assert!(a < self.num_parts && b < self.num_parts && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts);

        // Choose `a` as the part to keep, `b` as the part to eliminate.
        if self.part_sizes[a as usize] < self.part_sizes[b as usize] { return self.merge_parts(b, a, check) }

        if !self.part_borders_part(a, b) { return None } // parts must be adjacent

        // Update assignments.
        for u in 0..self.graph.len() {
            if self.assignments[u] == b { self.assignments[u] = a }
        }

        // Update boundary and frontier sets.
        for u in (0..self.graph.len()).filter(|&u| self.assignments[u] == a) {
            self.boundary[u] = self.graph.edges(u)
                .any(|v| self.assignments[v] != self.assignments[u]);
            self.frontiers.refresh(u, self.assignments[u], self.boundary[u]);
        }

        // update part_sizes
        self.part_sizes[a as usize] += self.part_sizes[b as usize];
        self.part_sizes[b as usize] = 0;

        // update part_weights
        self.part_weights.add_row(a as usize, b as usize);
        self.part_weights.clear_row(b as usize);

        Some(b)
    }
}
