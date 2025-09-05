use std::sync::Arc;

use ndarray::{s, Array1, Array2, Axis};

use crate::{graph::{WeightMatrix, WeightedGraph}, partition::frontier::FrontierSet};

/// A partition of a graph into contiguous parts (districts).
#[derive(Debug)]
pub struct GraphPartition {
    pub num_parts: u32, // Fixed number of parts (including unassigned 0)
    pub graph: Arc<WeightedGraph>, // Fixed graph structure
    pub assignments: Array1<u32>, // Current part assignment for each node, len = n
    pub boundary: Array1<bool>, // Whether each node is on a part boundary, len = n
    pub frontiers: FrontierSet, // Nodes on the boundary of each part
    pub part_sizes: Vec<usize>, // Number of nodes in each part, len = num_parts
    pub part_weights: WeightMatrix, // Aggregated weights for each part
}

impl GraphPartition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
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

        let mut part_sizes = vec![0; num_parts];
        part_sizes[0] = graph.len();

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

    /// Clear all assignments, setting every node to unassigned (0).
    pub fn clear_assignments(&mut self) {
        self.assignments.fill(0);
        self.boundary.fill(false);
        self.frontiers.clear();
        self.part_sizes.fill(0);
        self.part_sizes[0] = self.graph.len();
        if self.graph.node_weights.i64.ncols() > 0 {
            self.part_weights.i64.row_mut(0).assign(&self.graph.node_weights.i64.sum_axis(Axis(0)));
            self.part_weights.i64.slice_mut(s![1.., ..]).fill(0);
        }
        if self.graph.node_weights.f64.ncols() > 0 {
            self.part_weights.f64.row_mut(0).assign(&self.graph.node_weights.f64.sum_axis(Axis(0)));
            self.part_weights.f64.slice_mut(s![1.., ..]).fill(0.0);
        }
    }

    /// Generate assignments map from GeoId to district.
    pub fn set_assignments(&mut self, assignments: Vec<u32>) {
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

        self.rebuild_caches();
    }

    /// Recompute all caches from scratch.
    pub fn rebuild_caches(&mut self) {
        // Recompute per-part totals.
        self.part_weights = WeightMatrix {
            series: self.graph.node_weights.series.clone(),
            i64: Array2::<i64>::zeros((self.num_parts as usize, self.graph.node_weights.i64.ncols())),
            f64: Array2::<f64>::zeros((self.num_parts as usize, self.graph.node_weights.f64.ncols())),
        };

        for (u, &p) in self.assignments.iter().enumerate() {
            if self.graph.node_weights.i64.ncols() > 0 {
                self.part_weights.i64.row_mut(p as usize)
                    .scaled_add(1, &self.graph.node_weights.i64.row(u));
            }
            if self.graph.node_weights.f64.ncols() > 0 {
                self.part_weights.f64.row_mut(p as usize)
                    .scaled_add(1.0, &self.graph.node_weights.f64.row(u));
            }
        }

        self.part_sizes.fill(0);
        for &i in &self.assignments {
            self.part_sizes[i as usize] += 1
        }
    }

    /// Move a single node to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub fn move_node(&mut self, node: usize, part: u32, check: bool) {
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
        if self.graph.node_weights.i64.ncols() > 0 {
            let row_i = self.graph.node_weights.i64.row(node);
            self.part_weights.i64.row_mut(prev as usize).scaled_add(-1, &row_i);
            self.part_weights.i64.row_mut(part as usize).scaled_add(1, &row_i);
        }

        if self.graph.node_weights.f64.ncols() > 0 {
            let row_f = self.graph.node_weights.f64.row(node);
            self.part_weights.f64.row_mut(prev as usize).scaled_add(-1.0, &row_f);
            self.part_weights.f64.row_mut(part as usize).scaled_add(1.0, &row_f);
        }
    }

    /// Move a connected subgraph to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub fn move_subgraph(&mut self, nodes: &[usize], part: u32, check: bool) {
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
        if self.graph.node_weights.i64.ncols() > 0 {
            let mut sum_i = ndarray::Array1::<i64>::zeros(self.graph.node_weights.i64.ncols());
            subgraph.iter().for_each(|&u| sum_i += &self.graph.node_weights.i64.row(u));
            self.part_weights.i64.row_mut(prev as usize).scaled_add(-1, &sum_i);
            self.part_weights.i64.row_mut(part as usize).scaled_add(1, &sum_i);
        }

        if self.graph.node_weights.f64.ncols() > 0 {
            let mut sum_f = ndarray::Array1::<f64>::zeros(self.graph.node_weights.f64.ncols());
            subgraph.iter().for_each(|&u| sum_f += &self.graph.node_weights.f64.row(u));
            self.part_weights.f64.row_mut(prev as usize).scaled_add(-1.0, &sum_f);
            self.part_weights.f64.row_mut(part as usize).scaled_add(1.0, &sum_f);
        }
    }

    /// Articulation-aware move: move `u` and (if needed) the minimal "dangling" component
    /// that would be cut off by removing `u`, so the source stays contiguous.
    pub fn move_node_with_articulation(&mut self, node: usize, part: u32) {
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
    pub fn merge_parts(&mut self, a: u32, b: u32, check: bool) -> Option<u32> {
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
        if self.graph.node_weights.i64.ncols() > 0 {
            let row_b = self.part_weights.i64.row_mut(b as usize).to_owned();
            self.part_weights.i64.row_mut(a as usize).scaled_add(1, &row_b);
            self.part_weights.i64.row_mut(b as usize).fill(0);
        }

        Some(b)
    }
}
