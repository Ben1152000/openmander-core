use std::{sync::Arc, vec};

use crate::{graph::{Graph, WeightMatrix}, partition::{MultiSet, PartitionSet}};

/// A partition of a graph into contiguous parts (districts).
#[derive(Clone, Debug)]
pub(crate) struct Partition {
    graph: Arc<Graph>,                     // Fixed graph structure
    pub(super) parts: PartitionSet,        // Sets of nodes in each part (including unassigned 0)
    pub(super) frontiers: MultiSet,        // Nodes on the boundary of each part
    pub(super) part_weights: WeightMatrix, // Aggregated weights for each part
}

impl Partition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
    pub(crate) fn new(num_parts: usize, graph: impl Into<Arc<Graph>>) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");
        let graph: Arc<Graph> = graph.into();

        let mut part_weights = graph.node_weights().copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, graph.node_weights());

        Self {
            parts: PartitionSet::new(num_parts, graph.node_count()),
            frontiers: MultiSet::new(num_parts, graph.node_count()),
            part_weights,
            graph,
        }
    }

    /// Get the number of parts in this partition (including unassigned 0).
    #[inline] pub(crate) fn num_parts(&self) -> u32 { self.parts.num_sets() as u32 }

    /// Get the number of nodes in the underlying graph.
    #[inline] pub(crate) fn num_nodes(&self) -> usize { self.graph.node_count() }

    /// Get a reference to the underlying graph.
    #[inline] pub(crate) fn graph(&self) -> &Graph { &self.graph }

    /// Get the part assignment of a given node.
    #[inline] pub(crate) fn assignment(&self, node: usize) -> u32 { self.parts.find(node) as u32 }

    /// Get a complete vector of assignments for each node.
    #[inline]
    pub(crate) fn assignments(&self) -> Vec<u32> {
        self.parts.assignments().iter().map(|&p| p as u32).collect()
    }

    /// Get the set of boundary nodes for a given part.
    #[inline] pub(crate) fn frontier(&self, part: u32) -> &[usize] { self.frontiers.get(part as usize) }

    /// Clear all assignments, setting every node to unassigned (0).
    pub(crate) fn clear_assignments(&mut self) {
        self.parts.clear();
        self.frontiers.clear();

        self.part_weights.clear_all_rows();
        self.part_weights.set_row_to_sum_of(0, self.graph.node_weights());
    }

    /// Generate assignments map from GeoId to district.
    pub(crate) fn set_assignments(&mut self, assignments: Vec<u32>) {
        assert!(assignments.len() == self.num_nodes(), "assignments.len() must equal number of nodes");
        assert!(assignments.iter().all(|&p| p < self.num_parts()), "all assignments must be in range [0, {})", self.num_parts());

        // Copy assignments.
        self.parts.rebuild(&assignments.iter().map(|&p| p as usize).collect::<Vec<_>>());

        // Recompute boundary flags.
        let on_boundary = (0..self.num_nodes()).map(|u| {
            let part = self.assignment(u);
            self.graph.edges(u).any(|v| self.assignment(v) != part)
        }).collect::<Vec<_>>();

        // Recompute frontiers.
        self.frontiers.rebuild_from(
            self.assignments().iter().enumerate()
            .filter_map(|(node, &part)| {
                on_boundary[node].then_some((node, part as usize))
            })
        );

        // Recompute per-part totals.
        self.part_weights = WeightMatrix::copy_of_size(self.graph.node_weights(), self.num_parts() as usize);
        for (node, &part) in self.assignments().iter().enumerate() {
            self.part_weights.add_row_from(part as usize, self.graph.node_weights(), node);
        }
    }

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
        if self.graph.edges(node).any(|v| self.assignment(v) != part) {
            self.frontiers.insert(node, self.assignment(node) as usize);
        } else {
            self.frontiers.remove(node);
        }

        for u in self.graph.edges(node) {
            if self.graph.edges(u).any(|v| self.assignment(v) != self.assignment(u)) {
                self.frontiers.insert(u, self.assignment(u) as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // Update aggregated integer totals (subtract from old, add to new).
        self.part_weights.subtract_row_from(prev as usize, self.graph.node_weights(), node);
        self.part_weights.add_row_from(part as usize, self.graph.node_weights(), node);
    }

    /// Move a connected subgraph to a different part, updating caches.
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn move_subgraph(&mut self, nodes: &[usize], part: u32, check: bool) {
        assert!(part < self.num_parts(), "part {} out of range [0, {})", part, self.num_parts());
        if nodes.is_empty() { return }

        // Deduplicate and validate indices.
        let mut subgraph = Vec::with_capacity(nodes.len());
        let mut in_subgraph = vec![false; self.graph.node_count()];
        for &u in nodes {
            assert!(u < self.graph.node_count(), "node {} out of range", u);
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
        let mut in_boundary = vec![false; self.graph.node_count()];
        for &u in &subgraph {
            if !in_boundary[u] { in_boundary[u] = true; boundary.push(u); }
            self.graph.edges(u).for_each(|v| {
                if !in_boundary[v] { in_boundary[v] = true; boundary.push(v); }
            });
        }

        // Recompute boundary flags and frontier sets only where necessary.
        for &u in &boundary {
            if self.graph.edges(u).any(|v| self.assignment(v) != self.assignment(u)) {
                self.frontiers.insert(u, self.assignment(u) as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // Batch-update per-part totals.
        self.part_weights.subtract_rows_from(prev as usize, self.graph.node_weights(), &subgraph);
        self.part_weights.add_rows_from(part as usize, self.graph.node_weights(), &subgraph);
    }

    /// Articulation-aware move: move `u` and (if needed) the minimal "dangling" component
    /// that would be cut off by removing `u`, so the source stays contiguous.
    pub(crate) fn move_node_with_articulation(&mut self, node: usize, part: u32) {
        assert!(part < self.num_parts(), "part must be in range [0, {})", self.num_parts());
        if self.assignment(node) == part { return }

        // Ensure that `node` is adjacent to the new part, if it exists.
        if !(self.part_is_empty(part) || self.graph.edges(node).any(|v| self.assignment(v) == part)) { return }

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
        assert!(a < self.num_parts() && b < self.num_parts() && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        // Choose `a` as the part to keep, `b` as the part to eliminate.
        if self.parts.get(a as usize).len() < self.parts.get(b as usize).len() { return self.merge_parts(b, a, check) }

        if !self.part_borders_part(a, b) { return None } // parts must be adjacent

        // Update assignments.
        for u in 0..self.graph.node_count() {
            if self.assignment(u) == b {
                self.parts.move_to(u, a as usize);
            }
        }

        // Update boundary and frontier sets.
        for u in 0..self.graph.node_count() {
            if self.assignment(u) != a { continue }

            if self.graph.edges(u).any(|v| self.assignment(v) != a) {
                self.frontiers.insert(u, a as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // update part_weights
        self.part_weights.add_row(a as usize, b as usize);
        self.part_weights.clear_row(b as usize);

        Some(b)
    }
}
