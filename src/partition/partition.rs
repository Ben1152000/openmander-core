use std::sync::Arc;

use crate::{
    graph::{Graph, WeightMatrix},
    partition::{MultiSet, PartitionSet},
};

/// A partition of a graph into contiguous parts (districts).
#[derive(Clone, Debug)]
pub(crate) struct Partition {
    graph: Arc<Graph>,                     // Fixed graph structure
    region: Arc<Graph>,                    // Reference to full region (for access to totals)
    pub(super) parts: PartitionSet,        // Sets of nodes in each part (including unassigned 0)
    pub(super) frontiers: MultiSet,        // Nodes on the boundary of each part
    pub(super) part_weights: WeightMatrix, // Aggregated weights for each part
    // pub(super) part_hulls: Vec<Polygon<f64>>,   // Convex hull for each part
}

impl Partition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
    pub(crate) fn new(num_parts: usize, graph: impl Into<Arc<Graph>>, region: impl Into<Arc<Graph>>) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");
        let graph: Arc<Graph> = graph.into();
        let region: Arc<Graph> = region.into();

        let mut part_weights = graph.node_weights().copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, graph.node_weights());

        Self {
            parts: PartitionSet::new(num_parts, graph.node_count()),
            frontiers: MultiSet::new(num_parts, graph.node_count()),
            part_weights,
            // part_hulls: 
            graph,
            region,
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

    /// Sum of a given series for a specific part.
    pub(crate) fn part_total(&self, series: &str, part: u32) -> f64 {
        self.part_weights.get_as_f64(series, part as usize).unwrap()
    }

    /// Sum of a given series for each part (including unassigned 0).
    pub(crate) fn part_totals(&self, series: &str) -> Vec<f64> {
        (0..self.num_parts())
            .map(|part| self.part_total(series, part))
            .collect::<Vec<_>>()
    }

    /// Get the total weight of the entire region for a given series.
    pub(crate) fn region_total(&self, series: &str) -> f64 {
        self.region.node_weights().get_as_f64(series, 0).unwrap()
    }

    /// Update part weight totals for a single node move (from prev to part).
    pub(super) fn update_part_totals_for_node_move(&mut self, node: usize, prev: u32, part: u32) {
        self.part_weights.subtract_row_from(prev as usize, self.graph.node_weights(), node);
        self.part_weights.add_row_from(part as usize, self.graph.node_weights(), node);
    }

    /// Update part weight totals for a subgraph move (from prev to part).
    pub(super) fn update_part_totals_for_subgraph_move(&mut self, subgraph: &[usize], prev: u32, part: u32) {
        self.part_weights.subtract_rows_from(prev as usize, self.graph.node_weights(), subgraph);
        self.part_weights.add_rows_from(part as usize, self.graph.node_weights(), subgraph);
    }
}
