use std::sync::Arc;

use crate::{
    graph::{Graph, WeightMatrix},
    partition::{MultiSet, PartitionSet},
};

/// A partition of a graph into contiguous parts (districts).
#[derive(Clone, Debug)]
pub(crate) struct Partition {
    pub(super) parts: PartitionSet,  // Sets of nodes in each part (including unassigned 0)
    pub(super) frontiers: MultiSet,  // Nodes on the boundary of each part
    pub(super) part_graph: Graph,    // Graph structure for parts (including aggregated weights)
    units_graph: Arc<Graph>,         // Reference to unit graph (for access to weights)
    region_graph: Arc<Graph>,        // Reference to full region (for access to totals)
}

impl Partition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
    pub(crate) fn new(num_parts: usize, units_graph: impl Into<Arc<Graph>>, region_graph: impl Into<Arc<Graph>>) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");
        let units_graph: Arc<Graph> = units_graph.into();
        let region_graph: Arc<Graph> = region_graph.into();

        let mut part_weights = units_graph.node_weights().copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, units_graph.node_weights());

        let part_graph = Graph::new(
            num_parts,
            &vec![vec![]; num_parts],
            &vec![vec![]; num_parts],
            part_weights,
            &vec![],
        );

        Self {
            parts: PartitionSet::new(num_parts, units_graph.node_count()),
            frontiers: MultiSet::new(num_parts, units_graph.node_count()),
            part_graph,
            units_graph,
            region_graph,
        }
    }

    /// Get the number of parts in this partition (including unassigned 0).
    #[inline] pub(crate) fn num_parts(&self) -> u32 { self.parts.num_sets() as u32 }

    /// Get the number of nodes in the underlying graph.
    #[inline] pub(crate) fn num_nodes(&self) -> usize { self.units_graph.node_count() }

    /// Get a reference to the underlying graph.
    #[inline] pub(crate) fn graph(&self) -> &Graph { &self.units_graph }

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

        self.part_graph.node_weights_mut().clear_all_rows();
        self.part_graph.node_weights_mut().set_row_to_sum_of(0, self.units_graph.node_weights());
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
            self.units_graph.edges(u).any(|v| self.assignment(v) != part)
        }).collect::<Vec<_>>();

        // Recompute frontiers.
        self.frontiers.rebuild_from(
            self.assignments().iter().enumerate()
            .filter_map(|(node, &part)| {
                on_boundary[node].then_some((node, part as usize))
            })
        );

        // Recompute per-part totals.
        let mut part_weights = WeightMatrix::copy_of_size(self.units_graph.node_weights(), self.num_parts() as usize);
        for (node, &part) in self.assignments().iter().enumerate() {
            part_weights.add_row_from(part as usize, self.units_graph.node_weights(), node);
        }

        // Rebuild part graph.
        // Todo: compute part adjacencies
        self.part_graph = Graph::new(
            self.num_parts() as usize,
            &vec![vec![]; self.num_parts() as usize],
            &vec![vec![]; self.num_parts() as usize],
            part_weights,
            &vec![],
        );
    }

    /// Sum of a given series for a specific part.
    pub(crate) fn part_total(&self, series: &str, part: u32) -> f64 {
        self.part_graph.node_weights().get_as_f64(series, part as usize).unwrap()
    }

    /// Sum of a given series for each part (including unassigned 0).
    pub(crate) fn part_totals(&self, series: &str) -> Vec<f64> {
        (0..self.num_parts())
            .map(|part| self.part_total(series, part))
            .collect()
    }

    /// Get the total weight of the entire region for a given series.
    pub(crate) fn region_total(&self, series: &str) -> f64 {
        self.region_graph.node_weights().get_as_f64(series, 0).unwrap()
    }

    /// Get a reference to the part weights matrix.
    pub(super) fn part_weights(&self) -> &WeightMatrix { self.part_graph.node_weights() }

    /// Get a mutable reference to the part weights matrix.
    pub(super) fn part_weights_mut(&mut self) -> &mut WeightMatrix { self.part_graph.node_weights_mut() }

    /// Update part weight totals for a single node move (from prev to part).
    pub(super) fn update_on_node_move(&mut self, node: usize, prev: u32, part: u32) {
        // Add/subtract node weights from part totals.
        self.part_graph.node_weights_mut().subtract_row_from(
            prev as usize,
            self.units_graph.node_weights(),
            node,
        );
        self.part_graph.node_weights_mut().add_row_from(
            part as usize,
            self.units_graph.node_weights(),
            node,
        );

        // Todo: update graph of parts
    }

    /// Update part weight totals for a subgraph move (from prev to part).
    pub(super) fn update_on_subgraph_move(&mut self, subgraph: &[usize], prev: u32, part: u32) {
        // Add/subtract node weights from part totals.
        self.part_graph.node_weights_mut().subtract_rows_from(
            prev as usize,
            self.units_graph.node_weights(),
            subgraph,
        );
        self.part_graph.node_weights_mut().add_rows_from(
            part as usize,
            self.units_graph.node_weights(),
            subgraph,
        );

        // Todo: update graph of parts
    }
}
