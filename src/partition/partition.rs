use std::{collections::HashSet, sync::Arc};

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
    unit_graph: Arc<Graph>,          // Reference to graph of basic units (census block)
    region_graph: Arc<Graph>,        // Reference to full region graph (state)
}

impl Partition {
    /// Construct an empty partition from a weighted graph reference and number of parts.
    pub(crate) fn new(num_parts: usize, unit_graph: impl Into<Arc<Graph>>, region_graph: impl Into<Arc<Graph>>) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");
        let unit_graph: Arc<Graph> = unit_graph.into();
        let region_graph: Arc<Graph> = region_graph.into();

        let mut part_weights = unit_graph.node_weights().copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, unit_graph.node_weights());

        // Instantiate graph with a zero-length edge between each part (to be updated later).
        let part_graph = Graph::new(
            num_parts,
            &vec![(0..num_parts as u32).collect::<Vec<_>>(); num_parts],
            &vec![vec![0.0; num_parts]; num_parts],
            part_weights,
            &vec![], // To be implemented later
        );

        Self {
            parts: PartitionSet::new(num_parts, unit_graph.node_count()),
            frontiers: MultiSet::new(num_parts, unit_graph.node_count()),
            part_graph,
            unit_graph: unit_graph,
            region_graph,
        }
    }

    /// Get the number of parts in this partition (including unassigned 0).
    #[inline] pub(crate) fn num_parts(&self) -> u32 { self.parts.num_sets() as u32 }

    /// Get the number of nodes in the underlying graph.
    #[inline] pub(crate) fn num_nodes(&self) -> usize { self.unit_graph.node_count() }

    /// Get a reference to the underlying graph.
    #[inline] pub(crate) fn graph(&self) -> &Graph { &self.unit_graph }

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
        self.part_graph.node_weights_mut().set_row_to_sum_of(0, self.unit_graph.node_weights());
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
            self.unit_graph.edges(u).any(|v| self.assignment(v) != part)
        }).collect::<Vec<_>>();

        // Recompute frontiers.
        self.frontiers.rebuild_from(
            self.assignments().iter().enumerate()
            .filter_map(|(node, &part)| {
                on_boundary[node].then_some((node, part as usize))
            })
        );

        // Recompute per-part totals.
        let mut part_weights = WeightMatrix::copy_of_size(self.unit_graph.node_weights(), self.num_parts() as usize);
        for (node, &part) in self.assignments().iter().enumerate() {
            part_weights.add_row_from(part as usize, self.unit_graph.node_weights(), node);
        }

        let mut edge_weights = vec![vec![0.0; self.num_parts() as usize]; self.num_parts() as usize];
        for part in 0..self.num_parts() {
            for &u in self.frontiers.get(part as usize).iter() {
                for (v, w) in self.graph().edges_with_weights(u) {
                    let other = self.assignment(v);
                    if other != part { edge_weights[part as usize][other as usize] += w }
                }
            }
        }

        // Rebuild part graph.
        self.part_graph = Graph::new(
            self.num_parts() as usize,
            &vec![(0..self.num_parts()).collect::<Vec<_>>(); self.num_parts() as usize],
            &edge_weights,
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

    /// Update part weight totals for a single node move (from prev to next part).
    pub(super) fn update_on_node_move(&mut self, node: usize, prev: u32, next: u32) {
        // Update node weights between part totals.
        self.part_graph.node_weights_mut().subtract_row_from(
            prev as usize,
            self.unit_graph.node_weights(),
            node,
        );
        self.part_graph.node_weights_mut().add_row_from(
            next as usize,
            self.unit_graph.node_weights(),
            node,
        );

        // Update edge weights between parts.
        let size = self.num_parts();
        for (edge, weight) in self.unit_graph.edges_with_weights(node) {
            let part = self.assignment(edge);
            if part != prev {
                // Subtract edge weight from (prev, part)
                self.part_graph.edge_weights_mut()[(prev * size + part) as usize] -= weight;
                self.part_graph.edge_weights_mut()[(part * size + prev) as usize] -= weight;
            }
            if part != next {
                // Add edge weight to (next, part)
                self.part_graph.edge_weights_mut()[(next * size + part) as usize] += weight;
                self.part_graph.edge_weights_mut()[(part * size + next) as usize] += weight;
            }
        }

        // Account for exterior edge weights
        // let exterior = self.unit_graph.node_weights()
        //     .get_as_f64("outer_perimeter_m", node)
        //     .unwrap_or(0.0);
        // if prev != 0 {
        //     self.part_graph.edge_weights_mut()[(prev * size + 0) as usize] -= exterior;
        //     self.part_graph.edge_weights_mut()[(0 * size + prev) as usize] -= exterior;
        // }
        // if next != 0 {
        //     self.part_graph.edge_weights_mut()[(next * size + 0) as usize] += exterior;
        //     self.part_graph.edge_weights_mut()[(0 * size + next) as usize] += exterior;
        // }
    }

    /// Update part weight totals for a subgraph move (from prev to next part).
    pub(super) fn update_on_subgraph_move(&mut self, subgraph: &[usize], prev: u32, next: u32) {
        // Add/subtract node weights from part totals.
        self.part_graph.node_weights_mut().subtract_rows_from(
            prev as usize,
            self.unit_graph.node_weights(),
            subgraph,
        );
        self.part_graph.node_weights_mut().add_rows_from(
            next as usize,
            self.unit_graph.node_weights(),
            subgraph,
        );

        // Update edge weights between parts.
        let size = self.num_parts();
        let in_subgraph = subgraph.iter().collect::<HashSet<_>>();
        for &node in subgraph {
            for (edge, weight) in self.unit_graph.edges_with_weights(node) {
                let part = self.assignment(edge);
                if part != prev && !in_subgraph.contains(&edge) {
                    // Subtract edge weight from (prev, part)
                    self.part_graph.edge_weights_mut()[(prev * size + part) as usize] -= weight;
                    self.part_graph.edge_weights_mut()[(part * size + prev) as usize] -= weight;
                }
                if part != next {
                    // Add edge weight to (next, part)
                    self.part_graph.edge_weights_mut()[(next * size + part) as usize] += weight;
                    self.part_graph.edge_weights_mut()[(part * size + next) as usize] += weight;
                }
            }
        }

        // Todo: Account for exterior edge weights
    }

    pub(super) fn update_on_merge_parts(&mut self, target: u32, source: u32) {
        // Update part weights.
        self.part_graph.node_weights_mut().add_row(target as usize, source as usize);
        self.part_graph.node_weights_mut().clear_row(source as usize);

        // Update edge weights between parts.
        let size = self.num_parts();
        for part in 0..size {
            if part != target && part != source {
                self.part_graph.edge_weights_mut()[(target * size + part) as usize] += self.part_graph.edge_weights()[(source * size + part) as usize];
                self.part_graph.edge_weights_mut()[(part * size + target) as usize] += self.part_graph.edge_weights()[(part * size + target) as usize];
            }
            self.part_graph.edge_weights_mut()[(source * size + part) as usize] = 0.0;
            self.part_graph.edge_weights_mut()[(part * size + source) as usize] = 0.0;
        }
    }
}
