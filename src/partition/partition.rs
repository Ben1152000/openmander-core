use std::{collections::HashSet, sync::Arc};

use crate::{
    graph::{UnitGraph, WeightMatrix},
    partition::{FrontierEdgeList, MultiSet, PartGraph, PartitionSet},
};

/// A partition of a graph into contiguous parts (districts).
#[derive(Clone, Debug)]
pub(crate) struct Partition {
    pub(super) parts: PartitionSet,          // Sets of nodes in each part (including unassigned 0)
    pub(super) frontiers: MultiSet,          // Nodes on the boundary of each part
    pub(super) frontier_edges: FrontierEdgeList, // Half-edges on the boundary of each part
    pub(super) part_graph: PartGraph,        // Aggregated weights and perimeters for each part
    unit_graph: UnitGraph,                   // Graph topology for basic units (census block)
    unit_weights: Arc<WeightMatrix>,         // Demographic/election weights for basic units
    region_weights: Arc<WeightMatrix>,       // Summed weights for the entire region (state totals)
}

impl Partition {
    /// Construct an empty partition from a unit graph, unit weights, and number of parts.
    pub(crate) fn new(
        num_parts: usize,
        unit_graph: UnitGraph,
        unit_weights: Arc<WeightMatrix>,
        region_weights: Arc<WeightMatrix>,
    ) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");

        let mut part_weights = unit_weights.copy_of_size(num_parts);
        part_weights.set_row_to_sum_of(0, &unit_weights);
        let part_graph = PartGraph::new(num_parts, part_weights);

        // edge_count() returns total directed edges (each undirected edge counted twice)
        let num_directed_edges = unit_graph.edge_count();

        Self {
            parts: PartitionSet::new(num_parts, unit_graph.node_count()),
            frontiers: MultiSet::new(num_parts, unit_graph.node_count()),
            frontier_edges: FrontierEdgeList::new(num_parts, num_directed_edges / 2),
            part_graph,
            unit_graph,
            unit_weights,
            region_weights,
        }
    }

    /// Get the number of parts in this partition (including unassigned 0).
    pub(crate) fn num_parts(&self) -> u32 { self.parts.num_sets() as u32 }

    /// Get the number of nodes in the underlying graph.
    pub(crate) fn num_nodes(&self) -> usize { self.unit_graph.node_count() }

    /// Get the list of weight series available in the map's node weights.
    pub(crate) fn series(&self) -> HashSet<String> { self.unit_weights.series() }

    /// Get a reference to the underlying unit graph.
    pub(super) fn graph(&self) -> &UnitGraph { &self.unit_graph }

    /// Get a reference to the unit weights.
    pub(super) fn unit_weights(&self) -> &WeightMatrix { &self.unit_weights }

    /// Get the part assignment of a given node.
    pub(crate) fn assignment(&self, node: usize) -> u32 { self.parts.find(node) as u32 }

    /// Get a complete vector of assignments for each node.
    pub(crate) fn assignments(&self) -> Vec<u32> {
        self.parts.assignments().iter().map(|&p| p as u32).collect()
    }

    /// Get all nodes belonging to a given part.
    pub(crate) fn part_nodes(&self, part: u32) -> Vec<usize> {
        self.parts.assignments()
            .iter()
            .enumerate()
            .filter_map(|(node, &p)| if p == part as usize { Some(node) } else { None })
            .collect()
    }

    /// Get the set of boundary nodes for a given part.
    pub(crate) fn frontier(&self, part: u32) -> &[usize] { self.frontiers.get(part as usize) }

    /// Clear all assignments, setting every node to unassigned (0).
    pub(crate) fn clear_assignments(&mut self) {
        self.parts.clear();
        self.frontiers.clear();
        self.frontier_edges.clear();

        self.part_graph.node_weights_mut().clear_all_rows();
        self.part_graph.node_weights_mut().set_row_to_sum_of(0, &self.unit_weights);
        self.part_graph.clear_perimeters();
    }

    /// Whether a node is on the frontier of its part: it has a graph
    /// neighbor in a different part, or it borders the state exterior.
    #[inline]
    pub(super) fn is_frontier_node(&self, node: usize) -> bool {
        self.unit_graph.is_exterior(node)
            || self.unit_graph.edges(node).any(|v| self.assignment(v) != self.assignment(node))
    }

    /// Get the directed edge index for the edge from node u at local index i.
    /// In CSR format, this is simply offsets[u] + i.
    #[inline]
    fn directed_edge_index(&self, node: usize, local_idx: usize) -> usize {
        self.unit_graph.offset(node) + local_idx
    }

    /// Get the set of frontier edges for a given part.
    pub(crate) fn frontier_edges(&self, part: u32) -> &[usize] {
        self.frontier_edges.get(part as usize)
    }

    /// Get the (source, target) node pairs for all frontier edges of a given part.
    /// Each edge is a directed half-edge from a node in `part` to a node in a different part.
    pub(crate) fn frontier_edge_endpoints(&self, part: u32) -> Vec<(usize, usize)> {
        self.frontier_edges.get(part as usize)
            .iter()
            .filter_map(|&edge_idx| self.unit_graph.edge_endpoints(edge_idx))
            .collect()
    }

    /// Verify that frontier edges are consistent with current assignments.
    /// Returns true if all frontier edges are correctly tracked.
    #[cfg(debug_assertions)]
    pub(crate) fn verify_frontier_edges(&self) -> bool {
        use std::collections::HashSet;

        // Collect expected frontier edges from scratch.
        let mut expected: HashSet<(usize, usize)> = HashSet::new(); // (edge_idx, part)
        for u in 0..self.num_nodes() {
            let part_u = self.assignment(u) as usize;
            for (local_idx, v) in self.unit_graph.edges(u).enumerate() {
                let part_v = self.assignment(v) as usize;
                if part_u != part_v {
                    let edge_idx = self.directed_edge_index(u, local_idx);
                    expected.insert((edge_idx, part_u));
                }
            }
        }

        // Check all expected edges are present
        for &(edge_idx, part) in &expected {
            match self.frontier_edges.find(edge_idx) {
                Some(p) if p == part => {}
                Some(p) => {
                    eprintln!("Frontier edge {} should be in part {} but is in part {}", edge_idx, part, p);
                    return false;
                }
                None => {
                    eprintln!("Frontier edge {} should be in part {} but is missing", edge_idx, part);
                    return false;
                }
            }
        }

        // Check no extra edges are present
        for part in 0..self.num_parts() as usize {
            for &edge_idx in self.frontier_edges.get(part) {
                if !expected.contains(&(edge_idx, part)) {
                    eprintln!("Frontier edge {} in part {} should not exist", edge_idx, part);
                    return false;
                }
            }
        }

        true
    }

    /// Walk the frontier nodes of a part in order, forming a cycle.
    /// Returns a list of frontier nodes in traversal order, or empty if the frontier is not a simple cycle.
    pub(crate) fn frontier_cycle(&self, part: u32) -> Vec<usize> {
        use std::collections::{HashMap, HashSet};

        let frontier_nodes: HashSet<usize> = self.frontiers.get(part as usize).iter().copied().collect();
        if frontier_nodes.is_empty() {
            return vec![];
        }

        // Build adjacency among frontier nodes (only same-part neighbors)
        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        for &node in &frontier_nodes {
            let neighbors: Vec<usize> = self.unit_graph.edges(node)
                .filter(|&v| frontier_nodes.contains(&v) && self.assignment(v) == part)
                .collect();
            adj.insert(node, neighbors);
        }

        // Walk the frontier starting from any node
        let start = *frontier_nodes.iter().next().unwrap();
        let mut cycle = vec![start];
        let mut visited: HashSet<usize> = HashSet::new();
        visited.insert(start);

        let mut current = start;
        loop {
            let neighbors = adj.get(&current).map(|v| v.as_slice()).unwrap_or(&[]);
            let next = neighbors.iter().find(|&&n| !visited.contains(&n));

            match next {
                Some(&n) => {
                    cycle.push(n);
                    visited.insert(n);
                    current = n;
                }
                None => {
                    // Check if we can close the cycle
                    if neighbors.contains(&start) && cycle.len() > 2 {
                        cycle.push(start); // Close the cycle
                    }
                    break;
                }
            }
        }

        // If we didn't visit all frontier nodes, the frontier is not a simple cycle
        if visited.len() != frontier_nodes.len() {
            eprintln!("[District {}] Frontier has {} nodes but only visited {} - not a simple cycle",
                      part, frontier_nodes.len(), visited.len());
        }

        cycle
    }

    /// Generate assignments map from GeoId to district.
    pub(crate) fn set_assignments(&mut self, assignments: Vec<u32>) {
        assert!(assignments.len() == self.num_nodes(), "assignments.len() must equal number of nodes");
        assert!(assignments.iter().all(|&p| p < self.num_parts()), "all assignments must be in range [0, {})", self.num_parts());

        // Copy assignments.
        self.parts.rebuild(&assignments.iter().map(|&p| p as usize).collect::<Vec<_>>());

        // Recompute boundary flags.
        let on_boundary = (0..self.num_nodes()).map(|u| {
            self.is_frontier_node(u)
        }).collect::<Vec<_>>();

        // Recompute frontiers.
        self.frontiers.rebuild_from(
            self.assignments().iter().enumerate()
            .filter_map(|(node, &part)| {
                on_boundary[node].then_some((node, part as usize))
            })
        );

        // Recompute frontier edges.
        // A directed edge u→v belongs to part(u) if part(u) != part(v).
        self.frontier_edges.clear();
        for u in 0..self.num_nodes() {
            let part_u = self.assignment(u) as usize;
            for (local_idx, v) in self.unit_graph.edges(u).enumerate() {
                let part_v = self.assignment(v) as usize;
                if part_u != part_v {
                    let edge_idx = self.directed_edge_index(u, local_idx);
                    self.frontier_edges.insert(edge_idx, part_u);
                }
            }
        }

        // Recompute per-part totals.
        let mut part_weights = self.unit_weights.copy_of_size(self.num_parts() as usize);
        for (node, &part) in self.assignments().iter().enumerate() {
            part_weights.add_row_from(part as usize, &self.unit_weights, node);
        }

        // Rebuild part graph with fresh perimeters.
        let mut part_graph = PartGraph::new(self.num_parts() as usize, part_weights);
        for part in 0..self.num_parts() {
            for &node in self.frontiers.get(part as usize).iter() {
                for (edge, weight) in self.graph().edges_with_weights(node) {
                    let other = self.assignment(edge);
                    if other != part {
                        part_graph.add_perimeter(part as usize, other as usize, weight);
                    }
                }
            }
        }

        // Account for exterior perimeters.
        for part in 1..self.num_parts() {
            let exterior = part_graph.node_weights()
                .get_as_f64("outer_perimeter_m", part as usize)
                .unwrap_or(0.0);
            part_graph.add_perimeter(part as usize, 0, exterior);
            part_graph.add_perimeter(0, part as usize, exterior);
        }

        self.part_graph = part_graph;
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
        self.region_weights.get_as_f64(series, 0).unwrap()
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
            &self.unit_weights,
            node,
        );
        self.part_graph.node_weights_mut().add_row_from(
            next as usize,
            &self.unit_weights,
            node,
        );

        // Update perimeters between parts.
        for (edge, weight) in self.unit_graph.edges_with_weights(node) {
            let part = self.assignment(edge);
            if part != prev {
                self.part_graph.add_perimeter(prev as usize, part as usize, -weight);
                self.part_graph.add_perimeter(part as usize, prev as usize, -weight);
            }
            if part != next {
                self.part_graph.add_perimeter(next as usize, part as usize, weight);
                self.part_graph.add_perimeter(part as usize, next as usize, weight);
            }
        }

        // Account for exterior perimeter.
        let exterior = self.unit_weights
            .get_as_f64("outer_perimeter_m", node)
            .unwrap_or(0.0);
        if prev != 0 {
            self.part_graph.add_perimeter(prev as usize, 0, -exterior);
            self.part_graph.add_perimeter(0, prev as usize, -exterior);
        }
        if next != 0 {
            self.part_graph.add_perimeter(next as usize, 0, exterior);
            self.part_graph.add_perimeter(0, next as usize, exterior);
        }
    }

    /// Update part weight totals for a subgraph move (from prev to next part).
    pub(super) fn update_on_subgraph_move(&mut self, subgraph: &[usize], prev: u32, next: u32) {
        // Add/subtract node weights from part totals.
        self.part_graph.node_weights_mut().subtract_rows_from(
            prev as usize,
            &self.unit_weights,
            subgraph,
        );
        self.part_graph.node_weights_mut().add_rows_from(
            next as usize,
            &self.unit_weights,
            subgraph,
        );

        // Update perimeters between parts.
        let in_subgraph = subgraph.iter().copied().collect::<HashSet<_>>();
        for &node in subgraph {
            for (edge, weight) in self.unit_graph.edges_with_weights(node) {
                let part = self.assignment(edge);
                if part != prev && !in_subgraph.contains(&edge) {
                    self.part_graph.add_perimeter(prev as usize, part as usize, -weight);
                    self.part_graph.add_perimeter(part as usize, prev as usize, -weight);
                }
                if part != next {
                    self.part_graph.add_perimeter(next as usize, part as usize, weight);
                    self.part_graph.add_perimeter(part as usize, next as usize, weight);
                }
            }

            // Account for exterior perimeter.
            let exterior = self.unit_weights
                .get_as_f64("outer_perimeter_m", node)
                .unwrap_or(0.0);
            if prev != 0 {
                self.part_graph.add_perimeter(prev as usize, 0, -exterior);
                self.part_graph.add_perimeter(0, prev as usize, -exterior);
            }
            if next != 0 {
                self.part_graph.add_perimeter(next as usize, 0, exterior);
                self.part_graph.add_perimeter(0, next as usize, exterior);
            }
        }
    }

    pub(super) fn update_on_merge_parts(&mut self, target: u32, source: u32) {
        self.part_graph.merge_into(target as usize, source as usize);
    }
}
