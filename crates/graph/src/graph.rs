use std::collections::HashMap;

use ndarray::{s, Array1, Array2};

use crate::{WeightMatrix, WeightType};

/// A weighted, undirected graph in compressed sparse row format.
#[derive(Debug, Default)]
pub struct Graph {
    size: usize,
    offsets: Vec<u32>,
    edges: Vec<u32>,
    edge_weights: Vec<f64>,
    node_weights: WeightMatrix,
}

impl Graph {
    /// Construct a graph from adjacency lists and node weights.
    pub fn new(num_nodes: usize, edges: &[Vec<u32>], edge_weights: &[Vec<f64>],
        weights_i64: HashMap<String, Vec<i64>>,
        weights_f64: HashMap<String, Vec<f64>>,
    ) -> Self {
        assert!(edges.len() == num_nodes, "edges.len() must equal num_nodes");
        assert!(edge_weights.len() == num_nodes, "edge_weights.len() must equal num_nodes");
        edges.iter().zip(edge_weights.iter()).enumerate().for_each(|(i, (edges, weights))| {
            assert!(edges.len() == weights.len(), "edges[{i}].len() must equal edge_weights[{i}].len()");
        });

        Self {
            size: num_nodes,
            offsets: std::iter::once(0u32).chain(
                edges.iter()
                    .map(|v| v.len() as u32)
                    .scan(0u32, |acc, len| {*acc += len; Some(*acc)})
            ).collect::<Vec<u32>>(),
            edges: edges.iter().flatten().copied().collect(),
            edge_weights: edge_weights.iter().flatten().copied().collect(),
            node_weights: WeightMatrix::new(num_nodes, weights_i64, weights_f64),
        }
    }

    /// Get the number of nodes in the graph.
    #[inline] pub fn len(&self) -> usize { self.size }

    /// Check if the graph is empty (no nodes).
    #[inline] pub fn is_empty(&self) -> bool { self.size == 0 }

    /// Get the number of edges in the graph.
    #[inline] pub fn num_edges(&self) -> usize { self.edges.len() }

    /// Get a reference to the node weights matrix.
    #[inline] pub fn node_weights(&self) -> &WeightMatrix { &self.node_weights }

    /// Get the range of edges for a given node.
    #[inline]
    fn range(&self, node: usize) -> std::ops::Range<usize> {
        self.offsets[node] as usize..self.offsets[node + 1] as usize
    }

    /// Check if a node has no neighbors.
    #[inline] pub fn is_isolated(&self, node: usize) -> bool { self.range(node).is_empty() }

    /// Get the degree (number of neighbors) of a given node.
    #[inline] pub fn degree(&self, node: usize) -> usize { self.range(node).len() }

    /// Get the ith neighbor of a given node.
    #[inline]
    pub fn edge(&self, node: usize, i: usize) -> Option<usize> {
        self.range(node).nth(i).map(|v| self.edges[v] as usize)
    }

    /// Get an iterator over the neighbors of a given node.
    #[inline]
    pub fn edges(&self, node: usize) -> impl Iterator<Item = usize> + '_ {
        self.range(node).map(move |v| self.edges[v] as usize)
    }

    /// Get an iterator over the neighbors and edge weights of a given node.
    #[inline]
    pub fn edges_with_weights(&self, node: usize) -> impl Iterator<Item = (usize, f64)> + '_ {
        self.range(node).map(move |v| (self.edges[v] as usize, self.edge_weights[v]))
    }
}
