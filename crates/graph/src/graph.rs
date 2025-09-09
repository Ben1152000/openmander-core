use std::collections::HashMap;

use ndarray::{s, Array1, Array2};

use crate::{WeightMatrix, WeightType};

/// A weighted, undirected graph in compressed sparse row format.
#[derive(Debug, Default)]
pub struct Graph {
    size: usize,
    pub offsets: Vec<u32>,
    pub edges: Vec<u32>,
    pub edge_weights: Vec<f64>,
    pub node_weights: WeightMatrix,
}

impl Graph {
    #[inline] pub fn len(&self) -> usize { self.size }

    #[inline]
    pub fn range(&self, i: usize) -> std::ops::Range<usize> {
        self.offsets[i] as usize..self.offsets[i + 1] as usize
    }

    #[inline]
    pub fn edges(&self, u: usize) -> impl Iterator<Item = usize> + '_ {
        self.range(u).map(move |v| self.edges[v] as usize)
    }

    #[inline]
    pub fn edges_with_weights(&self, u: usize) -> impl Iterator<Item = (usize, f64)> + '_ {
        self.range(u).map(move |v| (self.edges[v] as usize, self.edge_weights[v]))
    }

    /// Construct a graph from adjacency lists and node weights.
    pub fn new(
        num_nodes: usize,
        edges: &[Vec<u32>],
        edge_weights: &[Vec<f64>],
        weights_i64: HashMap<String, Vec<i64>>,
        weights_f64: HashMap<String, Vec<f64>>,
    ) -> Self {
        assert!(edges.len() == num_nodes, "edges.len() must equal num_nodes");
        assert!(edge_weights.len() == num_nodes, "edge_weights.len() must equal num_nodes");
        edges.iter().zip(edge_weights.iter()).enumerate().for_each(|(i, (edges, weights))| {
            assert!(edges.len() == weights.len(), "edges[{i}].len() must equal edge_weights[{i}].len()");
        });

        let mut node_weights = WeightMatrix {
            series: HashMap::new(),
            i64: Array2::<i64>::zeros((num_nodes, weights_i64.len())),
            f64: Array2::<f64>::zeros((num_nodes, weights_f64.len())),
        };

        weights_i64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == num_nodes, "weights_i64[{}].len() must equal num_nodes", name);
            node_weights.i64.slice_mut(s![.., i]).assign(&Array1::from(values));
            node_weights.series.insert(name, (WeightType::I64, i));
        });

        weights_f64.into_iter().enumerate().for_each(|(i, (name, values))| {
            assert!(values.len() == num_nodes, "weights_f64[{}].len() must equal num_nodes", name);
            node_weights.f64.slice_mut(s![.., i]).assign(&Array1::from(values));
            node_weights.series.insert(name, (WeightType::F64, i));
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
            node_weights: node_weights,
        }
    }
}
