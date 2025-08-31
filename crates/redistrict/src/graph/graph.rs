use core::num;
use std::{collections::HashMap, hash::Hash};

use ndarray::{s, Array1, Array2, Axis};
use openmander_map::{GeoId, MapLayer};
use polars::{prelude::DataType, series::Series};

#[derive(Debug)]
pub enum WeightType { I64, F64 }

/// Node weights stored as type-separated matrices.
#[derive(Clone, Debug)]
pub struct WeightMatrix {
    pub i64: Array2<i64>, // (n, k_i)
    pub f64: Array2<f64>, // (n, k_f)
}

/// Compressed sparse row graph (undirected).
#[derive(Debug)]
pub struct WeightedGraph {
    pub offsets: Vec<u32>,
    pub edges: Vec<u32>,
    pub weights: Vec<f64>,
}

impl WeightedGraph {
    #[inline] pub fn range(&self, i: u32) -> std::ops::Range<usize> {
        self.offsets[i as usize] as usize..self.offsets[i as usize + 1] as usize
    }
}

/// Partition + caches for fast incremental updates.
#[derive(Debug)]
pub struct WeightedGraphPartition {
    pub num_parts: u32, // d
    pub graph: WeightedGraph,
    pub node_weights: WeightMatrix,

    pub assignments: Array1<u32>, // Current part assignment for each node (0..d-1), len = n
    pub boundary: Array1<bool>, // Whether each node is on a part boundary, len = n
    pub part_weights: WeightMatrix,
    pub series: HashMap<String, (WeightType, usize)>, // len = k_i + k_f
}

impl WeightedGraphPartition {
    /// Construct an empty partition from a map layer.
    pub fn new(
        num_parts: usize,
        num_nodes: usize,
        weights_i64: HashMap<String, Vec<i64>>,
        weights_f64: HashMap<String, Vec<f64>>,
        edges: Vec<Vec<u32>>,
        edge_weights: Vec<Vec<f64>>,
    ) -> Self {
        assert!(num_parts > 0, "num_parts must be at least 1");
        assert!(edges.len() == num_nodes, "edges.len() must equal num_nodes");
        assert!(edge_weights.len() == num_nodes, "edge_weights.len() must equal num_nodes");
        edges.iter().zip(edge_weights.iter()).enumerate().for_each(|(i, (v, w))| {
            assert!(v.len() == w.len(), "edges[{i}].len() must equal edge_weights[{i}].len()");
        });
        weights_i64.iter().for_each(|(name, v)| {
            assert!(v.len() == num_nodes, "weights_i64[{}].len() must equal num_nodes", name);
        });

        let mut node_weights = WeightMatrix {
            i64: Array2::<i64>::zeros((num_nodes, weights_i64.len())),
            f64: Array2::<f64>::zeros((num_nodes, weights_f64.len())),
        };
        let mut series: HashMap<String, (WeightType, usize)> = HashMap::new();

        weights_i64.into_iter().enumerate().for_each(|(i, (name, values))| {
            node_weights.i64.slice_mut(s![.., i]).assign(&Array1::from(values));
            series.insert(name, (WeightType::I64, i));
        });
        weights_f64.into_iter().enumerate().for_each(|(i, (name, values))| {
            node_weights.f64.slice_mut(s![.., i]).assign(&Array1::from(values));
            series.insert(name, (WeightType::F64, i));
        });

        let mut part_weights = WeightMatrix {
            i64: Array2::<i64>::zeros((num_parts, node_weights.i64.ncols())),
            f64: Array2::<f64>::zeros((num_parts, node_weights.f64.ncols())),
        };

        // initialize part 0 to contain the sum of all node weights
        part_weights.i64.row_mut(0).assign(&node_weights.i64.sum_axis(Axis(0)));
        part_weights.f64.row_mut(0).assign(&node_weights.f64.sum_axis(Axis(0)));

        Self {
            num_parts: num_parts as u32,
            assignments: Array1::<u32>::zeros(num_nodes as usize),
            boundary: Array1::<bool>::from_elem(num_nodes as usize, false),
            graph: WeightedGraph {
                offsets: std::iter::once(0u32).chain(
                    edges.iter()
                        .map(|v| v.len() as u32)
                        .scan(0u32, |acc, len| {*acc += len; Some(*acc)})
                ).collect::<Vec<u32>>(),
                edges: edges.iter().flatten().copied().collect(),
                weights: edge_weights.iter().flatten().copied().collect(),
            },
            node_weights,
            part_weights,
            series,
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
            let part = self.assignments[node];
            *flag = self.graph.range(node as u32)
                .any(|i| self.assignments[self.graph.edges[i] as usize] != part);
        });

        // Recompute per-part totals
        self.part_weights = WeightMatrix {
            i64: Array2::<i64>::zeros((self.num_parts as usize, self.node_weights.i64.ncols())),
            f64: Array2::<f64>::zeros((self.num_parts as usize, self.node_weights.f64.ncols())),
        };

        self.part_weights.i64.axis_iter_mut(Axis(0)).enumerate().for_each(
            |(i, mut acc)| {
                for (node, &part) in self.assignments.iter().enumerate() {
                    if part as usize == i {
                        acc += &self.node_weights.i64.row(node);
                    }
                }
            }
        );

        self.part_weights.f64.axis_iter_mut(Axis(0)).enumerate().for_each(
            |(i, mut acc)| {
                for (node, &part) in self.assignments.iter().enumerate() {
                    if part as usize == i {
                        acc += &self.node_weights.f64.row(node);
                    }
                }
            }
        );
    }

    /// Move a single node to a different part, updating caches.
    pub fn move_node(&mut self, node: usize, part: u32) {
        assert!(node < self.assignments.len(), "node {} out of range", node);
        assert!(part < self.num_parts, "part {} out of range [0, {})", part, self.num_parts);

        let prev = self.assignments[node];
        if prev == part { return }

        // Update aggregated integer totals (subtract from old, add to new).
        let row_i = self.node_weights.i64.row(node);
        self.part_weights.i64.row_mut(prev as usize).scaled_add(-1, &row_i);
        self.part_weights.i64.row_mut(part as usize).scaled_add(1, &row_i);

        let row_f = self.node_weights.f64.row(node);
        self.part_weights.f64.row_mut(prev as usize).scaled_add(-1.0, &row_f);
        self.part_weights.f64.row_mut(part as usize).scaled_add(1.0, &row_f);

        // Commit assignment.
        self.assignments[node] = part;

        // Recompute boundary flag for `node`.
        self.boundary[node] = self.graph.range(node as u32)
            .any(|i| self.assignments[self.graph.edges[i] as usize] != part);

        // Recompute boundary flags for neighbors of `node`.
        self.graph.range(node as u32).for_each(|i| {
            self.boundary[self.graph.edges[i] as usize] = self.graph.range(self.graph.edges[i])
                .any(|j| self.assignments[self.graph.edges[j] as usize] != self.assignments[self.graph.edges[i] as usize]);
        });
    }

    /// Move a connected subgraph to a different part, updating caches.
    pub fn move_subgraph(&mut self, nodes: Vec<usize>, part: u32) { todo!() }

}
