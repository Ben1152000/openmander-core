use std::collections::HashMap;

use crate::graph::WeightMatrix;

/// A weighted, undirected graph in compressed sparse row format.
#[derive(Debug, Default)]
pub(crate) struct Graph {
    size: usize,
    offsets: Vec<u32>,
    edges: Vec<u32>,
    edge_weights: Vec<f64>,
    node_weights: WeightMatrix,
}

impl Graph {
    /// Construct a graph from adjacency lists and node weights.
    pub(crate) fn new(num_nodes: usize, edges: &[Vec<u32>], edge_weights: &[Vec<f64>],
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
    #[inline] pub(crate) fn node_count(&self) -> usize { self.size }

    /// Get the number of edges in the graph.
    #[inline] pub(crate) fn edge_count(&self) -> usize { self.edges.len() }

    /// Get a reference to the node weights matrix.
    #[inline] pub(crate) fn node_weights(&self) -> &WeightMatrix { &self.node_weights }

    /// Get the range of edges for a given node.
    #[inline]
    fn range(&self, node: usize) -> std::ops::Range<usize> {
        self.offsets[node] as usize .. self.offsets[node + 1] as usize
    }

    /// Get the degree (number of neighbors) of a given node.
    #[inline] pub(crate) fn degree(&self, node: usize) -> usize { self.range(node).len() }

    /// Get the ith neighbor of a given node.
    #[inline]
    pub(crate) fn edge(&self, node: usize, i: usize) -> Option<usize> {
        self.range(node).nth(i).map(|v| self.edges[v] as usize)
    }

    /// Get an iterator over the neighbors of a given node.
    #[inline]
    pub(crate) fn edges(&self, node: usize) -> impl Iterator<Item = usize> + '_ {
        self.range(node).map(move |v| self.edges[v] as usize)
    }

    /// Get an iterator over the neighbors and edge weights of a given node.
    #[inline]
    pub(crate) fn edges_with_weights(&self, node: usize) -> impl Iterator<Item = (usize, f64)> + '_ {
        self.range(node).map(move |v| (self.edges[v] as usize, self.edge_weights[v]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_test_graph() -> Graph {
        Graph::new(
            4,
            &[
                vec![1, 2],       // 0
                vec![0, 2],       // 1
                vec![0, 1, 3],    // 2
                vec![2],          // 3
            ],
            &[
                vec![1.5, 2.0],
                vec![1.5, 3.5],
                vec![2.0, 3.5, 0.5],
                vec![0.5],
            ],
            HashMap::new(),
            HashMap::new(),
        )
    }

    #[test]
    fn csr_graph_construction() {
        let graph = make_test_graph();

        // Basic counts
        assert_eq!(graph.node_count(), 4);
        assert_eq!(graph.edge_count(), 8);

        // Offsets are cumulative neighbor counts, len = nodes + 1
        assert_eq!(graph.offsets.len(), graph.node_count() + 1);
        assert_eq!(graph.offsets, vec![0, 2, 4, 7, 8]);

        // Flattened neighbor list & weights are in insertion order
        assert_eq!(graph.edges,        vec![  1,   2,   0,   2,   0,   1,   3,   2]);
        assert_eq!(graph.edge_weights, vec![1.5, 2.0, 1.5, 3.5, 2.0, 3.5, 0.5, 0.5]);

        // CSR invariant: last offset == total edge entries == #weights
        assert_eq!(*graph.offsets.last().unwrap() as usize, graph.edges.len());
        assert_eq!(graph.edges.len(), graph.edge_weights.len());

        // Offsets must be non-decreasing
        for window in graph.offsets.windows(2) { assert!(window[0] <= window[1]) }
    }

    #[test]
    fn range_matches_offsets() {
        let graph = make_test_graph();
        for i in 0..graph.node_count() {
            let range = graph.range(i);
            let expected = graph.offsets[i] as usize .. graph.offsets[i + 1] as usize;
            assert_eq!(range, expected);
        }
    }

    #[test]
    fn degree_matches_offsets() {
        let graph = make_test_graph();

        assert_eq!(graph.degree(0), 2);
        assert_eq!(graph.degree(1), 2);
        assert_eq!(graph.degree(2), 3);
        assert_eq!(graph.degree(3), 1);
    }

    #[test]
    fn edge_access() {
        let graph = make_test_graph();

        // Random spot checks for edge(indexed) access
        assert_eq!(graph.edge(2, 0), Some(0));
        assert_eq!(graph.edge(2, 1), Some(1));
        assert_eq!(graph.edge(2, 2), Some(3));
        assert_eq!(graph.edge(2, 3), None); // out-of-range within node
    }

    #[test]
    fn edge_iterators() {
        let graph = make_test_graph();

        // Iterators preserve order and pair neighbors with weights
        assert_eq!(graph.edges(2).collect::<Vec<_>>(), vec![0, 1, 3]);
        assert_eq!(graph.edges_with_weights(2).collect::<Vec<_>>(), vec![(0, 2.0), (1, 3.5), (3, 0.5)]);
    }

    #[test]
    fn empty_graph_is_valid() {
        let graph = Graph::new(
            0,
            &[],
            &[],
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.offsets, vec![0]);
    }

    #[test]
    fn isolated_nodes_have_zero_degree_and_no_edges() {
        let graph = Graph::new(
            3,
            &[vec![], vec![], vec![]],
            &[vec![], vec![], vec![]],
            HashMap::new(),
            HashMap::new(),
        );

        assert_eq!(graph.offsets, vec![0, 0, 0, 0]);
        assert_eq!(graph.edge_count(), 0);

        for n in 0..3 {
            assert_eq!(graph.degree(n), 0);
            assert_eq!(graph.edge(n, 0), None);
            assert!(graph.edges(n).next().is_none());
            assert!(graph.edges_with_weights(n).next().is_none());
        }
    }

    #[test]
    #[should_panic(expected = "edges.len() must equal num_nodes")]
    fn new_panics_when_edges_len_mismatch() {
        Graph::new(
            0,
            &[vec![]],
            &[],
            HashMap::new(),
            HashMap::new(),
        );
    }

    #[test]
    #[should_panic(expected = "edge_weights.len() must equal num_nodes")]
    fn new_panics_when_edge_weights_len_mismatch() {
        Graph::new(
            0,
            &[],
            &[vec![]],
            HashMap::new(),
            HashMap::new(),
        );
    }

    #[test]
    #[should_panic(expected = "edges[0].len() must equal edge_weights[0].len()")]
    fn new_panics_when_per_node_len_mismatch() {
        let _ = Graph::new(
            2,
            &[vec![1], vec![]],
            &[vec![], vec![]],
            HashMap::new(),
            HashMap::new(),
        );
    }

    #[test]
    #[should_panic]
    fn degree_panics_for_out_of_bounds_node() {
        let graph = make_test_graph();
        graph.degree(graph.node_count());
    }

    #[test]
    #[should_panic]
    fn edges_iter_panics_for_out_of_bounds_node() {
        let graph = make_test_graph();
        // Calling edges(node) will compute range(node) immediately and panic.
        let _ = graph.edges(graph.node_count()).collect::<Vec<_>>();
    }

    #[test]
    #[should_panic]
    fn edge_accessor_panics_for_out_of_bounds_node() {
        let graph = make_test_graph();
        graph.edge(graph.node_count(), 0);
    }

    #[test]
    fn node_weights_reference_is_accessible() {
        let graph = make_test_graph();
        // Just ensure the reference can be borrowed repeatedly without mutation.
        let p1 = graph.node_weights() as *const _;
        let p2 = graph.node_weights() as *const _;
        assert_eq!(p1, p2);
    }
}
