use std::sync::Arc;

use geograph::{Region, UnitId};

/// A graph backend for unit-level (block-level) graph operations.
///
/// Wraps a `Region` object from the geograph crate,
/// providing a uniform interface for partition algorithms.
#[derive(Clone)]
pub(crate) struct UnitGraph(pub Arc<Region>);

impl std::fmt::Debug for UnitGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UnitGraph::Region")
            .field(&format_args!("{} units", self.0.num_units()))
            .finish()
    }
}

impl UnitGraph {
    /// Number of nodes in the graph.
    #[inline] pub(crate) fn node_count(&self) -> usize { self.0.num_units() }

    /// Total number of directed edges.
    #[inline]
    pub(crate) fn edge_count(&self) -> usize {
        self.0.adjacency().num_directed_edges()
    }

    /// Degree of a node (number of neighbors).
    #[inline]
    pub(crate) fn degree(&self, node: usize) -> usize {
        self.0.adjacency().degree(UnitId(node as u32))
    }

    /// CSR offset for a node's edge list.
    #[inline]
    pub(crate) fn offset(&self, node: usize) -> usize {
        self.0.adjacency().offset(UnitId(node as u32))
    }

    /// Whether a node borders the exterior (state boundary).
    #[inline]
    pub(crate) fn is_exterior(&self, node: usize) -> bool {
        self.0.is_exterior(UnitId(node as u32))
    }

    /// Get the (source, target) pair for a directed edge index.
    pub(crate) fn edge_endpoints(&self, edge_idx: usize) -> Option<(usize, usize)> {
        self.0.adjacency()
            .edge_at(edge_idx)
            .map(|(s, t)| (s.0 as usize, t.0 as usize))
    }

    /// Get the ith neighbor of a node.
    #[inline]
    pub(crate) fn edge(&self, node: usize, i: usize) -> Option<usize> {
        self.0.adjacency()
            .neighbors(UnitId(node as u32))
            .get(i)
            .map(|u| u.0 as usize)
    }

    /// Iterate over neighbors of a node.
    #[inline]
    pub(crate) fn edges(&self, node: usize) -> UnitGraphEdgeIter<'_> {
        UnitGraphEdgeIter(self.0.adjacency().neighbors(UnitId(node as u32)).iter())
    }

    /// Iterate over (neighbor, edge_weight) pairs for a node.
    #[inline]
    pub(crate) fn edges_with_weights(&self, node: usize) -> UnitGraphEdgeWeightIter<'_> {
        let uid = UnitId(node as u32);
        let adj = self.0.adjacency();
        UnitGraphEdgeWeightIter {
            neighbors: adj.neighbors(uid).iter(),
            weights: adj.weights_of(uid).iter(),
        }
    }
}

impl From<Arc<Region>> for UnitGraph {
    fn from(r: Arc<Region>) -> Self { Self(r) }
}

/// Zero-allocation iterator over a node's neighbors, yielding `usize`.
pub(crate) struct UnitGraphEdgeIter<'a>(std::slice::Iter<'a, UnitId>);

impl Iterator for UnitGraphEdgeIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        self.0.next().map(|u| u.0 as usize)
    }

    #[inline] fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}

impl ExactSizeIterator for UnitGraphEdgeIter<'_> {}

/// Zero-allocation iterator over a node's (neighbor, weight) pairs.
pub(crate) struct UnitGraphEdgeWeightIter<'a> {
    neighbors: std::slice::Iter<'a, UnitId>,
    weights: std::slice::Iter<'a, f64>,
}

impl Iterator for UnitGraphEdgeWeightIter<'_> {
    type Item = (usize, f64);

    #[inline]
    fn next(&mut self) -> Option<(usize, f64)> {
        Some((self.neighbors.next()?.0 as usize, *self.weights.next()?))
    }

    #[inline] fn size_hint(&self) -> (usize, Option<usize>) { self.neighbors.size_hint() }
}

impl ExactSizeIterator for UnitGraphEdgeWeightIter<'_> {}
