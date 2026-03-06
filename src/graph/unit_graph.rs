use std::sync::Arc;

use geograph::{Region, UnitId};

use super::WeightedGraph;

/// A graph backend for unit-level (block-level) graph operations.
///
/// Wraps either a legacy `WeightedGraph` or a `Region` from geograph,
/// providing a uniform interface for partition algorithms.
#[derive(Clone)]
pub(crate) enum UnitGraph {
    /// Legacy CSR-based weighted graph.
    Legacy(Arc<WeightedGraph>),
    /// Region-based graph using geograph's DCEL + adjacency matrix.
    Region(Arc<Region>),
}

impl std::fmt::Debug for UnitGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(g) => f.debug_tuple("UnitGraph::Legacy").field(g).finish(),
            Self::Region(r) => f.debug_tuple("UnitGraph::Region")
                .field(&format_args!("{} units", r.num_units()))
                .finish(),
        }
    }
}

impl UnitGraph {
    /// Number of nodes in the graph.
    #[inline]
    pub(crate) fn node_count(&self) -> usize {
        match self {
            Self::Legacy(g) => g.node_count(),
            Self::Region(r) => r.num_units(),
        }
    }

    /// Total number of directed edges.
    #[inline]
    pub(crate) fn edge_count(&self) -> usize {
        match self {
            Self::Legacy(g) => g.edge_count(),
            Self::Region(r) => r.adjacency().num_directed_edges(),
        }
    }

    /// Degree of a node (number of neighbors).
    #[inline]
    pub(crate) fn degree(&self, node: usize) -> usize {
        match self {
            Self::Legacy(g) => g.degree(node),
            Self::Region(r) => r.adjacency().degree(UnitId(node as u32)),
        }
    }

    /// CSR offset for a node's edge list.
    #[inline]
    pub(crate) fn offset(&self, node: usize) -> usize {
        match self {
            Self::Legacy(g) => g.offset(node),
            Self::Region(r) => r.adjacency().offset(UnitId(node as u32)),
        }
    }

    /// Whether a node borders the exterior (state boundary).
    #[inline]
    pub(crate) fn is_exterior(&self, node: usize) -> bool {
        match self {
            Self::Legacy(g) => g.is_exterior(node),
            Self::Region(r) => r.is_exterior(UnitId(node as u32)),
        }
    }

    /// Get the (source, target) pair for a directed edge index.
    pub(crate) fn edge_endpoints(&self, edge_idx: usize) -> Option<(usize, usize)> {
        match self {
            Self::Legacy(g) => g.edge_endpoints(edge_idx),
            Self::Region(r) => r.adjacency().edge_at(edge_idx)
                .map(|(s, t)| (s.0 as usize, t.0 as usize)),
        }
    }

    /// Get the ith neighbor of a node.
    #[inline]
    pub(crate) fn edge(&self, node: usize, i: usize) -> Option<usize> {
        match self {
            Self::Legacy(g) => g.edge(node, i),
            Self::Region(r) => r.adjacency()
                .neighbors(UnitId(node as u32))
                .get(i)
                .map(|u| u.0 as usize),
        }
    }

    /// Iterate over neighbors of a node.
    #[inline]
    pub(crate) fn edges(&self, node: usize) -> UnitGraphEdgeIter<'_> {
        match self {
            Self::Legacy(g) => UnitGraphEdgeIter::Legacy(g.neighbors_raw(node).iter()),
            Self::Region(r) => UnitGraphEdgeIter::Region(
                r.adjacency().neighbors(UnitId(node as u32)).iter()
            ),
        }
    }

    /// Iterate over (neighbor, edge_weight) pairs for a node.
    #[inline]
    pub(crate) fn edges_with_weights(&self, node: usize) -> UnitGraphEdgeWeightIter<'_> {
        match self {
            Self::Legacy(g) => {
                UnitGraphEdgeWeightIter::Legacy {
                    edges: g.neighbors_raw(node).iter(),
                    weights: g.edge_weights_for(node).iter(),
                }
            }
            Self::Region(r) => {
                let uid = UnitId(node as u32);
                let adj = r.adjacency();
                UnitGraphEdgeWeightIter::Region {
                    neighbors: adj.neighbors(uid).iter(),
                    weights: adj.weights_of(uid).iter(),
                }
            }
        }
    }
}

impl From<Arc<WeightedGraph>> for UnitGraph {
    fn from(g: Arc<WeightedGraph>) -> Self { Self::Legacy(g) }
}

impl From<Arc<Region>> for UnitGraph {
    fn from(r: Arc<Region>) -> Self { Self::Region(r) }
}

/// Zero-allocation iterator over a node's neighbors, yielding `usize`.
pub(crate) enum UnitGraphEdgeIter<'a> {
    Legacy(std::slice::Iter<'a, u32>),
    Region(std::slice::Iter<'a, UnitId>),
}

impl Iterator for UnitGraphEdgeIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            Self::Legacy(it) => it.next().map(|&v| v as usize),
            Self::Region(it) => it.next().map(|u| u.0 as usize),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Legacy(it) => it.size_hint(),
            Self::Region(it) => it.size_hint(),
        }
    }
}

impl ExactSizeIterator for UnitGraphEdgeIter<'_> {}

/// Zero-allocation iterator over a node's (neighbor, weight) pairs.
pub(crate) enum UnitGraphEdgeWeightIter<'a> {
    Legacy {
        edges: std::slice::Iter<'a, u32>,
        weights: std::slice::Iter<'a, f64>,
    },
    Region {
        neighbors: std::slice::Iter<'a, UnitId>,
        weights: std::slice::Iter<'a, f64>,
    },
}

impl Iterator for UnitGraphEdgeWeightIter<'_> {
    type Item = (usize, f64);

    #[inline]
    fn next(&mut self) -> Option<(usize, f64)> {
        match self {
            Self::Legacy { edges, weights } => {
                Some((*edges.next()? as usize, *weights.next()?))
            }
            Self::Region { neighbors, weights } => {
                Some((neighbors.next()?.0 as usize, *weights.next()?))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Legacy { edges, .. } => edges.size_hint(),
            Self::Region { neighbors, .. } => neighbors.size_hint(),
        }
    }
}

impl ExactSizeIterator for UnitGraphEdgeWeightIter<'_> {}
