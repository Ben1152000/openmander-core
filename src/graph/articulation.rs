use std::collections::{HashSet, VecDeque};

use crate::graph::Graph;

impl Graph {
    /// If `node` is an articulation point in the underlying undirected graph (ignoring assignments),
    /// return the nodes in the smaller components created by removing `node`.
    pub(crate) fn cut_subgraph(&self, node: usize) -> Vec<usize> {
        assert!(node < self.node_count(), "node {} out of range", node);

        // If fewer than 2 neighbors, node cannot be an articulation point.
        let neighbors = self.edges(node).collect::<Vec<_>>();
        if neighbors.len() <= 1 { return vec![] }

        let mut components = neighbors.iter()
            .map(|&u| vec![u])
            .collect::<Vec<_>>();

        // Index of component that reached v, or usize::MAX if unvisited
        let mut in_component = vec![usize::MAX; self.node_count()];
        in_component[node] = self.node_count(); // mark removed
        for i in 0..neighbors.len() { in_component[neighbors[i]] = i }

        // One queue per component seed (each neighbor).
        let mut queues = neighbors.iter()
            .map(|&u| VecDeque::from([u]))
            .collect::<Vec<_>>();

        // Tiny union-find data structure over label indices to merge when frontiers meet.
        struct UnionFind { parent: Vec<usize>, reps: Vec<u8>, components: usize }

        impl UnionFind {
            fn new(n: usize) -> Self {
                Self { parent: (0..n).collect(), reps: vec![0; n], components: n }
            }

            fn find(&mut self, u: usize) -> usize {
                if self.parent[u] == u { u } else {
                    let rep = self.find(self.parent[u]);
                    self.parent[u] = rep;
                    rep
                }
            }

            fn union(&mut self, a: usize, b: usize) {
                let (mut a, mut b) = (self.find(a), self.find(b));
                if a == b { return }
                if self.reps[a] < self.reps[b] { std::mem::swap(&mut a, &mut b) }
                self.parent[b] = a;
                if self.reps[a] == self.reps[b] { self.reps[a] += 1 }
                self.components -= 1;
            }
        }

        let mut union_find = UnionFind::new(neighbors.len());

        // Round-robin expansion; stop when a single label-group remains active.
        loop {
            // Count active union-find roots with non-empty queues
            let active_roots = (0..neighbors.len())
                .filter(|&i| !queues[i].is_empty())
                .map(|i| union_find.find(i))
                .collect::<HashSet<_>>();

            // Check if all but one BFS has completed
            if active_roots.len() <= 1 { break }

            // If all components have merged, node is not an articulation point
            if union_find.components == 1 { return vec![] }

            for i in 0..neighbors.len() {
                if let Some(u) = queues[i].pop_front() {
                    for v in self.edges(u) {
                        if v == node { continue }
                        if in_component[v] == usize::MAX {
                            in_component[v] = i;
                            queues[i].push_back(v);
                            components[i].push(v);
                        } else if in_component[v] < neighbors.len() && in_component[v] != i {
                            // BFS encountered node from another component
                            union_find.union(i, in_component[v]);
                        }
                    }
                }
            }
        }

        // Determine main (still-active) root; if none, use largest by size.
        let active_roots = (0..neighbors.len())
            .filter(|&i| !queues[i].is_empty())
            .map(|i| union_find.find(i))
            .collect::<HashSet<_>>();

        let main_root = active_roots.iter().copied().next().unwrap_or({
            let mut size_by_root = vec![0; neighbors.len()];
            for i in 0..neighbors.len() {
                size_by_root[union_find.find(i)] += components[i].len();
            }
            (0..neighbors.len()).max_by_key(|&r| size_by_root[r]).unwrap()
        });

        // Collect nodes from all non-main components.
        components.into_iter().enumerate()
            .filter(|(i, _)| union_find.find(*i) != main_root)
            .flat_map(|(_, component)| component)
            .collect()
    }
}
