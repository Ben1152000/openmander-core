/// Cut-friendly spanning tree representation.
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct SpanningTree {
    root: usize,
    parent: Vec<Option<usize>>, // parent[root] = root; None if node not in tree.
    order: Vec<usize>,          // preorder traversal (root first)
    index: Vec<Option<usize>>,  // position of each node in `order`, or None if not in tree.
    size: Vec<Option<usize>>,   // subtree sizes in preorder, or None if not in tree.
}

impl SpanningTree {
    /// Build a random spanning tree using Wilson's loop-erased random walk algorithm.
    ///
    /// - `nodes` — the set of nodes to span (shuffled to choose a random root).
    /// - `graph_size` — total number of nodes in the graph (for internal vec allocation;
    ///   `nodes` may be a subset).
    /// - `random_step(node, rng)` — returns a random neighbor of `node` within the subgraph.
    ///
    /// Assumes the induced subgraph over `nodes` is fully connected.
    pub(crate) fn random_wilson<R: rand::Rng + ?Sized, F>(
        mut nodes: Vec<usize>,
        graph_size: usize,
        rng: &mut R,
        mut random_step: F,
    ) -> Self
    where F: FnMut(usize, &mut R) -> Option<usize>,
    {
        use rand::seq::SliceRandom;
        assert!(!nodes.is_empty(), "cannot build spanning tree for empty node set");

        // Shuffle to pick a random root.
        nodes.shuffle(rng);
        let root = nodes[0];

        let mut parent = vec![None; graph_size];
        parent[root] = Some(root);

        // Loop-erased random walks (Wilson's algorithm).
        let mut walk_start    = vec![0; graph_size];
        let mut walk_position = vec![0; graph_size];

        for &start in &nodes[1..] {
            if parent[start].is_some() { continue } // already in the tree

            let mut walk = vec![start];
            walk_start[start]    = start;
            walk_position[start] = 0;

            let mut current = start;
            while parent[current].is_none() {
                current = random_step(current, rng).unwrap();

                if walk_start[current] == start && walk.get(walk_position[current]) == Some(&current) {
                    // Loop detected — erase it.
                    walk.truncate(walk_position[current] + 1);
                } else {
                    walk_start[current]    = start;
                    walk_position[current] = walk.len();
                    walk.push(current);
                }
            }

            // Stitch loop-erased path into the tree.
            while let Some(node) = walk.pop() {
                if parent[node].is_some() { continue }
                parent[node] = Some(current);
                current = node;
            }
        }

        let (order, index, size) = Self::build_preorder(&nodes, &parent, root, graph_size);
        Self { root, parent, order, index, size }
    }

    /// Build a minimum spanning tree using Prim's algorithm with caller-supplied edge weights.
    ///
    /// - `nodes` — the set of nodes to span.
    /// - `graph_size` — total number of nodes in the graph (for internal vec allocation;
    ///   `nodes` may be a subset).
    /// - `weighted_adj` — weighted adjacency list indexed by node ID; each entry is a list of
    ///   `(neighbor, weight)` pairs. Only edges between nodes in `nodes` are used.
    ///   Weights must be non-negative and finite; lower weight = preferred edge.
    ///
    /// Assumes the induced subgraph over `nodes` is fully connected.
    pub(crate) fn minimum_spanning_tree(
        nodes: Vec<usize>,
        graph_size: usize,
        weighted_adj: &[Vec<(usize, f64)>],
    ) -> Self {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        assert!(!nodes.is_empty(), "cannot build minimum spanning tree for empty node set");

        let root = nodes[0];

        let mut in_nodes = vec![false; graph_size];
        for &u in &nodes { in_nodes[u] = true; }

        let mut parent  = vec![None::<usize>; graph_size];
        let mut visited = vec![false; graph_size];

        parent[root] = Some(root);
        visited[root] = true;

        // Min-heap keyed by (weight_bits, to, from).
        // f64::to_bits() preserves order for non-negative finite values.
        let mut heap: BinaryHeap<Reverse<(u64, usize, usize)>> = BinaryHeap::new();
        for &(v, w) in &weighted_adj[root] {
            if in_nodes[v] { heap.push(Reverse((w.to_bits(), v, root))); }
        }

        while let Some(Reverse((_, v, u))) = heap.pop() {
            if visited[v] { continue; }
            visited[v] = true;
            parent[v] = Some(u);
            for &(w, wt) in &weighted_adj[v] {
                if in_nodes[w] && !visited[w] {
                    heap.push(Reverse((wt.to_bits(), w, v)));
                }
            }
        }

        let (order, index, size) = Self::build_preorder(&nodes, &parent, root, graph_size);
        Self { root, parent, order, index, size }
    }

    /// Build the preorder (DFS) traversal arrays from a parent map.
    fn build_preorder(
        nodes: &[usize],
        parent: &[Option<usize>],
        root: usize,
        graph_size: usize,
    ) -> (Vec<usize>, Vec<Option<usize>>, Vec<Option<usize>>) {
        let mut children = vec![Vec::new(); graph_size];
        for &u in nodes {
            if let Some(p) = parent[u] && p != u {
                children[p].push(u);
            }
        }

        let mut order = Vec::with_capacity(nodes.len());
        let mut index = vec![None; graph_size];
        let mut size  = vec![None; graph_size];

        let mut stack = vec![(root, false)];
        while let Some((i, entered)) = stack.pop() {
            if !entered {
                index[i] = Some(order.len());
                order.push(i);
                stack.push((i, true));
                for &u in children[i].iter().rev() { stack.push((u, false)); }
            } else {
                let count = 1 + children[i].iter().map(|&u| size[u].unwrap()).sum::<usize>();
                size[i] = Some(count);
            }
        }

        (order, index, size)
    }

    /// Whether `node` is in this tree.
    #[allow(unused)]
    #[inline] pub(crate) fn in_tree(&self, node: usize) -> bool { self.parent[node].is_some() }

    /// The nodes in the subtree rooted at `node`, in preorder, or None if `node` is not in the tree.
    #[inline]
    pub(crate) fn subtree_nodes(&self, node: usize) -> Option<&[usize]> {
        let index = self.index[node]?;
        let size  = self.size[node]?;
        Some(&self.order[index .. index + size])
    }

    /// The parent of `node`, or None if `node` is the root or not in the tree.
    #[inline]
    pub(crate) fn parent_of(&self, node: usize) -> Option<usize> {
        self.parent[node].filter(|&p| p != node)
    }

    /// Iterates non-root nodes in bottom-up order (leaves first, root's children last).
    /// Each node is guaranteed to appear before its parent — suitable for accumulating
    /// subtree values toward the root.
    #[inline]
    pub(crate) fn non_root_nodes_bottom_up(&self) -> impl Iterator<Item = usize> + '_ {
        self.order[1..].iter().rev().copied()
    }

    /// Choose a random non-root node, returning `(parent, node)`. None if the tree has ≤ 1 node.
    #[allow(unused)]
    pub(crate) fn random_tree_edge(&self, rng: &mut impl rand::Rng) -> Option<(usize, usize)> {
        use rand::seq::SliceRandom;
        if self.order.len() <= 1 { return None }
        let &u = self.order[1..].choose(rng)?;
        Some((self.parent[u].unwrap(), u))
    }

    /// Find the non-root node whose subtree weight is closest to half the total tree weight.
    /// `node_weight(u)` should return the weight (e.g. population) of node `u`.
    pub(crate) fn balanced_cut(&self, node_weight: impl Fn(usize) -> f64) -> Option<usize> {
        // Prefix sums over the preorder traversal.
        let mut prefix = vec![0.0f64; self.order.len() + 1];
        for (i, &u) in self.order.iter().enumerate() {
            prefix[i + 1] = prefix[i] + node_weight(u);
        }

        let total  = *prefix.last().unwrap();
        let target = total * 0.5;

        let mut best_cut = None;
        let mut best_err = f64::INFINITY;
        for &u in &self.order[1..] { // skip root — cutting its edge removes the entire tree
            let idx = self.index[u].unwrap();
            let sz  = self.size[u].unwrap();
            let sub = prefix[idx + sz] - prefix[idx];
            let err = (sub - target).abs();
            if err < best_err {
                best_err = err;
                best_cut = Some(u);
            }
        }

        best_cut
    }
}
