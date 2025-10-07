use crate::partition::Partition;

/// Cut-friendly spanning tree representation.
#[derive(Debug)]
struct SpanningTree {
    root: usize,
    parent: Vec<Option<usize>>, // parent[root] = root; None if node not in tree.
    order: Vec<usize>,          // preorder over nodes in the tree (components concatenated)
    index: Vec<Option<usize>>,  // preorder entry index, or None if node not in tree.
    size: Vec<Option<usize>>,   // subtree sizes, or None if node not in tree.
}

impl SpanningTree {
    #[inline] pub fn in_tree(&self, node: usize) -> bool { self.parent[node].is_some() }

    /// Subtree slice for `node` (contiguous in `order`), or None if `node` not in tree.
    #[inline]
    pub fn subtree_slice(&self, node: usize) -> Option<&[usize]> {
        let index = self.index[node]?;
        let size = self.size[node]?;
        Some(&self.order[index .. index + size])
    }

    /// Choose a random tree edge (parent[node], node), or None if |V| <= 1.
    pub fn random_edge(&self, rng: &mut impl rand::Rng) -> Option<(usize, usize)> {
        use rand::seq::IndexedRandom;
        if self.order.len() <= 1 { return None }
        let &u = self.order[1..].choose(rng)?;
        Some((self.parent[u].unwrap(), u))
    }
}

impl Partition {
    /// Generate a random spanning tree for all nodes in `part`, using Wilson's algorithm.
    /// Assumes the part is fully connected.
    fn random_spanning_tree(&self, part: u32, rng: &mut impl rand::Rng) -> SpanningTree {
        use rand::seq::SliceRandom;

        let mut nodes = self.parts.get(part as usize).to_vec();
        assert!(!nodes.is_empty(), "cannot build spanning tree for empty part {}", part);

        let mut parent = vec![None; self.num_nodes()];

        // Randomize the order of the nodes in part, and choose a random root.
        nodes.shuffle(rng);
        let root = nodes[0];
        parent[root] = Some(root);

        // Loop-erased random walks (Wilson)
        let mut walk_start = vec![0; self.num_nodes()];
        let mut walk_position = vec![0; self.num_nodes()];

        for &start in &nodes[1..] {
            if parent[start].is_some() { continue } // already in the tree

            let mut walk = vec![start];
            walk_start[start] = start;
            walk_position[start] = 0;

            // Walk until we hit the tree
            let mut current = start;
            while parent[current].is_none() {
                current = self.random_same_part_edge(current, rng).unwrap(); // step stays inside the part

                if walk_start[current] == start && walk.get(walk_position[current]) == Some(&current) {
                    walk.truncate(walk_position[current] + 1);
                } else {
                    walk_start[current] = start;
                    walk_position[current] = walk.len();
                    walk.push(current);
                }
            }

            // Stitch loop-erased path into the tree (reverse)
            while let Some(node) = walk.pop() {
                if parent[node].is_some() { continue }
                parent[node] = Some(current);
                current = node;
            }
        }

        // ---- Euler tour (preorder) for cut-friendly slices ----
        // Build children lists once (tree is single component)
        let mut children = vec![Vec::new(); self.num_nodes()];
        for &u in &nodes {
            if let Some(p) = parent[u] {
                if p != u { children[p].push(u) }
            }
        }

        let mut order = Vec::with_capacity(nodes.len());
        let mut index = vec![None; self.num_nodes()];
        let mut size = vec![None; self.num_nodes()];

        // Iterative DFS preorder from the unique root
        let mut stack = vec![(root, false)];
        while let Some((i, entered)) = stack.pop() {
            if !entered {
                index[i] = Some(order.len());
                order.push(i);
                stack.push((i, true));
                // process children in original order (reverse push)
                for &u in children[i].iter().rev() {
                    stack.push((u, false));
                }
            } else {
                let mut count = 1usize;
                for &u in &children[i] {
                    count += size[u].unwrap();
                }
                size[i] = Some(count);
            }
        }

        eprintln!("Part size: {}", nodes.len());
        eprintln!("Spanning tree size: {}", order.len());

        SpanningTree { root, parent, order, index, size }
    }

    /// Find the child `u` that yields the most balanced split when cutting (parent[u], u).
    fn balanced_cut(&self, tree: &SpanningTree, series: &str) -> Option<usize> {
        // 1) Pull node weights for the order (release the &self borrow quickly).
        let weights = tree.order.iter()
            .map(|&u| self.graph().node_weights().get_as_f64(series, u).unwrap())
            .collect::<Vec<_>>();

        // 2) Prefix sums over preorder.
        let mut prefix = Vec::with_capacity(tree.order.len() + 1);
        prefix.push(0.0);
        for &w in &weights { prefix.push(prefix.last().unwrap() + w) }

        let total = *prefix.last().unwrap();
        let target = total * 0.5;

        // 3) Scan non-root nodes; pick subtree closest to half.
        let mut best_cut = None;
        let mut best_err = f64::INFINITY;
        for &u in &tree.order[1..] { // order[0] is the unique root (contiguous part)
            let index = tree.index[u].unwrap();
            let size = tree.size[u].unwrap();
            let sub = prefix[index + size] - prefix[index];
            let err = (sub - target).abs();

            if err < best_err {
                best_err = err;
                best_cut = Some(u);
            }
        }

        eprintln!("total: {total}, target: {target}, best_err: {best_err}");

        best_cut
    }

    /// Recombine two parts by merging them into one and then repartitioning.
    /// If the two parts are not contiguous, does nothing.
    pub(crate) fn recombine_parts(&mut self, a: u32, b: u32) {
        let rng = &mut rand::rng();

        // If the two part are not contiguous, do nothing.
        let Some(other) = self.merge_parts(a, b, true) else { return };
        let merged = if other == a { b } else { a };

        // Create a rooted spanning tree for the combined part.
        let tree = self.random_spanning_tree(merged, rng);

        // Select a random edge of the spanning tree to cut the subgraph
        let edge = self.balanced_cut(&tree, "T_20_CENS_Total").unwrap();
        let subtree = tree.subtree_slice(edge).unwrap();

        self.move_subgraph(&subtree, other, false);

        println!("a population: {}", self.part_weights.get_as_f64("T_20_CENS_Total", a as usize).unwrap());
        println!("b population: {}", self.part_weights.get_as_f64("T_20_CENS_Total", b as usize).unwrap());
    }
}
