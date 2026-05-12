use crate::partition::{Partition, structures::SpanningTree};

impl Partition {
    /// Generate a random spanning tree for all nodes in `part`, using Wilson's algorithm.
    /// Assumes the part is fully connected.
    pub(crate) fn random_spanning_tree_for_part<R: rand::Rng + ?Sized>(&self, part: u32, rng: &mut R) -> SpanningTree {
        let nodes = self.parts.get(part as usize).to_vec();
        assert!(!nodes.is_empty(), "cannot build spanning tree for empty part {}", part);
        SpanningTree::random_wilson(nodes, self.num_nodes(), rng, |node, rng| {
            self.random_same_part_edge(node, rng)
        })
    }

    /// Generate a random spanning tree over the entire graph, using Wilson's algorithm.
    /// Assumes the graph is fully connected.
    #[allow(unused)]
    pub(crate) fn random_spanning_tree<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> SpanningTree {
        let nodes: Vec<usize> = (0..self.num_nodes()).collect();
        SpanningTree::random_wilson(nodes, self.num_nodes(), rng, |node, rng| {
            self.random_edge(node, rng)
        })
    }

    /// Recombine two parts by merging them into one and then repartitioning.
    /// If the two parts are not contiguous, does nothing.
    pub(crate) fn recombine_parts(&mut self, a: u32, b: u32) {
        let rng = &mut rand::thread_rng();

        let Some(other) = self.merge_parts(a, b, true) else { return };
        let merged = if other == a { b } else { a };

        let tree = self.random_spanning_tree_for_part(merged, rng);
        let cut  = tree.balanced_cut(|u| self.unit_weights().get_as_f64("T_20_CENS_Total", u).unwrap());
        let subtree = tree.subtree_nodes(cut.unwrap()).unwrap();

        self.move_subgraph(subtree, other, false);

        println!("a population: {}", self.part_weights().get_as_f64("T_20_CENS_Total", a as usize).unwrap());
        println!("b population: {}", self.part_weights().get_as_f64("T_20_CENS_Total", b as usize).unwrap());
    }
}
