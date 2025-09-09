use rand::{seq::IteratorRandom, Rng};

use crate::partition::Partition;

impl Partition {
    /// Select a random block from the map.
    pub fn random_node(&self) -> usize {
        rand::rng().random_range(0..self.graph().len())
    }

    /// Select a random unassigned block from the map.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub fn random_unassigned_node(&self, rng: &mut impl Rng) -> Option<usize> {
        for _ in 0..32 { // Fast path: a few random probes
            let i = rng.random_range(0..self.assignments.len());
            if self.assignments[i] == 0 { return Some(i); }
        }
        self.assignments.iter().enumerate()
            .filter_map(|(i, &part)| (part == 0).then_some(i))
            .choose(rng)
    }

    /// Select a random unassigned block from the map that is on a district boundary.
    pub fn random_unassigned_boundary_node(&self, rng: &mut impl Rng) -> Option<usize> {
        let set = self.frontiers.get(0);
        if set.is_empty() { None } else { Some(set[rng.random_range(0..set.len())]) }
    }

    /// Select a random neighbor of a given block.
    pub fn random_edge(&self, node: usize, rng: &mut impl Rng) -> Option<usize> {
        assert!(node < self.graph().len(), "node {} out of range", node);
        if self.graph().is_isolated(node) { return None }
        Some(self.graph().edge(node, rng.random_range(0..self.graph().degree(node)) as usize).unwrap())
    }

    /// Select a random neighboring district of a given block.
    pub fn random_neighboring_part(&self, node: usize, rng: &mut impl Rng) -> Option<u32> {
        assert!(node < self.graph().len(), "node {} out of range", node);
        if self.graph().is_isolated(node) { return None }
        self.graph().edges(node)
            .map(|v| self.assignments[v])
            .filter(|&p| p != self.assignments[node])
            .choose(rng)
    }

    /// Randomly assign all nodes to contiguous districts.
    pub fn randomize(&mut self) {
        let mut rng = rand::rng();
        self.clear_assignments();

        // Seed districts with random starting blocks.
        for part in 1..self.num_parts() {
            self.move_node(self.random_unassigned_node(&mut rng).unwrap(), part, false);
        }

        // Expand districts until all blocks are assigned.
        while let Some(u) = self.random_unassigned_boundary_node(&mut rng) {
            self.move_node(u, self.random_neighboring_part(u, &mut rng).unwrap(), false);
        }
    }
}
