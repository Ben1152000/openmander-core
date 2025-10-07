use rand::{distr::{weighted::WeightedIndex, Distribution}, seq::{IndexedRandom, IteratorRandom}, Rng};

use crate::partition::Partition;

impl Partition {
    /// Select a random node from the map.
    pub(crate) fn random_node(&self, rng: &mut impl Rng) -> usize {
        rng.random_range(0..self.graph().node_count())
    }

    /// Select a random node from a given part.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_node_from_part(&self, part: u32, rng: &mut impl Rng) -> Option<usize> {
        self.parts.get(part as usize).choose(rng).copied()
    }

    /// Select a random unassigned node from the map.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_unassigned_node(&self, rng: &mut impl Rng) -> Option<usize> {
        self.random_node_from_part(0, rng)
    }

    /// Select a random unassigned node from the map that is on a part boundary.
    pub(crate) fn random_unassigned_boundary_node(&self, rng: &mut impl Rng) -> Option<usize> {
        let set = self.frontiers.get(0);
        if set.is_empty() { None } else { Some(set[rng.random_range(0..set.len())]) }
    }

    /// Select a random neighbor of a given node.
    pub(crate) fn random_edge(&self, node: usize, rng: &mut impl Rng) -> Option<usize> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        Some(self.graph().edge(node, rng.random_range(0..self.graph().degree(node)) as usize).unwrap())
    }

    /// Select a random neighbor of a given node that is in the same part.
    pub(crate) fn random_same_part_edge(&self, node: usize, rng: &mut impl Rng) -> Option<usize> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        let part = self.assignment(node);
        let same_part_neighbors = self.graph().edges(node)
            .filter(|&v| self.assignment(v) == part)
            .collect::<Vec<_>>();
        if same_part_neighbors.is_empty() { None }
        else { same_part_neighbors.choose(rng).copied() }
    }

    /// Select a random neighboring part of a given node.
    pub(crate) fn random_neighboring_part(&self, node: usize, rng: &mut impl Rng) -> Option<u32> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        self.graph().edges(node)
            .map(|v| self.assignment(v))
            .filter(|&p| p != self.assignment(node))
            .choose(rng)
    }

    /// Select a random part, weighted by frontier size.
    pub(crate) fn random_part_weighted_by_frontier<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<u32> {
        let weights = self.frontiers.iter()
            .map(|set| set.len().saturating_sub(1))
            .collect::<Vec<_>>();
        let dist = WeightedIndex::new(&weights).ok()?; // None if all weights are zero
        Some(dist.sample(rng) as u32)
    }

    /// Randomly assign all nodes to contiguous parts.
    pub(crate) fn randomize(&mut self) {
        let mut rng = rand::rng();
        self.clear_assignments();

        // Seed parts with random starting nodes.
        for part in 1..self.num_parts() {
            self.move_node(self.random_unassigned_node(&mut rng).unwrap(), part, false);
        }

        // Expand parts until all nodes are assigned.
        while let Some(u) = self.random_unassigned_boundary_node(&mut rng) {
            self.move_node(u, self.random_neighboring_part(u, &mut rng).unwrap(), false);
        }
    }
}
