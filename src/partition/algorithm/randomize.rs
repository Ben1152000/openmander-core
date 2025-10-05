use rand::{distr::{weighted::WeightedIndex, Distribution}, seq::IteratorRandom, Rng};

use crate::partition::Partition;

impl Partition {
    /// Select a random block from the map.
    pub(crate) fn random_node(&self, rng: &mut impl Rng) -> usize {
        rng.random_range(0..self.graph().node_count())
    }

    /// Select a random block from a given district.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_node_from_part(&self, part: u32, rng: &mut impl Rng) -> Option<usize> {
        for _ in 0..32 { // Fast path: a few random probes
            let i = self.random_node(rng);
            if self.assignment(i) == part { return Some(i); }
        }
        self.assignments().iter().enumerate()
            .filter_map(|(i, &p)| (p == part).then_some(i))
            .choose(rng)
    }

    /// Select a random unassigned block from the map.
    /// Tries a few random probes first, then falls back to full O(n) scan.
    pub(crate) fn random_unassigned_node(&self, rng: &mut impl Rng) -> Option<usize> {
        self.random_node_from_part(0, rng)
    }

    /// Select a random unassigned block from the map that is on a district boundary.
    pub(crate) fn random_unassigned_boundary_node(&self, rng: &mut impl Rng) -> Option<usize> {
        let set = self.frontiers.get(0);
        if set.is_empty() { None } else { Some(set[rng.random_range(0..set.len())]) }
    }

    /// Select a random neighbor of a given block.
    pub(crate) fn random_edge(&self, node: usize, rng: &mut impl Rng) -> Option<usize> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        Some(self.graph().edge(node, rng.random_range(0..self.graph().degree(node)) as usize).unwrap())
    }

    /// Select a random neighboring district of a given block.
    pub(crate) fn random_neighboring_part(&self, node: usize, rng: &mut impl Rng) -> Option<u32> {
        assert!(node < self.graph().node_count(), "node {} out of range", node);
        if self.graph().degree(node) == 0 { return None }
        self.graph().edges(node)
            .map(|v| self.assignment(v))
            .filter(|&p| p != self.assignment(node))
            .choose(rng)
    }

    /// Select a random district, weighted by frontier size.
    pub(crate) fn random_part_weighted_by_frontier<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<u32> {
        let weights = self.frontiers.iter()
            .map(|set| set.len().saturating_sub(1))
            .collect::<Vec<_>>();
        let dist = WeightedIndex::new(&weights).ok()?; // None if all weights are zero
        Some(dist.sample(rng) as u32)
    }

    /// Randomly assign all nodes to contiguous districts.
    pub(crate) fn randomize(&mut self) {
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
