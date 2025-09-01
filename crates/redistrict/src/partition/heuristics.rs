use crate::partition::WeightedGraphPartition;

impl WeightedGraphPartition {
    /// Select a random block from the map.
    pub fn random_node(&self) -> usize {
        use rand::Rng;
        rand::rng().random_range(0..self.graph.len())
    }

    /// Select a random unassigned block from the map.
    fn random_unassigned_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.assignments.iter().enumerate()
            .filter_map(|(i, &part)| (part == 0).then_some(i))
            .choose(&mut rand::rng())
    }

    /// Select a random block from the map that is on a district boundary.
    fn random_boundary_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.boundary.iter().enumerate()
            .filter_map(|(i, &flag)| flag.then_some(i))
            .choose(&mut rand::rng())
    }

    /// Select a random unassigned block from the map that is on a district boundary.
    fn random_unassigned_boundary_node(&self) -> Option<usize> {
        use rand::seq::IteratorRandom;
        self.assignments.iter().zip(self.boundary.iter()).enumerate()
            .filter_map(|(i, (&part, &flag))| (flag && part == 0).then_some(i))
            .choose(&mut rand::rng())
    }

    /// Equalize all districts by given series, within a given tolerance.
    pub fn equalize(&mut self, series: &str, tol: u32) { todo!() }

    /// Randomly assign all nodes to contiguous districts, updating caches.
    pub fn randomize(&mut self) {
        // 1) Seed districts with random starting blocks
        // 2) Expand districts until all blocks are assigned
        // 3) Equalize populations in each district
        todo!()
    }
}
