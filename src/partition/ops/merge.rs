use crate::partition::Partition;

impl Partition {
    /// Merge two parts into one, updating caches.
    /// Returns the index of the eliminated part (if merge is successful).
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn merge_parts(&mut self, a: u32, b: u32, check: bool) -> Option<u32> {
        assert!(a < self.num_parts() && b < self.num_parts() && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        // Choose `a` as the part to keep, `b` as the part to eliminate.
        if self.parts.get(a as usize).len() < self.parts.get(b as usize).len() { return self.merge_parts(b, a, check) }

        if !self.part_borders_part(a, b) { return None } // parts must be adjacent

        // Update assignments.
        for u in 0..self.graph().node_count() {
            if self.assignment(u) == b {
                self.parts.move_to(u, a as usize);
            }
        }

        // Update boundary and frontier sets.
        for u in 0..self.graph().node_count() {
            if self.assignment(u) != a { continue }

            if self.graph().edges(u).any(|v| self.assignment(v) != a) {
                self.frontiers.insert(u, a as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        // update part_weights
        self.part_weights.add_row(a as usize, b as usize);
        self.part_weights.clear_row(b as usize);

        Some(b)
    }
}