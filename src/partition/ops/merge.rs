use crate::partition::Partition;

impl Partition {
    /// Merge two parts into one, updating caches.
    /// Returns the index of the eliminated part (if merge is successful).
    /// `check` toggles whether to check contiguity constraints.
    pub(crate) fn merge_parts(&mut self, target: u32, source: u32, check: bool) -> Option<u32> {
        assert!(target < self.num_parts() && source < self.num_parts() && target != source,
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        // Choose `target` as the part to keep, `source` as the part to eliminate.
        if self.parts.get(target as usize).len() < self.parts.get(source as usize).len() {
            return self.merge_parts(source, target, check)
        }

        if !self.part_borders_part(target, source) { return None } // parts must be adjacent

        // Update assignments.
        for u in 0..self.graph().node_count() {
            if self.assignment(u) == source {
                self.parts.move_to(u, target as usize);
            }
        }

        // Update boundary and frontier sets.
        for u in 0..self.graph().node_count() {
            if self.assignment(u) != target { continue }

            if self.graph().edges(u).any(|v| self.assignment(v) != target) {
                self.frontiers.insert(u, target as usize);
            } else {
                self.frontiers.remove(u);
            }
        }

        self.update_on_merge_parts(target, source);

        Some(source)
    }
}
