/// PartitionSet maintains a total assignment of elements to sets, with O(1) move/contains
#[derive(Debug, Clone)]
pub(crate) struct PartitionSet {
    sets: Vec<Vec<usize>>,  // sets[s] = elements currently in set s
    index: Vec<usize>,      // index[e] = s when e is in sets[s]
    position: Vec<usize>    // position[e] = i when sets[s][i] is e
}

impl PartitionSet {
    /// Create a MultiSet with `num_sets` sets and `num_elems` elements,
    /// initially assigning all elements to set 0.
    pub(crate) fn new(num_sets: usize, num_elems: usize) -> Self {
        assert!(num_sets > 0, "must have at least one set");
        let capacity = (num_elems / num_sets.max(1)).isqrt().saturating_add(1);
        let mut sets = (0..num_sets)
            .map(|_| Vec::with_capacity(capacity))
            .collect::<Vec<_>>();
        sets[0] = (0..num_elems).collect();

        let index = vec![0; num_elems];
        let position = (0..num_elems).collect();

        Self { sets, index, position }
    }

    /// Number of sets.
    #[inline] pub fn num_sets(&self) -> usize { self.sets.len() }

    /// Universe size (number of elements addressable by index).
    #[inline] pub fn num_elems(&self) -> usize { self.index.len() }

    /// Return the set that `elem` is currently in.
    #[inline]
    pub fn find(&self, elem: usize) -> usize {
        debug_assert!(elem < self.index.len(), "element out of range");
        self.index[elem]
    }

    /// Returns a reference to the elements currently in `set`.
    #[inline]
    pub fn get(&self, set: usize) -> &[usize] {
        debug_assert!(set < self.sets.len(), "set out of range");
        &self.sets[set]
    }

    /// Get a complete vector of assignments for each element.
    #[inline] pub fn assignments(&self) -> &[usize] { &self.index }

    /// Iterator over each set as a slice.
    #[inline]
    pub fn iter_sets(&self) -> impl Iterator<Item = &[usize]> + '_ {
        self.sets.iter().map(|v| v.as_slice())
    }

    /// Iterator over all elements in all sets.
    #[inline]
    pub fn iter_all(&self) -> impl Iterator<Item = usize> + '_ {
        self.sets.iter().flat_map(|v| v.iter().copied())
    }

    /// Remove all elements from all sets, placing them in set 0.
    pub(crate) fn clear(&mut self) {
        self.sets.iter_mut().for_each(|v| v.clear());
        self.sets[0] = (0..self.num_elems()).collect();
        self.index = vec![0; self.num_elems()];
        self.position = (0..self.num_elems()).collect();
    }

    /// Rebuild partition from a complete slice of assignments.
    pub(crate) fn rebuild(&mut self, assignments: &[usize]) {
        assert!(assignments.len() == self.num_elems(), "assignments length mismatch");

        self.sets.iter_mut().for_each(|v| v.clear());
        for (elem, &set) in assignments.iter().enumerate() {
            assert!(set < self.num_sets(), "set out of range");
            self.index[elem] = set;
            self.position[elem] = self.sets[set].len();
            self.sets[set].push(elem);
        }
    }

    /// Move `elem` to `set`. Panics in debug if out of range.
    pub(crate) fn move_to(&mut self, elem: usize, set: usize) {
        debug_assert!(elem < self.index.len(), "element out of range");
        debug_assert!(set < self.sets.len(), "set out of range");

        let (prev, pos) = (self.index[elem], self.position[elem]);
        if prev == set { return }

        // Remove from previous set by swapping with last element.
        let last_elem = self.sets[prev].pop().unwrap();
        if last_elem != elem {
            self.sets[prev][pos] = last_elem;
            self.position[last_elem] = pos;
        }

        // Add to new set.
        self.index[elem] = set;
        self.position[elem] = self.sets[set].len();
        self.sets[set].push(elem);
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::PartitionSet;

    #[test]
    fn new_fills_first_set() {
        let ps = PartitionSet::new(3, 5);
        assert_eq!(ps.num_sets(), 3);
        assert_eq!(ps.num_elems(), 5);

        // All in set 0
        assert_eq!(ps.get(0), &[0, 1, 2, 3, 4]);
        assert!(ps.get(1).is_empty());
        assert!(ps.get(2).is_empty());

        // find() agrees
        for elem in 0..5 {
            assert_eq!(ps.find(elem), 0);
        }

        // iterators
        let sets = ps.iter_sets().map(|s| s.to_vec()).collect::<Vec<_>>();
        assert_eq!(sets.len(), 3);
        let mut all = ps.iter_all().collect::<Vec<_>>();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    #[should_panic] // new() indexes sets[0]; with 0 sets this should panic
    fn new_panics_on_zero_sets() {
        PartitionSet::new(0, 4);
    }

    #[test]
    fn clear_resets_to_first_set() {
        let mut ps = PartitionSet::new(3, 6);
        ps.move_to(1, 1);
        ps.move_to(4, 2);
        ps.clear();

        assert_eq!(ps.get(0), &[0, 1, 2, 3, 4, 5]);
        assert!(ps.get(1).is_empty());
        assert!(ps.get(2).is_empty());
        for elem in 0..ps.num_elems() {
            assert_eq!(ps.find(elem), 0);
        }
    }

    #[test]
    fn rebuild_basic_assignment() {
        let mut ps = PartitionSet::new(3, 6);
        let assign = [0, 1, 2, 0, 2, 1];
        ps.rebuild(&assign);

        assert_eq!(ps.get(0), &[0, 3]);
        assert_eq!(ps.get(1), &[1, 5]);
        assert_eq!(ps.get(2), &[2, 4]);

        for (elem, &set) in assign.iter().enumerate() {
            assert_eq!(ps.find(elem), set);
        }
    }

    #[test]
    #[should_panic(expected = "assignments length mismatch")]
    fn rebuild_panics_on_len_mismatch() {
        let mut ps = PartitionSet::new(2, 4);
        ps.rebuild(&[0, 1, 0]);
    }

    #[test]
    #[should_panic(expected = "set out of range")]
    fn rebuild_panics_on_set_oob() {
        let mut ps = PartitionSet::new(2, 3);
        ps.rebuild(&[0, 1, 2]);
    }

    #[test]
    fn move_to_idempotent_when_same_set() {
        let mut ps = PartitionSet::new(2, 3);
        ps.move_to(2, 0);
        assert_eq!(ps.find(2), 0);
        assert!(ps.get(0).contains(&2));
        assert!(ps.get(1).is_empty());
    }

    #[test]
    fn move_to_between_sets_simple() {
        let mut ps = PartitionSet::new(3, 4);
        ps.move_to(3, 2);
        assert_eq!(ps.find(3), 2);
        assert!(!ps.get(0).contains(&3));
        assert!(ps.get(2).contains(&3));
    }

    #[test]
    fn move_to_updates_swapped_element_index() {
        let mut ps = PartitionSet::new(2, 5);
        ps.move_to(4, 0);
        ps.move_to(3, 0);
        ps.rebuild(&[0, 0, 0, 1, 0]);
        ps.move_to(1, 1);

        assert_eq!(ps.find(1), 1);
        assert!(!ps.get(0).contains(&1));
        assert!(ps.get(0).contains(&4));
        assert!(ps.get(0).contains(&0));
        assert!(ps.get(0).contains(&2));
        assert_eq!(ps.find(4), 0);
        assert!(ps.get(1).contains(&3));
        assert!(ps.get(1).contains(&1));
    }

    #[test]
    fn multiple_moves_keep_invariant() {
        let mut ps = PartitionSet::new(3, 6);
        ps.move_to(0, 1);
        ps.move_to(1, 2);
        ps.move_to(2, 1);
        ps.move_to(3, 2);
        ps.move_to(4, 1);
        ps.move_to(5, 2);

        // Each element is in exactly one set and find() matches
        for elem in 0..6 {
            let set = ps.find(elem);
            assert!(set <= 2);
            assert!(ps.get(set).contains(&elem));
        }

        // Sets have all elements, no duplicates across sets
        let mut all: Vec<_> = ps.iter_all().collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5]);
    }
}
