/// MultiSet maintains "elements per set" with O(1) insert/remove/contains.
#[derive(Debug, Clone)]
pub(super) struct MultiSet {
    sets: Vec<Vec<usize>>,
    index: Vec<Option<(usize, usize)>>, // index[i] = Some((part, pos)) if i is in sets[part] @ pos
}

impl MultiSet {
    /// Create an empty multimap with `num_sets` sets and a universe of `num_elems` elements.
    pub(super) fn new(num_sets: usize, num_elems: usize) -> Self {
        let capacity = (num_elems / num_sets.max(1)).isqrt().saturating_add(1);
        Self {
            sets: (0..num_sets).map(|_| Vec::with_capacity(capacity)).collect(),
            index: vec![None; num_elems],
        }
    }

    /// Number of sets.
    #[inline] pub(super) fn num_sets(&self) -> usize { self.sets.len() }

    /// Universe size (number of elements addressable by index).
    #[inline] pub(super) fn num_elems(&self) -> usize { self.index.len() }

    /// Remove all elements from all sets (O(total_size)).
    pub(super) fn clear(&mut self) {
        for set in &mut self.sets { set.clear(); }
        self.index.fill(None);
    }

    /// Rebuild from an iterator of (elem, set) pairs. Elements not mentioned end up in no set.
    /// Panics in debug if elem/set are out of range or an element is listed more than once.
    pub(super) fn rebuild_from<I>(&mut self, iter: I) where I: IntoIterator<Item = (usize, usize)> {
        self.clear();

        for (elem, set) in iter {
            debug_assert!(elem < self.index.len(), "element out of range");
            debug_assert!(set < self.sets.len(), "set out of range");
            debug_assert!(self.index[elem].is_none(), "element listed multiple times in rebuild");

            let vec = &mut self.sets[set];
            self.index[elem] = Some((set, vec.len()));
            vec.push(elem);
        }
    }

    /// Return the set that `elem` is currently in, or `None` if absent.
    #[inline]
    pub(super) fn find(&self, elem: usize) -> Option<usize> {
        debug_assert!(elem < self.index.len(), "element out of range");
        self.index[elem].map(|(set, _)| set)
    }

    /// Returns true if element is in any set.
    #[inline] pub(super) fn contains(&self, elem: usize) -> bool { self.find(elem).is_some() }

    /// Read-only view of elements in set.
    #[inline]
    pub(super) fn get(&self, set: usize) -> &[usize] {
        debug_assert!(set < self.sets.len(), "bucket out of range");
        &self.sets[set]
    }

    /// Iterator over each set as a slice.
    #[inline]
    pub(super) fn iter(&self) -> impl Iterator<Item = &[usize]> + '_ {
        self.sets.iter().map(|v| v.as_slice())
    }

    /// Iterator over all elements present (across all sets).
    #[inline]
    pub(super) fn iter_all(&self) -> impl Iterator<Item = usize> + '_ {
        self.sets.iter().flat_map(|v| v.iter().copied())
    }

    /// Insert single element into set. If it's already in a set, it is moved.
    pub(super) fn insert(&mut self, elem: usize, set: usize) {
        debug_assert!(elem < self.index.len(), "element out of range");
        debug_assert!(set < self.sets.len(), "set out of range");
        match self.index[elem] {
            Some((current, _)) if current == set => { /* already correct */ }
            Some(_) => { self.remove(elem); self.insert_unchecked(elem, set);},
            None => self.insert_unchecked(elem, set),
        }
    }

    /// Remove element from whichever set it is in (no-op if absent).
    pub(super) fn remove(&mut self, elem: usize) {
        if let Some((set, pos)) = self.index[elem] {
            let vec = &mut self.sets[set];
            let last = vec.pop().unwrap();
            if pos < vec.len() {
                vec[pos] = last;
                self.index[last] = Some((set, pos));
            }
            self.index[elem] = None;
        }
    }

    #[inline]
    fn insert_unchecked(&mut self, elem: usize, set: usize) {
        let vec = &mut self.sets[set];
        self.index[elem] = Some((set, vec.len()));
        vec.push(elem);
    }
}

#[cfg(test)]
mod tests {
    use super::MultiSet;

    #[test]
    fn new_sizes() {
        let ms = MultiSet::new(3, 10);
        assert_eq!(ms.num_sets(), 3);
        assert_eq!(ms.num_elems(), 10);
        for set in 0..ms.num_sets() { assert!(ms.get(set).is_empty()) }
        for elem in 0..ms.num_elems() { assert!(!ms.contains(elem)) }
    }

    #[test]
    fn insert_contains_get() {
        let mut ms = MultiSet::new(3, 10);
        ms.insert(4, 1);
        ms.insert(7, 1);
        ms.insert(2, 2);

        assert!(ms.contains(4));
        assert!(ms.contains(7));
        assert!(ms.contains(2));
        assert!(!ms.contains(0));

        let set1 = ms.get(1);
        let set2 = ms.get(2);
        assert!(set1.contains(&4) && set1.contains(&7) && set1.len() == 2);
        assert_eq!(set2, &[2]);
    }

    #[test]
    fn insert_same_set() {
        let mut ms = MultiSet::new(2, 5);
        ms.insert(3, 1);
        ms.insert(3, 1); // no-op
        assert_eq!(ms.get(1).len(), 1);
        assert!(ms.contains(3));
    }

    #[test]
    fn move_between_sets() {
        let mut ms = MultiSet::new(3, 10);
        ms.insert(6, 0);
        ms.insert(6, 2); // move to set
        assert!(!ms.get(0).contains(&6));
        assert!(ms.get(2).contains(&6));
        assert!(ms.contains(6));
    }

    #[test]
    fn remove_absent_is_noop() {
        let mut ms = MultiSet::new(2, 5);
        ms.remove(3);
        assert!(!ms.contains(3));
        for set in 0..ms.num_sets() { assert!(ms.get(set).is_empty()) }
    }

    #[test]
    fn find_absent() {
        let ms = MultiSet::new(3, 10);
        assert_eq!(ms.find(0), None);
        assert!(!ms.contains(0));
    }

    #[test]
    fn find_present() {
        let mut ms = MultiSet::new(3, 10);
        ms.insert(4, 1);
        ms.insert(7, 2);

        assert_eq!(ms.find(4), Some(1));
        assert_eq!(ms.find(7), Some(2));
        assert!(ms.contains(4));
        assert!(ms.contains(7));
    }

    #[test]
    fn find_after_move() {
        let mut ms = MultiSet::new(3, 10);
        ms.insert(6, 0);
        assert_eq!(ms.find(6), Some(0));

        ms.insert(6, 2); // move
        assert_eq!(ms.find(6), Some(2));
        assert!(ms.get(2).contains(&6));
        assert!(!ms.get(0).contains(&6));
    }

    #[test]
    fn find_after_remove() {
        let mut ms = MultiSet::new(2, 6);
        ms.insert(3, 1);
        assert_eq!(ms.find(3), Some(1));

        ms.remove(3);
        assert_eq!(ms.find(3), None);
        assert!(!ms.contains(3));
    }

    #[test]
    fn remove_updates_index() {
        // Ensure remove updates the moved element's index.
        let mut ms = MultiSet::new(2, 4);
        ms.insert(0, 0);
        ms.insert(1, 0);
        ms.insert(2, 0);
        ms.insert(3, 1);
        ms.insert(1, 1);
        ms.remove(3);

        assert_eq!(ms.find(3), None);
        assert_eq!(ms.find(0), Some(0));
        assert_eq!(ms.find(1), Some(1));
        assert_eq!(ms.find(2), Some(0));
        assert_eq!(ms.get(0).len(), 2);
        assert_eq!(ms.get(1).len(), 1);
    }

    #[test]
    fn find_many_elements() {
        let mut ms = MultiSet::new(4, 12);
        // put evens into set 0, odds into set 3
        for elem in 0..12 {
            let set = if elem % 2 == 0 { 0 } else { 3 };
            ms.insert(elem, set);
        }
        for elem in 0..12 {
            let expected = if elem % 2 == 0 { 0 } else { 3 };
            assert_eq!(ms.find(elem), Some(expected));
        }
    }

    #[test]
    fn clear_resets_everything() {
        let mut ms = MultiSet::new(2, 6);
        ms.insert(0, 0);
        ms.insert(1, 0);
        ms.insert(2, 1);
        ms.clear();

        for set in 0..ms.num_sets() { assert!(ms.get(set).is_empty()) }
        for elem in 0..ms.num_elems() { assert!(!ms.contains(elem)) }
    }

    #[test]
    fn rebuild_from_basic() {
        let mut ms = MultiSet::new(3, 8);
        ms.rebuild_from([(0, 0), (2, 1), (5, 1), (7, 2)]);

        assert!(ms.get(0).contains(&0));
        let set1 = ms.get(1);
        assert!(set1.contains(&2) && set1.contains(&5) && set1.len() == 2);
        assert_eq!(ms.get(2), &[7]);
        for elem in [1, 3, 4, 6] { assert!(!ms.contains(elem)) }
    }

    #[test]
    fn iter_and_iter_all() {
        let mut ms = MultiSet::new(3, 6);
        ms.insert(0, 0);
        ms.insert(2, 1);
        ms.insert(4, 2);

        // iter(): returns slices per set
        let sets = ms.iter().map(|s| s.to_vec()).collect::<Vec<_>>();
        assert_eq!(sets.len(), 3);
        assert!(sets[0].contains(&0));
        assert!(sets[1].contains(&2));
        assert!(sets[2].contains(&4));

        // iter_all(): flatten
        let mut all = ms.iter_all().collect::<Vec<_>>();
        all.sort_unstable();
        assert_eq!(all, vec![0, 2, 4]);
    }

    #[test]
    fn insert_then_remove_then_reinsert() {
        let mut ms = MultiSet::new(2, 5);
        ms.insert(3, 0);
        ms.remove(3);
        assert!(!ms.contains(3));
        ms.insert(3, 1);
        assert!(ms.get(1).contains(&3));
    }
}
