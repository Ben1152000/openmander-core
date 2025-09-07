use ndarray::{Array, Array1};

/// FrontierSet maintains "boundary nodes per part".
/// Invariant: node i is in lists[p]  <=>  assignments[i] == p && boundary[i] == true.
#[derive(Debug, Clone)]
pub struct FrontierSet {
    lists: Vec<Vec<usize>>, // lists[p] = frontier nodes for part p
    index: Array1<Option<(u32, usize)>>, // index[i] = Some((part, pos)) if i is in lists[part] at pos
}

impl FrontierSet {
    /// Create an empty frontier structure.
    pub fn new(num_parts: usize, num_nodes: usize) -> Self {
        Self {
            lists: vec![Vec::with_capacity(num_nodes.isqrt()); num_parts],
            index: Array::from_elem(num_nodes, None),
        }
    }

    /// Remove all nodes from all frontiers (O(total_size)).
    pub fn clear(&mut self) {
        for set in &mut self.lists { set.clear(); }
        self.index.fill(None);
    }

    /// Rebuild entire structure from assignments & boundary in one pass.
    pub fn rebuild(&mut self, assignments: &[u32], boundary: &[bool]) {
        debug_assert_eq!(self.index.len(), assignments.len());
        debug_assert_eq!(boundary.len(), assignments.len());
        self.clear();

        for (u, (&part, &flag)) in assignments.iter().zip(boundary.iter()).enumerate() {
            if flag {
                let set = &mut self.lists[part as usize];
                self.index[u] = Some((part, set.len()));
                set.push(u);
            }
        }
    }

    /// Refresh membership for node i given *current* part and boundary flag.
    /// Call this after you have recomputed boundary[i] (and assignments[i] if it changed).
    pub fn refresh(&mut self, u: usize, part: u32, on_boundary: bool) {
        match (self.index[u], on_boundary) {
            (Some((prev, _)), true) if prev == part => { /* already correct */ }
            (Some(_), true)  => { self.remove(u); self.insert_unchecked(u, part); }
            (Some(_), false) => self.remove(u),
            (None, true)     => self.insert_unchecked(u, part),
            (None, false)    => { /* nothing */ }
        }
    }

    /// Check if node i is currently in any frontier.
    #[inline] pub fn contains(&self, u: usize) -> bool { self.index[u].is_some() }

    /// Read-only view of frontier nodes for part p.
    #[inline] pub fn get(&self, part: u32) -> &[usize] { &self.lists[part as usize] }

    /// Iterator over all nodes currently present (across all parts).
    #[inline]
    pub fn items_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.lists.iter().flat_map(|v| v.iter().copied())
    }

    // ---------- internal O(1) ops ----------

    #[inline]
    fn insert_unchecked(&mut self, u: usize, part: u32) {
        debug_assert!((part as usize) < self.lists.len());
        debug_assert!(self.index[u].is_none());
        let set = &mut self.lists[part as usize];
        self.index[u] = Some((part, set.len()));
        set.push(u);
    }

    #[inline]
    fn remove(&mut self, u: usize) {
        if let Some((part, pos)) = self.index[u] {
            let set = &mut self.lists[part as usize];
            let last = set.pop().unwrap();
            if pos < set.len() {
                set[pos] = last;
                self.index[last] = Some((part, pos));
            }
            self.index[u] = None;
        }
    }
}
