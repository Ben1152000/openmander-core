use std::sync::Arc;

/// A dynamic per-part convex hull cache.
/// - coords[u] = (x, y) for node u
/// - hulls[p] = convex hull of the nodes currently in part p (CCW, unique)
#[derive(Debug, Clone)]
pub struct HullSet {
    coords: Arc<Vec<(f64, f64)>>,     // len = num_nodes
    members: Vec<Vec<usize>>,         // members[p] = nodes in part p
    index: Vec<Option<(u32, usize)>>, // index[u] = Some((part, pos_in_members[p]))
    hulls: Vec<Vec<usize>>,           // hulls[p] = node indices on hull of part p (CCW)
}

impl HullSet {
    /// Create an empty HullSet. Call `rebuild(assignments)` next to populate.
    pub fn new(num_parts: usize, coords: Arc<Vec<(f64, f64)>>) -> Self {
        Self {
            members: vec![Vec::new(); num_parts],
            index: vec![None; coords.len()],
            hulls: vec![Vec::new(); num_parts],
            coords,
        }
    }

    /// Rebuild everything from scratch in one pass.
    /// Useful after `GraphPartition::set_assignments` / `rebuild_caches`.
    pub fn rebuild(&mut self, assignments: &[u32]) {
        assert_eq!(assignments.len(), self.index.len(), "assignments/coords length mismatch");

        // Clear membership/index/hulls
        for v in &mut self.members { v.clear(); }
        self.index.fill(None);
        for h in &mut self.hulls { h.clear(); }

        // Fill members & index
        for (u, &p) in assignments.iter().enumerate() {
            let set = &mut self.members[p as usize];
            self.index[u] = Some((p, set.len()));
            set.push(u);
        }

        // Recompute all hulls (skip empty parts for speed).
        for p in 0..self.members.len() {
            if !self.members[p].is_empty() {
                let hull = self.compute_hull_from_members(p);
                self.hulls[p] = hull;
            }
        }
    }

    /// Return a read-only view of the hull for a part (list of node indices, CCW).
    #[inline]
    pub fn hull(&self, part: u32) -> &[usize] {
        &self.hulls[part as usize]
    }

    /// Number of nodes in a part (mirrors GraphPartition::part_sizes, but local).
    #[inline]
    pub fn part_size(&self, part: u32) -> usize {
        self.members[part as usize].len()
    }

    /// Notify after moving one node `u` from `prev` to `next`.
    /// (Use this from GraphPartition::move_node after you update assignments.)
    pub fn on_move(&mut self, u: usize, prev: u32, next: u32) {
        if prev == next { return; }

        self.remove_from_members(u, prev);
        self.insert_into_members(u, next);

        // Recompute hulls only for the affected parts.
        self.hulls[prev as usize] = self.compute_hull_from_members(prev as usize);
        self.hulls[next as usize] = self.compute_hull_from_members(next as usize);
    }

    /// Notify after moving a connected subgraph `nodes` from `prev` to `next`.
    /// (Use this from GraphPartition::move_subgraph after you update assignments.)
    pub fn on_move_subgraph(&mut self, nodes: &[usize], prev: u32, next: u32) {
        if nodes.is_empty() || prev == next { return; }

        // Bulk remove then bulk insert to keep index sane.
        for &u in nodes { self.remove_from_members(u, prev); }
        for &u in nodes { self.insert_into_members(u, next); }

        self.hulls[prev as usize] = self.compute_hull_from_members(prev as usize);
        self.hulls[next as usize] = self.compute_hull_from_members(next as usize);
    }

    /// Notify after merging parts: move every node from `drop_part` into `keep_part`.
    /// (Call this after you've updated assignments & sizes/weights.)
    pub fn on_merge(&mut self, keep_part: u32, drop_part: u32) {
        if keep_part == drop_part { return; }
        let k = keep_part as usize;
        let d = drop_part as usize;

        // Move all members of drop -> keep
        let moved = std::mem::take(&mut self.members[d]);
        for u in moved {
            // Update index to reflect the new part membership
            self.index[u] = None; // remove stale
            self.insert_into_members(u, keep_part);
        }

        // Recompute keep hull; dropped hull becomes empty.
        self.hulls[k] = self.compute_hull_from_members(k);
        self.hulls[d].clear();
    }

    /// For bulk fixes or sanity checks: recompute hull for a single part from current members.
    pub fn recompute_part(&mut self, part: u32) {
        self.hulls[part as usize] = self.compute_hull_from_members(part as usize);
    }

    /// Compute convex hull perimeter length for a part (0 if <2 vertices).
    pub fn hull_perimeter(&self, part: u32) -> f64 {
        let h = &self.hulls[part as usize];
        perimeter(&self.coords, h)
    }

    /// Compute convex hull area for a part (0 if <3 vertices).
    pub fn hull_area(&self, part: u32) -> f64 {
        let h = &self.hulls[part as usize];
        polygon_area(&self.coords, h).abs()
    }

    // ---------------- internal helpers ----------------

    #[inline]
    fn remove_from_members(&mut self, u: usize, prev: u32) {
        if let Some((p, pos)) = self.index[u] {
            debug_assert_eq!(p, prev);
            let set = &mut self.members[p as usize];
            let last = set.pop().unwrap();
            if pos < set.len() {
                set[pos] = last;
                self.index[last] = Some((p, pos));
            }
            self.index[u] = None;
        }
    }

    #[inline]
    fn insert_into_members(&mut self, u: usize, part: u32) {
        let set = &mut self.members[part as usize];
        self.index[u] = Some((part, set.len()));
        set.push(u);
    }

    /// Recompute a hull from current members[p] using Andrew’s monotone chain.
    fn compute_hull_from_members(&self, p: usize) -> Vec<usize> {
        let idxs = &self.members[p];
        match idxs.len() {
            0 => return Vec::new(),
            1 => return vec![idxs[0]],
            2 => {
                let (a, b) = (idxs[0], idxs[1]);
                if self.coords[a] == self.coords[b] { return vec![a]; }
                return vec![a, b];
            }
            _ => {}
        }

        // Sort by (x, y), but keep original node ids.
        let mut pts: Vec<(usize, f64, f64)> = idxs
            .iter()
            .map(|&u| {
                let (x, y) = self.coords[u];
                (u, x, y)
            })
            .collect();

        pts.sort_by(|a, b| {
            match a.1.partial_cmp(&b.1).unwrap() {
                std::cmp::Ordering::Equal => a.2.partial_cmp(&b.2).unwrap(),
                ord => ord,
            }
        });

        // Build lower hull
        let mut lower: Vec<(usize, f64, f64)> = Vec::with_capacity(pts.len());
        for p in &pts {
            while lower.len() >= 2
                && cross(lower[lower.len() - 2], lower[lower.len() - 1], *p) <= 0.0
            {
                lower.pop();
            }
            lower.push(*p);
        }

        // Build upper hull
        let mut upper: Vec<(usize, f64, f64)> = Vec::with_capacity(pts.len());
        for p in pts.iter().rev() {
            while upper.len() >= 2
                && cross(upper[upper.len() - 2], upper[upper.len() - 1], *p) <= 0.0
            {
                upper.pop();
            }
            upper.push(*p);
        }

        // Concatenate lower and upper to form full hull; remove the last element of each list (duplicate)
        if !lower.is_empty() { lower.pop(); }
        if !upper.is_empty() { upper.pop(); }

        let mut hull: Vec<usize> = lower.into_iter().chain(upper.into_iter()).map(|q| q.0).collect();

        // Deduplicate in case of degeneracy (all collinear or repeated points)
        hull.dedup();
        hull
    }
}

/// Cross product (OA x OB). Positive for left turn.
#[inline]
fn cross(o: (usize, f64, f64), a: (usize, f64, f64), b: (usize, f64, f64)) -> f64 {
    (a.1 - o.1) * (b.2 - o.2) - (a.2 - o.2) * (b.1 - o.1)
}

#[inline]
fn perimeter(coords: &[(f64, f64)], hull: &[usize]) -> f64 {
    match hull.len() {
        0 | 1 => 0.0,
        2 => {
            let (x1, y1) = coords[hull[0]];
            let (x2, y2) = coords[hull[1]];
            ((x2 - x1).hypot(y2 - y1)) * 2.0
        }
        _ => {
            let mut p = 0.0;
            for i in 0..hull.len() {
                let (x1, y1) = coords[hull[i]];
                let (x2, y2) = coords[hull[(i + 1) % hull.len()]];
                p += (x2 - x1).hypot(y2 - y1);
            }
            p
        }
    }
}

#[inline]
fn polygon_area(coords: &[(f64, f64)], hull: &[usize]) -> f64 {
    if hull.len() < 3 { return 0.0; }
    let mut a = 0.0;
    for i in 0..hull.len() {
        let (x1, y1) = coords[hull[i]];
        let (x2, y2) = coords[hull[(i + 1) % hull.len()]];
        a += x1 * y2 - x2 * y1;
    }
    0.5 * a
}
