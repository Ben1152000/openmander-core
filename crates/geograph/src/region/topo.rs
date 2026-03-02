use std::collections::{HashSet, VecDeque};

use crate::unit::UnitId;

use super::Region;

impl Region {
    /// Returns `true` if all units in `units` form a single connected component
    /// under Rook adjacency.
    pub fn is_contiguous(&self, units: impl IntoIterator<Item = UnitId>) -> bool {
        self.connected_components(units).len() <= 1
    }

    /// Partition `units` into maximal connected components under Rook
    /// adjacency.
    pub fn connected_components(
        &self,
        units: impl IntoIterator<Item = UnitId>,
    ) -> Vec<Vec<UnitId>> {
        let set: HashSet<UnitId> = units.into_iter().collect();
        if set.is_empty() { return Vec::new(); }

        let adj = self.adjacency();
        let mut visited: HashSet<UnitId> = HashSet::new();
        let mut components: Vec<Vec<UnitId>> = Vec::new();

        for &seed in &set {
            if visited.contains(&seed) { continue; }

            // BFS within the subset.
            let mut component = Vec::new();
            let mut queue = VecDeque::new();
            queue.push_back(seed);
            visited.insert(seed);

            while let Some(u) = queue.pop_front() {
                component.push(u);
                for &nb in adj.neighbors(u) {
                    if set.contains(&nb) && !visited.contains(&nb) {
                        visited.insert(nb);
                        queue.push_back(nb);
                    }
                }
            }

            components.push(component);
        }

        components
    }

    /// Returns `true` if the complement of `units` contains any component
    /// entirely surrounded by `units` (i.e. not adjacent to the exterior).
    pub fn has_holes(&self, units: impl IntoIterator<Item = UnitId>) -> bool {
        !self.enclaves(units).is_empty()
    }

    /// Returns each connected component of the complement of `units` that is
    /// entirely surrounded by `units` (not adjacent to the exterior).
    pub fn enclaves(
        &self,
        units: impl IntoIterator<Item = UnitId>,
    ) -> Vec<Vec<UnitId>> {
        let set: HashSet<UnitId> = units.into_iter().collect();
        // Complement = all real units NOT in `set`.
        let complement: HashSet<UnitId> = self
            .unit_ids()
            .filter(|u| !set.contains(u))
            .collect();

        let adj = self.adjacency();
        let mut visited: HashSet<UnitId> = HashSet::new();
        let mut enclaves: Vec<Vec<UnitId>> = Vec::new();

        for &seed in &complement {
            if visited.contains(&seed) { continue; }

            // BFS within the complement.
            let mut component = Vec::new();
            let mut touches_exterior = false;
            let mut queue = VecDeque::new();
            queue.push_back(seed);
            visited.insert(seed);

            while let Some(u) = queue.pop_front() {
                component.push(u);
                if self.is_exterior[u.0 as usize] {
                    touches_exterior = true;
                }
                for &nb in adj.neighbors(u) {
                    if complement.contains(&nb) && !visited.contains(&nb) {
                        visited.insert(nb);
                        queue.push_back(nb);
                    }
                }
            }

            if !touches_exterior {
                enclaves.push(component);
            }
        }

        enclaves
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::unit::UnitId;
    use crate::region::test_helpers::make_two_unit_region;

    // -----------------------------------------------------------------------
    // Helpers: 4-unit 2×2 grid
    //
    //  u2(0,1)--u3(1,1)
    //    |   |   |
    //  u0(0,0)--u1(1,0)
    //
    // We simulate this using the 2-unit region for simpler tests,
    // then extend with hand-crafted structures below.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // is_contiguous
    // -----------------------------------------------------------------------

    #[test]
    fn single_unit_is_contiguous() {
        let r = make_two_unit_region();
        assert!(r.is_contiguous([UnitId(0)]));
        assert!(r.is_contiguous([UnitId(1)]));
    }

    #[test]
    fn both_adjacent_units_are_contiguous() {
        let r = make_two_unit_region();
        assert!(r.is_contiguous([UnitId(0), UnitId(1)]));
    }

    #[test]
    fn empty_set_is_contiguous() {
        let r = make_two_unit_region();
        assert!(r.is_contiguous([]));
    }

    // -----------------------------------------------------------------------
    // connected_components
    // -----------------------------------------------------------------------

    #[test]
    fn empty_set_has_no_components() {
        let r = make_two_unit_region();
        assert!(r.connected_components([]).is_empty());
    }

    #[test]
    fn single_unit_has_one_component() {
        let r = make_two_unit_region();
        assert_eq!(r.connected_components([UnitId(0)]).len(), 1);
    }

    #[test]
    fn two_adjacent_units_form_one_component() {
        let r = make_two_unit_region();
        assert_eq!(
            r.connected_components([UnitId(0), UnitId(1)]).len(),
            1
        );
    }

    #[test]
    fn all_units_in_each_component_are_subset_members() {
        let r = make_two_unit_region();
        let comps = r.connected_components(r.unit_ids());
        let all: Vec<UnitId> = comps.into_iter().flatten().collect();
        assert!(all.contains(&UnitId(0)));
        assert!(all.contains(&UnitId(1)));
    }

    #[test]
    fn components_partition_the_input() {
        let r = make_two_unit_region();
        let input: Vec<UnitId> = r.unit_ids().collect();
        let comps = r.connected_components(input.iter().copied());
        let total: usize = comps.iter().map(Vec::len).sum();
        assert_eq!(total, input.len());
    }

    // -----------------------------------------------------------------------
    // has_holes / enclaves — two-unit region has no interior enclosed units
    // -----------------------------------------------------------------------

    #[test]
    fn two_unit_region_has_no_holes() {
        let r = make_two_unit_region();
        // Selecting only unit 0 leaves unit 1 in the complement.
        // Unit 1 touches the exterior → no enclave.
        assert!(!r.has_holes([UnitId(0)]));
    }

    #[test]
    fn empty_units_has_no_enclaves() {
        let r = make_two_unit_region();
        assert!(r.enclaves([]).is_empty());
    }

    #[test]
    fn all_units_selected_complement_is_empty() {
        let r = make_two_unit_region();
        // Complement is empty → no enclaves.
        assert!(r.enclaves(r.unit_ids()).is_empty());
    }

    #[test]
    fn enclaves_of_exterior_touching_complement_is_empty() {
        let r = make_two_unit_region();
        // Unit 0 selected; unit 1 is the complement and is exterior → not an enclave.
        assert!(r.enclaves([UnitId(0)]).is_empty());
    }
}
