use std::{collections::HashSet};
use crate::{partition::WeightedGraphPartition};

impl WeightedGraphPartition {

    /// Compute the change in boundary length if `node` were moved to `part`.
    #[inline]
    fn get_boundary_delta(&self, node: usize, part: u32) -> f64 {
        self.graph.edges_with_weights(node)
            .filter_map(|(v, w)| (self.assignments[v] == self.assignments[node]).then_some(w))
            .sum::<f64>()
        - self.graph.edges_with_weights(node)
            .filter_map(|(v, w)| (self.assignments[v] == part).then_some(w))
            .sum::<f64>()
    }

    /// Distribute surplus population from part `src` to part `dest`.
    fn distribute_surplus(&mut self, series: &str, src: u32, dest: u32) {
        let delta = self.part_weights.get_as_f64(series, src as usize).unwrap()
            - self.part_weights.get_as_f64(series, dest as usize).unwrap();

        // Swap to ensure src is the surplus part.
        if delta < 0.0 { return self.distribute_surplus(series, dest, src) }

        // Transfer nodes from part to neighbor in order of longest shared perimeter.
        let mut candidates: Vec<(usize, f64, f64)> = Vec::new();
        let mut in_candidates = vec![false; self.graph.len()];

        for &u in self.frontiers.get(src).iter()
            .filter(|&&u| self.graph.edges(u).any(|v| self.assignments[v] == dest)) {
            candidates.push((
                u,
                self.get_boundary_delta(u, dest),
                self.graph.node_weights.get_as_f64(series, u).unwrap()
            ));
            in_candidates[u] = true;
        }

        let mut remaining = 0.5 * delta;
        while !candidates.is_empty() && remaining > 0.0 {
            // Sort by descencing perimeter delta.
            candidates.sort_by(|a, b| { b.1.partial_cmp(&a.1).unwrap() });

            // Move best candidate to neighboring part.
            let (node, _, value) = candidates.pop().unwrap();
            in_candidates[node] = false;

            if !self.check_node_contiguity(node, dest) { continue }

            // Make sure moving the node wouldn't exhaust the delta.
            remaining -= value;
            if remaining < 0.0 { return }

            self.move_node_without_rebuild(node, dest);

            // Add new candidates that are now on the frontier.
            for u in self.graph.edges(node).filter(|&v| self.assignments[v] == src) {
                if in_candidates[u] {
                    // Recompute perimeter for existing candidate.
                    if let Some(candidate) = candidates.iter_mut().find(|(x,_,_)| *x == u) {
                        candidate.1 = self.get_boundary_delta(u, dest);
                    }
                } else {
                    candidates.push((
                        u,
                        self.get_boundary_delta(u, dest),
                        self.graph.node_weights.get_as_f64(series, u).unwrap()
                    ));
                    in_candidates[u] = true;
                }
            }
        }

        self.rebuild_caches();
    }

    /// Equalize populations to within tolerance (fraction) of ideal using greedy boundary moves.
    /// `series` should name a population column in node_weights.series.
    pub fn equalize(&mut self, series: &str, tolerance: f64) {
        assert_ne!(self.num_parts, 1, "cannot equalize with only one part");
        assert!(self.graph.node_weights.series.contains_key(series),
            "series '{}' not found in node weights", series);

        // Compute target population and tolerance band (ignoring unassigned part 0).
        let total: f64 = (1..self.num_parts)
            .map(|part| self.part_weights.get_as_f64(series, part as usize).unwrap())
            .sum();
        let target = total / ((self.num_parts - 1) as f64);
        let allowed = target * tolerance;

        // Iterate until all parts are within tolerance, or we give up.
        let limit = self.num_parts * 10;
        for _ in 0..limit {
            // Find the worst-offending part (max absolute deviation).
            let (part, deviation) = (1..self.num_parts)
                .map(|p| (p, (self.part_weights.get_as_f64(series, p as usize).unwrap() - target).abs()))
                .max_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
                .unwrap();

            if deviation <= allowed { return } // all parts within tolerance

            // Find neighboring part with largest opposite deviation.
            let neighbors = self.frontiers.get(part).iter()
                .flat_map(|&u| self.graph.edges(u))
                .map(|u| self.assignments[u])
                .filter(|&p| p != 0 && p != part)
                .collect::<HashSet<_>>();

            assert!(!neighbors.is_empty(), "part {} has no neighbors", part);

            let part_total = self.part_weights.get_as_f64(series, part as usize).unwrap();

            println!("Part {} has population {:.0} (target {:.0} ±{:.0})", part, part_total, target, allowed);

            if part_total >= target {
                let (neighbor, _delta) = neighbors.iter()
                    .map(|&p| (p, (part_total - self.part_weights.get_as_f64(series, p as usize).unwrap())))
                    .max_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
                    .unwrap();

                println!("  transferring to part {} with population {:.0} (delta {:.0})", neighbor, self.part_weights.get_as_f64(series, neighbor as usize).unwrap(), _delta);

                self.distribute_surplus(series, part, neighbor);
            } else {
                let (neighbor, _delta) = neighbors.iter()
                    .map(|&p| (p, (self.part_weights.get_as_f64(series, p as usize).unwrap() - part_total)))
                    .max_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
                    .unwrap();

                println!("  transferring from part {} with population {:.0} (delta {:.0})", neighbor, self.part_weights.get_as_f64(series, neighbor as usize).unwrap(), _delta);

                self.distribute_surplus(series, neighbor, part);
            }
        }
    }
}
