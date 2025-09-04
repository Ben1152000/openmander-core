use std::collections::{HashSet};

use rand::Rng;

use crate::{partition::WeightedGraphPartition};

impl WeightedGraphPartition {
    /// Equalize total weights between two parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    pub fn equalize_parts(&mut self, series: &str, a: u32, b: u32) {
        // Validate parts and adjacency.
        assert!(a < self.num_parts && b < self.num_parts && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts);

        let mut rng = rand::rng();

        // Define src as the part with surplus weight.
        let a_total = self.part_weights.get_as_f64(series, a as usize).unwrap();
        let b_total = self.part_weights.get_as_f64(series, b as usize).unwrap();
        let (src, dest, src_total, dest_total) =
            if a_total >= b_total { (a, b, a_total, b_total) }
            else { (b, a, b_total, a_total) };

        let delta = src_total - dest_total;
        let mut remaining = delta / 2.0;

        println!("Moving from part {} (pop: {:.0}) to part {} (pop: {:.0})", src, src_total, dest, dest_total);

        while remaining > 0.0 {
            // Pick a random candidate on the boundary of src.
            let candidates = self.frontiers.get(src);
            let node = candidates[rng.random_range(0..candidates.len())];

            // Skip if not adjacent.
            if !(self.part_is_empty(dest) || self.node_borders_part(node, dest)) { continue }

            if self.check_node_contiguity(node, dest) {
                self.move_node(node, dest, false);
                remaining -= self.graph.node_weights.get_as_f64(series, node).unwrap();
            } else {
                // Compute articulation bundle and move node with it (if necessary).
                let mut subgraph = self.cut_subgraph_within_part(node);
                subgraph.push(node);

                self.move_subgraph(&subgraph, dest, false);
                remaining -= subgraph.iter()
                    .map(|&u| self.graph.node_weights.get_as_f64(series, u).unwrap())
                    .sum::<f64>();
            }
        }
    }

    /// Equalize total weights across all parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    /// `tolerance` is the allowed fraction deviation from ideal (e.g. 0.01 = ±1%).
    /// `iter` is the maximum number of equalization passes to attempt.
    pub fn equalize(&mut self, series: &str, tolerance: f64, iter: usize) {
        assert_ne!(self.num_parts, 1, "cannot equalize with only one part");
        assert!(self.graph.node_weights.series.contains_key(series),
            "series '{}' not found in node weights", series);

        let mut rng = rand::rng();

        // Compute target population and tolerance band (ignoring unassigned part 0).
        let total = (1..self.num_parts)
            .map(|part| self.part_weights.get_as_f64(series, part as usize).unwrap())
            .sum::<f64>();
        let target = total / ((self.num_parts - 1) as f64);
        let allowed = target * tolerance;

        println!("Target population per part: {:.0} ±{:.0}", target, allowed);

        // Iterate until all parts are within tolerance, or we give up.
        for _ in 0..iter {
            // Find the worst-offending part (max absolute deviation).
            let (part, deviation) = (1..self.num_parts)
                .map(|p| (p, (self.part_weights.get_as_f64(series, p as usize).unwrap() - target).abs()))
                .max_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
                .unwrap();

            if deviation <= allowed { break } // all parts within tolerance

            // Pick random neighboring part and equalize.
            let frontier = self.frontiers.get(part);
            assert!(!frontier.is_empty(), "part {} has no neighboring parts", part);

            let mut neighbors = HashSet::new();
            for _ in 0..8 {
                let node = frontier[rng.random_range(0..frontier.len())];
                neighbors.extend(self.graph.edges(node)
                    .map(|u| self.assignments[u])
                    .filter(|&p| p != 0 && p != part));
            }
            if neighbors.len() == 0 { continue }

            // let part_total = self.part_weights.get_as_f64(series, part as usize).unwrap();
            // let other = neighbors.iter()
            //     .map(|&p| (p, (part_total - self.part_weights.get_as_f64(series, p as usize).unwrap()).abs()))
            //     .max_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
            //     .map(|(p, _)| p).unwrap();

            let neighbors = neighbors.into_iter().collect::<Vec<_>>();
            let other = neighbors[rng.random_range(0..neighbors.len())];

            self.equalize_parts(series, part, other);
        }
    }
}
