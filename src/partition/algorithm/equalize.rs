use std::collections::HashSet;

use rand::{distr::{weighted::WeightedIndex, Distribution}, Rng};

use crate::partition::Partition;

impl Partition {
    /// Find the part with the minimum total weight.
    /// Returns (part, part_weight).
    fn part_with_min_weight(&self, series: &str) -> (u32, f64) {
        assert!(self.num_parts() > 1, "cannot find min part with only one part");
        assert!(self.graph().node_weights().contains(series),
            "series '{}' not found in node weights", series);

        (1..self.num_parts())
            .map(|p| (p, self.part_weights.get_as_f64(series, p as usize).unwrap()))
            .min_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
            .unwrap()
    }

    /// Attempt to find neighboring parts to a given part by sampling its frontier.
    /// `samples` is the number of random frontier nodes to sample.
    /// Use this function when computing the full neighbor set is too expensive.
    fn sample_neighboring_parts(&self, part: u32, samples: usize, rng: &mut impl Rng) -> Vec<u32> {
        assert!(part < self.num_parts(), "part {} out of range", part);

        let frontier = self.frontiers.get(part as usize);
        if frontier.is_empty() { return vec![] }

        let mut neighbors = HashSet::new();
        for _ in 0..samples {
            let node = frontier[rng.random_range(0..frontier.len())];
            neighbors.extend(self.graph().edges(node)
                .map(|u| self.assignments[u])
                .filter(|&p| p != 0 && p != part));
        }

        neighbors.into_iter().collect()
    }

    /// Equalize total weights between two parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    pub(crate) fn equalize_parts(&mut self, series: &str, a: u32, b: u32, tolerance: f64) {
        // Validate parts and adjacency.
        assert!(a < self.num_parts() && b < self.num_parts() && a != b,
            "a and b must be distinct parts in range [0, {})", self.num_parts());

        let mut rng = rand::rng();

        // Define src as the part with surplus weight.
        let a_total = self.part_weights.get_as_f64(series, a as usize).unwrap();
        let b_total = self.part_weights.get_as_f64(series, b as usize).unwrap();
        let (src, dest, src_total, dest_total) =
            if a_total >= b_total { (a, b, a_total, b_total) }
            else { (b, a, b_total, a_total) };

        let delta = src_total - dest_total;
        let mut remaining = delta / 2.0;

        while remaining > 0.0 {
            // Pick a random candidate on the boundary of src.
            let candidates = self.frontiers.get(src as usize);
            let node = candidates[rng.random_range(0..candidates.len())];

            // Skip if not adjacent.
            if !(self.part_is_empty(dest) || self.node_borders_part(node, dest)) { continue }

            if self.check_node_contiguity(node, dest) {
                let delta = self.graph().node_weights().get_as_f64(series, node).unwrap();
                self.move_node(node, dest, false);
                remaining -= delta;
            } else {
                // Compute articulation bundle and move node with it (if necessary).
                let mut subgraph = self.cut_subgraph_within_part(node);
                subgraph.push(node);

                let delta = subgraph.iter()
                    .map(|&u| self.graph().node_weights().get_as_f64(series, u).unwrap())
                    .sum::<f64>();
                self.move_subgraph(&subgraph, dest, false);
                remaining -= delta;
            }
        }

        // If we overshot, recursively equalize in the other direction with higher tolerance.
        if -remaining > tolerance { self.equalize_parts(series, a, b, tolerance * 1.2) }
    }

    /// Equalize total weights across all parts using greedy swaps.
    /// `series` should name a column in node_weights.series.
    /// `tolerance` is the allowed fraction deviation from ideal (e.g. 0.01 = ±1%).
    /// `max_iter` is the maximum number of equalization passes to attempt.
    pub(crate) fn equalize(&mut self, series: &str, tolerance: f64, max_iter: usize) {
        assert_ne!(self.num_parts(), 1, "cannot equalize with only one part");
        assert!(self.graph().node_weights().contains(series),
            "series '{}' not found in node weights", series);

        let mut rng = rand::rng();

        // Compute target population and tolerance band (ignoring unassigned part 0).
        let total = (1..self.num_parts())
            .map(|part| self.part_weights.get_as_f64(series, part as usize).unwrap())
            .sum::<f64>();
        let target = total / ((self.num_parts() - 1) as f64);
        let allowed = target * tolerance;

        println!("Target population per part: {:.0} ±{:.0}", target, allowed);

        // Iterate until all parts are within tolerance, or we give up.
        for i in 0..max_iter {
            let totals = (1..self.num_parts())
                .map(|p| self.part_weights.get_as_f64(series, p as usize).unwrap())
                .collect::<Vec<_>>();
            let deviations = totals.iter()
                .map(|&total| (total - target).abs())
                .collect::<Vec<_>>();

            // Find the worst-offending part (max absolute deviation).
            let largest_deviation = *deviations.iter()
                .max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
            if largest_deviation <= allowed { break } // all parts within tolerance

            // Select a random part (weighted by absolute deviation)
            let distribution = WeightedIndex::new(&deviations).unwrap();
            let part = distribution.sample(&mut rng) as u32 + 1;

            // If the part total is more than twice the target, split into two districts while the smallest.
            if totals[part as usize - 1] > target * 2.0 {
                let (smallest, _) = self.part_with_min_weight(series);
                let (neighbor, _) = self.sample_neighboring_parts(smallest, 8, &mut rng).iter()
                    .map(|&p| (p, self.part_weights.get_as_f64(series, p as usize).unwrap()))
                    .min_by(|(_, a), (_, b)| a.partial_cmp(&b).unwrap())
                    .unwrap();

                // If merged successfully, assign a random frontier to the eliminated district and equalize with part
                if let Some(new_part) = self.merge_parts(neighbor, smallest, false) {
                    println!("Merged part {} into part {}, and split part {}", neighbor, smallest, part);

                    let frontier = self.frontiers.get(part as usize);
                    if !frontier.is_empty() {
                        let node = frontier[rng.random_range(0..frontier.len())];
                        self.move_node_with_articulation(node, new_part);
                        self.equalize_parts(series, part, new_part, largest_deviation / 2.0);
                        continue;
                    }
                }
            }

            // Pick random neighboring part and equalize.
            let neighbors = self.sample_neighboring_parts(part, 8, &mut rng);
            if neighbors.len() == 0 { continue }

            // Pick random neighbor
            let neighbors = neighbors.into_iter().collect::<Vec<_>>();
            let other = neighbors[rng.random_range(0..neighbors.len())];

            println!("{} ({:.0}): Equalizing part {} (pop: {:.0}) and part {} (pop: {:.0})", 
                i, largest_deviation, part, totals[part as usize - 1], other, totals[other as usize - 1]);

            self.equalize_parts(series, part, other, largest_deviation / 2.0);

        }
    }
}
