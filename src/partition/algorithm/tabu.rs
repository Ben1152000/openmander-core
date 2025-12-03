use std::collections::HashMap;

use rand::Rng;

use crate::partition::Partition;

impl Partition {
    /// Tabu search to balance total weights across all districts while controlling cut length.
    ///
    /// - `series`: name of the node weight column to balance (e.g. population).
    /// - `max_iter`: maximum number of Tabu iterations.
    /// - `tabu_tenure`: number of iterations for which the reverse move is tabu.
    /// - `boundary_factor`: mix between population imbalance and boundary length:
    ///      0.0 = balance only, 1.0 = cut length only.
    /// - `candidates_per_iter`: how many random neighbor moves to sample per iteration.
    pub(crate) fn tabu_balance(
        &mut self,
        series: &str,
        max_iter: usize,
        tabu_tenure: usize,
        boundary_factor: f64,
        candidates_per_iter: usize,
    ) {
        assert!(self.parts.get(0).is_empty(), "part 0 (unassigned) must be empty");
        assert!(self.num_parts() > 2, "need at least two parts for tabu_balance");
        assert!(
            self.part_weights.contains(series),
            "part_weights must contain series '{series}'"
        );

        let mut rng = rand::rng();

        // --- 1. Compute target part weight (same as anneal_balance) ---
        let part_values = (0..self.num_parts())
            .map(|part| self.part_weights.get_as_f64(series, part as usize).unwrap())
            .collect::<Vec<_>>();
        let target = part_values.iter().sum::<f64>() / (self.num_parts() - 1) as f64;

        // --- 2. Compute initial total cost (population + boundary) ---
        let mut pop_cost = 0.0;
        for part in 1..self.num_parts() {
            let w = self
                .part_weights
                .get_as_f64(series, part as usize)
                .unwrap();
            let diff = w - target;
            pop_cost += diff * diff / target;
        }

        let mut boundary_cost = 0.0;
        for u in 0..self.num_nodes() {
            for (v, w) in self.graph().edges_with_weights(u) {
                if self.assignment(u) != self.assignment(v) {
                    boundary_cost += w;
                }
            }
        }
        // Each edge is seen twice.
        boundary_cost *= 0.5;

        let mut current_cost =
            pop_cost * (1.0 - boundary_factor) + boundary_cost * boundary_factor;

        let mut best_cost = current_cost;
        let mut best_partition = self.clone();

        // Tabu list: (node, forbidden_dest_part) -> expire_iter
        let mut tabu: HashMap<(usize, u32), usize> = HashMap::new();

        // --- 3. Main Tabu loop ---
        for iter in 0..max_iter {
            let mut best_move_node: Option<usize> = None;
            let mut best_move_src: u32 = 0;
            let mut best_move_dest: u32 = 0;
            let mut best_move_bundle: Vec<usize> = Vec::new();
            let mut best_move_new_cost: f64 = f64::INFINITY;

            // Sample a subset of the neighborhood (for speed).
            'candidate_loop: for _ in 0..candidates_per_iter {
                let Some(src) = self.random_part_weighted_by_frontier(&mut rng) else {
                    break 'candidate_loop;
                };

                let candidates = self.frontiers.get(src as usize);
                if candidates.is_empty() {
                    continue;
                }

                let node = candidates[rng.random_range(0..candidates.len())];

                // Collect distinct destination parts from neighbors.
                let mut dest_parts = self
                    .graph()
                    .edges(node)
                    .map(|v| self.assignment(v))
                    .filter(|&p| p != src && p != 0)
                    .collect::<Vec<_>>();
                dest_parts.sort();
                dest_parts.dedup();

                if dest_parts.is_empty() {
                    continue;
                }

                for &dest in &dest_parts {
                    // Maintain contiguity; compute dangling bundle if needed.
                    let bundle = if self.check_node_contiguity(node, dest) {
                        Vec::new()
                    } else {
                        self.cut_subgraph_within_part(node)
                    };

                    // Skip moves that would empty the source district.
                    if bundle.len() + 1 >= self.parts.get(src as usize).len() {
                        continue;
                    }

                    // --- Compute delta, reusing anneal_balance logic ---
                    let node_weight = self
                        .graph()
                        .node_weights()
                        .get_as_f64(series, node)
                        .unwrap()
                        + bundle
                            .iter()
                            .map(|&u| {
                                self.graph()
                                    .node_weights()
                                    .get_as_f64(series, u)
                                    .unwrap()
                            })
                            .sum::<f64>();

                    let src_weight = self
                        .part_weights
                        .get_as_f64(series, src as usize)
                        .unwrap();
                    let dest_weight = self
                        .part_weights
                        .get_as_f64(series, dest as usize)
                        .unwrap();

                    let weight_delta =
                        2.0 * node_weight * (node_weight + dest_weight - src_weight) / target;

                    let boundary_delta = self
                        .graph()
                        .edges_with_weights(node)
                        .filter_map(|(v, w)| (self.assignment(v) == src).then_some(w))
                        .sum::<f64>()
                        - self
                            .graph()
                            .edges_with_weights(node)
                            .filter_map(|(v, w)| (self.assignment(v) == dest).then_some(w))
                            .sum::<f64>()
                        - if !bundle.is_empty() {
                            self.graph()
                                .edges_with_weights(node)
                                .filter(|&(v, _)| self.assignment(v) == src)
                                .filter_map(|(v, w)| bundle.contains(&v).then_some(w))
                                .sum::<f64>()
                        } else {
                            0.0
                        };

                    let delta =
                        weight_delta * (1.0 - boundary_factor) + boundary_delta * boundary_factor;
                    let new_cost = current_cost + delta;

                    // --- Tabu + aspiration ---
                    let is_tabu = tabu
                        .get(&(node, dest))
                        .map_or(false, |&expire| expire > iter);
                    let is_aspiration = new_cost < best_cost;

                    if is_tabu && !is_aspiration {
                        continue;
                    }

                    // Keep best admissible move.
                    if new_cost < best_move_new_cost {
                        best_move_new_cost = new_cost;
                        best_move_node = Some(node);
                        best_move_src = src;
                        best_move_dest = dest;
                        best_move_bundle = bundle;
                    }
                }
            }

            let Some(node) = best_move_node else {
                // No admissible move found — terminate early.
                break;
            };

            // --- 4. Apply the chosen move ---
            if best_move_bundle.is_empty() {
                self.move_node(node, best_move_dest, false);
            } else {
                let mut subgraph = best_move_bundle.clone();
                subgraph.push(node);
                self.move_subgraph(&subgraph, best_move_dest, false);
            }

            current_cost = best_move_new_cost;

            // Update tabu list: forbid reassigning this node back to src for `tabu_tenure`.
            tabu.insert((node, best_move_src), iter + tabu_tenure);

            // Track global best solution.
            if current_cost < best_cost {
                best_cost = current_cost;
                best_partition = self.clone();
            }

            // (Optional) logging
            // if iter % 1000 == 0 {
            //     eprintln!(
            //         "[tabu] iter {} current {:.3} best {:.3}",
            //         iter, current_cost, best_cost
            //     );
            // }
        }

        // NOTE: currently we leave the partition at the final `current_cost`.
        // If you want to *restore* the global best solution, add a method
        // to reset assignments from `best_assignments` and recompute caches.
        *self = best_partition;
    }
}
