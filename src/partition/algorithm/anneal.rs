use rand::Rng;

use crate::partition::Partition;

/// Geometric cooling schedule for temperature `T`.
///
/// Parameters
/// - `temp_initial` = initial temperature T₀ (> 0)
/// - `temp_final`   = final/target temperature T_f (> 0)
/// - `iters`        = total number of cooling steps N (the schedule is defined for k = 0..=N)
/// - `iter`         = current iteration k (0-based)
///
/// Schedule
///     T_k = T₀ * α^k,  where  α = (T_f / T₀)^(1/N)
/// so that T_0 = T₀ and T_N = T_f exactly.
fn temp_geometric(initial_temp: f64, final_temp: f64, max_iter: usize, iter: usize) -> f64 {
    debug_assert!(initial_temp > 0.0 && final_temp > 0.0, "temperatures must be > 0");

    if iter > max_iter { return final_temp }
    let alpha = (final_temp / initial_temp).powf(1.0 / max_iter as f64);
    let temp = initial_temp * alpha.powi(iter as i32);

    if final_temp < initial_temp { temp.max(final_temp) } else { temp.min(final_temp) }
}

/// Metropolis acceptance criterion for simulated annealing in temperature space.
/// Accept if `delta <= 0` or with probability `exp(-delta / T)`.
fn accept_metropolis<R: Rng + ?Sized>(delta: f64, temp: f64, rng: &mut R) -> bool {
    delta <= 0.0 || rng.random::<f64>() < (-delta / temp).exp()
}

impl Partition {
    /// Run a longer annealing pass to reduce k-district imbalance while minimizing cut length.
    /// `series` is the name of the balanced column in node weights.
    /// `boundary_factor` is the weight on cut change relative to population change.
    /// `beta_initial` is the initial inverse temperature.
    /// `beta_final` is the final inverse temperature.
    /// `iters` is the number of iterations to run.
    pub(crate) fn anneal_balance(&mut self,
        series: &str,
        max_iter: usize,
        initial_temp: f64,
        final_temp: f64,
        boundary_factor: f64,
    ) {
        assert!(self.parts.get(0).len() == 0, "part 0 (unassigned) must be empty");
        assert!(self.num_parts() > 2, "need at least two parts for anneal_balance");
        assert!(self.part_weights.contains(series), "part_weights must contain series '{series}'");

        let mut rng = rand::rng();

        // Compute target part weight (average over all parts).
        let part_values = (0..self.num_parts())
            .map(|part| self.part_weights.get_as_f64(series, part as usize).unwrap())
            .collect::<Vec<_>>();
        let target = part_values.iter().sum::<f64>() / (self.num_parts() - 1) as f64;

        for i in 0..max_iter {
            // Pick random part, weighted by frontier size - 1.
            let src = self.random_part_weighted_by_frontier(&mut rng).unwrap();

            // Pick random node on part boundary (where part size > 1)
            let candidates = self.frontiers.get(src as usize);
            let node = candidates[rng.random_range(0..candidates.len())];

            // Pick random destination part (that neighbors node)
            let dest = self.random_neighboring_part(node, &mut rng).unwrap();

            // Collect articulation bundle (if necessary)
            let bundle =
                if self.check_node_contiguity(node, dest) { vec![] }
                else { self.cut_subgraph_within_part(node) };

            // Compute cost of move, randomly accept based on metropolis filter
            let node_weight = self.graph().node_weights().get_as_f64(series, node).unwrap()
                + bundle.iter()
                    .map(|&u| self.graph().node_weights().get_as_f64(series, u).unwrap())
                    .sum::<f64>();
            let src_weight = self.part_weights.get_as_f64(series, src as usize).unwrap();
            let dest_weight = self.part_weights.get_as_f64(series, dest as usize).unwrap();
            let weight_delta = 2.0 * node_weight * (node_weight + dest_weight - src_weight) / target;

            let boundary_delta = self.graph().edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignment(v) == src).then_some(w))
                .sum::<f64>()
            - self.graph().edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignment(v) == dest).then_some(w))
                .sum::<f64>()
            - if bundle.len() > 0 {
                self.graph().edges_with_weights(node)
                    .filter(|&(v, _)| self.assignment(v) == src)
                    .filter_map(|(v, w)| bundle.contains(&v).then_some(w))
                    .sum::<f64>()
            } else { 0.0 };

            let delta = weight_delta * (1.0 - boundary_factor) + boundary_delta * boundary_factor;
            let temp = temp_geometric(initial_temp, final_temp, max_iter, i);
            let accept = accept_metropolis(delta, temp, &mut rng);

            if i % 1000 == 0 {
                println!("Moving from part {} ({:.0}) to part {} ({:.0}) with temp {:.8} prob {:.3} weight {:.2} boundary {:.2} delta {:.2} accept {}",
                    src, src_weight, dest, dest_weight, temp,
                    if delta < 0.0 { 1.0 } else { (-delta / temp).exp() },
                    weight_delta * (1.0 - boundary_factor),
                    boundary_delta * boundary_factor,
                    delta,
                    accept,
                );
            }

            if accept {
                if bundle.is_empty() {
                    self.move_node(node, dest, false)
                } else {
                    let subgraph = bundle.iter().chain(std::iter::once(&node)).copied().collect::<Vec<_>>();
                    self.move_subgraph(&subgraph, dest, false);
                }
            }
        }
    }

    /// Implement simulated annealing with energy function, hard constraints
    #[allow(dead_code, unused_variables)]
    pub(crate) fn anneal_optimize(&mut self) { todo!() }
}