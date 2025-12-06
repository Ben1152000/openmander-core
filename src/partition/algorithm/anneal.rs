use rand::Rng;

use crate::{Objective, partition::Partition};

struct OptimizationParams {
    pub max_iter: usize,
    pub init_temp: f64,
    pub cooling_rate: f64,
    pub init_prob: f64,
    pub final_prob: f64,
    pub early_stop_iters: usize,
    pub window_size: usize,
    pub log_every: usize,
}

struct OptimizationState<Rng: rand::Rng> {
    pub rng: Rng,
    pub current_score: f64,
    pub current_iter: usize,
    pub best_score: f64,
    pub best_assignments: Vec<u32>,
    pub best_iter: usize,
    pub temperature: f64,
}

/// Epsilon threshold for treating small deltas as improvements (handles floating point precision).
const EPSILON: f64 = 1e-10;

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

/// Calculate acceptance probability for a move with given delta and temperature.
/// Returns 1.0 if delta <= 0 (improvement), otherwise exp(-delta/temp).
/// Uses epsilon to treat tiny positive deltas as improvements (handles floating point precision).
fn acceptance_probability(delta: f64, temp: f64) -> f64 {
    if delta < -EPSILON { 1.0 } else { ((-delta - EPSILON) / temp).exp() }
}

/// Metropolis acceptance criterion for simulated annealing in temperature space.
/// Accept if `delta <= 0` or with probability `exp(-delta / T)`.
fn accept_metropolis<R: Rng + ?Sized>(delta: f64, temp: f64, rng: &mut R) -> bool {
    delta < -EPSILON || rng.random::<f64>() < acceptance_probability(delta, temp)
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
        assert!(self.part_weights().contains(series), "part_weights must contain series '{series}'");

        let mut rng = rand::rng();

        // Compute target part weight (average over all parts).
        let part_values = (0..self.num_parts())
            .map(|part| self.part_weights().get_as_f64(series, part as usize).unwrap())
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
            let src_weight = self.part_weights().get_as_f64(series, src as usize).unwrap();
            let dest_weight = self.part_weights().get_as_f64(series, dest as usize).unwrap();
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

    /// Run simulated annealing to optimize a generic objective function with adaptive temperature.
    /// This is the generalized version of `anneal_balance` that works with any `Objective`.
    /// 
    /// The algorithm maximizes the objective value (higher is better).
    /// At the end, the partition is restored to the best state found during the search.
    /// 
    /// Two-phase adaptive annealing:
    /// 1. Find initial temperature where average acceptance probability ≈ 0.9
    /// 2. Cool geometrically at specified rate with early stopping (stops after N iters without improvement)
    /// 
    /// Parameters:
    /// - `objective`: The objective to maximize
    /// - `max_iter`: Safety maximum iterations (prevents infinite loops)
    /// - `init_temp`: Initial temperature guess for phase 1 (default: 1.0)
    /// - `cooling_rate`: Geometric cooling rate (temp *= rate each iteration, e.g., 0.99999)
    /// - `early_stop_iters`: Stop phase 3 after this many iterations without improvement
    /// - `window_size`: Rolling window size for measuring acceptance rates (default: 1000)
    pub(crate) fn anneal(&mut self,
        objective: &Objective,
        max_iter: usize,
        init_temp: f64,
        cooling_rate: f64,
        early_stop_iters: usize,
        window_size: usize,
        log_every: usize,
    ) {
        assert!(self.parts.get(0).len() == 0, "part 0 (unassigned) must be empty");
        assert!(self.num_parts() > 2, "need at least two parts for annealing");
        assert!(cooling_rate > 0.0 && cooling_rate < 1.0, "cooling_rate must be in (0, 1)");

        let params = OptimizationParams {
            max_iter,
            init_temp,
            cooling_rate,
            init_prob: 0.9,
            final_prob: 0.1,
            early_stop_iters,
            window_size,
            log_every,
        };

        let mut state = OptimizationState {
            rng: rand::rng(),
            current_score: objective.compute(self),
            current_iter: 0,
            best_score: 0.0,
            best_assignments: self.assignments(),
            best_iter: 0,
            temperature: params.init_temp,
        };

        // Phase 1: Find initial temperature where average acceptance probability ≈ 0.9
        self.tune_initial_temperature(objective, &params, &mut state);

        // Phase 2: Cool with early stopping (stop after N iters without improvement)
        self.cool_to_target_acceptance(objective, &params, &mut state);

        // Restore the best solution found
        if state.current_score < state.best_score {
            self.set_assignments(state.best_assignments);
        }
    }

    /// Phase 1: Find initial temperature where average acceptance probability reaches target (typically 0.9)
    /// Uses binary search to adaptively find the right temperature.
    fn tune_initial_temperature(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
    ) {
        let mut min_temp = state.temperature * 1e-10;  // Lower bound for binary search (very low)
        let mut max_temp = state.temperature * 1e10; // Upper bound for binary search

        // Binary search for the right temperature - keep going until we find it or hit max_iter
        for iter in (0..50) {
            let avg_prob = self.measure_average_probability(objective, params, state);

            // Check if we're close enough to target (within 1%)
            if (avg_prob - params.init_prob).abs() < 0.01 { return }

            // Adjust temperature bounds
            if avg_prob < params.init_prob { min_temp = state.temperature }
            else { max_temp = state.temperature }

            // Binary search midpoint
            state.temperature = (min_temp * max_temp).sqrt();
        }
    }

    /// Phase 2: Cool geometrically with early stopping based on no improvement
    fn cool_to_target_acceptance(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
    ) {
        let mut iters_since_change = 0;

        // For printing: non-overlapping windows
        let mut window_prob_sum = 0.0;
        let mut window_count = 0;
        
        while state.current_iter < params.max_iter {
            let prev_best = state.best_score;
            
            // Perform one iteration
            let (accept, delta) = self.anneal_iteration(objective, state);

            if accept { iters_since_change = 0; } else { iters_since_change += 1; }
            
            // Calculate acceptance probability for this move
            let prob = acceptance_probability(delta, state.temperature);
            
            // Accumulate for printing window (non-overlapping)
            window_prob_sum += prob;
            window_count += 1;
            
            // Check if we improved the best objective
            if state.best_score > prev_best { state.best_iter = state.current_iter; }

            // Print progress every log_every iterations (non-overlapping windows)
            if state.current_iter % params.log_every == 0 {
                let avg_prob = window_prob_sum / window_count as f64;
                self.print_progress_with_avg_prob_and_curr(objective, avg_prob, prob, delta, state);
                // Reset window for next period
                window_prob_sum = 0.0;
                window_count = 0;
            }

            // Early stopping check
            if iters_since_change >= params.early_stop_iters { return }

            // Cool temperature
            state.temperature *= 1.0 - params.cooling_rate;
        }
    }

    /// Measure average acceptance probability at a given temperature over a window of iterations
    fn measure_average_probability(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
    ) -> f64 {
        let mut prob_sum = 0.0;
        let mut count = 0;
        
        // For printing: accumulate over non-overlapping windows
        let mut window_prob_sum = 0.0;
        let mut window_count = 0;
        
        for _ in 0..params.window_size {
            if state.current_iter >= params.max_iter {
                break;
            }
            
            let prev_best = state.best_score;
            
            let (_, delta) = self.anneal_iteration(objective, state);
            
            // Track best iteration
            if state.best_score > prev_best {
                state.best_iter = state.current_iter;
            }
            
            // Calculate acceptance probability for this move
            let prob = acceptance_probability(delta, state.temperature);
            prob_sum += prob;
            count += 1;
            
            // Accumulate for printing window
            window_prob_sum += prob;
            window_count += 1;
            
            // Print progress every log_every iterations (non-overlapping windows)
            if state.current_iter % params.log_every == 0 {
                let avg_prob = window_prob_sum / window_count as f64;
                self.print_progress_with_avg_prob_and_curr(objective, avg_prob, prob, delta, state);
                // Reset window for next period
                window_prob_sum = 0.0;
                window_count = 0;
            }
        }
        
        if count > 0 {
            prob_sum / count as f64
        } else {
            0.0
        }
    }

    /// Perform a single annealing iteration (propose move, accept/reject)
    /// Returns (accepted, delta) tuple
    fn anneal_iteration(
        &mut self,
        objective: &Objective,
        state: &mut OptimizationState<impl Rng>,
    ) -> (bool, f64) {
        // Pick random part, weighted by frontier size
        let src = self.random_part_weighted_by_frontier(&mut state.rng).unwrap();

        // Pick random node on part boundary
        let candidates = self.frontiers.get(src as usize);
        let node = candidates[state.rng.random_range(0..candidates.len())];

        // Pick random destination part (that neighbors node)
        let dest = self.random_neighboring_part(node, &mut state.rng).unwrap();

        // Collect articulation bundle (if necessary to maintain contiguity)
        let bundle =
            if self.check_node_contiguity(node, dest) { vec![] }
            else { self.cut_subgraph_within_part(node) };

        // Apply the move temporarily to compute new objective
        let prev_src = src;
        if bundle.is_empty() {
            self.move_node(node, dest, false);
        } else {
            let subgraph = bundle.iter().chain(std::iter::once(&node)).copied().collect::<Vec<_>>();
            self.move_subgraph(&subgraph, dest, false);
        }

        // Compute new objective value
        let new_objective = objective.compute(self);
        
        // Delta: negative of improvement (for minimization in Metropolis criterion)
        let delta = state.current_score - new_objective;
        
        let accept = accept_metropolis(delta, state.temperature, &mut state.rng);

        if accept {
            // Keep the move
            state.current_score = new_objective;
            
            // Update best if this is better
            if new_objective > state.best_score {
                state.best_score = new_objective;
                state.best_assignments = self.assignments();
            }
        } else {
            // Revert the move
            if bundle.is_empty() {
                self.move_node(node, prev_src, false);
            } else {
                let subgraph = bundle.iter().chain(std::iter::once(&node)).copied().collect::<Vec<_>>();
                self.move_subgraph(&subgraph, prev_src, false);
            }
        }

        state.current_iter += 1;
        (accept, delta)
    }

    /// Print progress information (for phase 3 where we don't have rolling window)
    fn print_progress(
        &self,
        objective: &Objective,
        delta: f64,
        state: &OptimizationState<impl Rng>,
    ) {
        let comp_str = objective.metrics().iter()
            .map(|metric| { format!("{}={:.4}", metric.short_name(), metric.compute_score(self)) })
            .collect::<Vec<_>>()
            .join(" ");
        
        // Calculate acceptance probability for this single move
        let prob = acceptance_probability(delta, state.temperature);
        
        println!("Iter {}: obj {:.4} | {} | best {:.4} | temp {:.12e} | prob {:.8}",
            state.current_iter,
            state.current_score,
            comp_str,
            state.best_score,
            state.temperature,
            prob,
        );
    }

    /// Print progress information with average probability over rolling window
    fn print_progress_with_avg_prob(
        &self,
        objective: &Objective,
        avg_prob: f64,
        state: &OptimizationState<impl Rng>,
    ) {
        let comp_str = objective.metrics().iter()
            .map(|metric| { format!("{}={:.4}", metric.short_name(), metric.compute_score(self)) })
            .collect::<Vec<_>>()
            .join(" ");
        
        println!("Iter {}: obj {:.4} | {} | best {:.4} | temp {:.12e} | prob {:.8}",
            state.current_iter,
            state.current_score,
            comp_str,
            state.best_score,
            state.temperature,
            avg_prob,
        );
    }

    /// Print progress information with both average probability and current move probability
    fn print_progress_with_avg_prob_and_curr(
        &self,
        objective: &Objective,
        avg_prob: f64,
        curr_prob: f64,
        delta: f64,
        state: &OptimizationState<impl Rng>,
    ) {
        let comp_str = objective.metrics().iter()
            .map(|metric| { format!("{}={:.4}", metric.short_name(), metric.compute_score(self)) })
            .collect::<Vec<_>>()
            .join(" ");
        
        let delta_print = if delta < 0.0 { delta.min(EPSILON) } else { delta };
        println!("Iter {}: obj {:.12e} | {} | best {:.12e} @ {} | temp {:.12e} | prob {:.8} | curr_prob {:.8} | delta {:.8e}",
            state.current_iter,
            state.current_score,
            comp_str,
            state.best_score,
            state.best_iter,
            state.temperature,
            avg_prob,
            curr_prob,
            delta_print,
        );
    }
}