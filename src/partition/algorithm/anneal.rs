use rand::Rng;

use crate::{Objective, partition::Partition};

struct OptimizationParams {
    pub max_iter: usize,
    pub init_temp: f64,
    pub cooling_rate: f64,
    pub early_stop_iters: usize,
    pub temp_search_batch_size: usize,
    pub batch_size: usize,
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
    if delta > EPSILON { 1.0 } else { ((delta - EPSILON) / temp).exp() }
}

/// Metropolis acceptance criterion for simulated annealing in temperature space.
/// Accept if `delta <= 0` or with probability `exp(-delta / T)`.
fn accept_metropolis<R: Rng + ?Sized>(delta: f64, temp: f64, rng: &mut R) -> bool {
    delta > EPSILON || rng.random::<f64>() < acceptance_probability(delta, temp)
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
            let accept = accept_metropolis(-delta, temp, &mut rng);

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

    /// Run multi-phase simulated annealing to optimize multiple objectives sequentially.
    /// 
    /// The algorithm maximizes the objective value (higher is better).
    /// At the end, the partition is restored to the best state found during the search.
    /// 
    /// Multi-phase adaptive annealing:
    /// Each phase consists of two steps:
    /// 1. Temperature tuning: Adjust temperature to reach start_prob acceptance rate
    /// 2. Cooling: Cool until end_prob is reached (or use early stopping if end_prob is None)
    /// 
    /// Parameters:
    /// - `objectives`: List of objectives to optimize (one per phase)
    /// - `max_iter`: Safety maximum iterations (prevents infinite loops)
    /// - `init_temp`: Initial temperature guess for first phase
    /// - `phase_start_probs`: Target acceptance probability to reach at start of each phase
    /// - `phase_end_probs`: Target acceptance probability to cool to (None = use early stopping)
    /// - `phase_cooling_rates`: Geometric cooling rate for each phase (temp *= (1 - rate) each batch)
    /// - `early_stop_iters`: Stop phase after this many iterations without improvement (when end_prob is None)
    /// - `temp_search_batch_size`: Batch size for temperature tuning steps
    /// - `batch_size`: Batch size for cooling phases
    pub(crate) fn anneal(&mut self,
        objectives: &[Objective],
        max_iter: usize,
        init_temp: f64,
        phase_start_probs: &[f64],
        phase_end_probs: &[Option<f64>],
        phase_cooling_rates: &[f64],
        early_stop_iters: usize,
        temp_search_batch_size: usize,
        batch_size: usize,
    ) {
        assert!(self.parts.get(0).len() == 0, "part 0 (unassigned) must be empty");
        assert!(self.num_parts() > 2, "need at least two parts for annealing");
        assert!(!objectives.is_empty(), "must provide at least one objective");
        assert!(phase_start_probs.len() == objectives.len(), "must provide start_prob for each phase");
        assert!(phase_end_probs.len() == objectives.len(), "must provide end_prob for each phase");
        assert!(phase_cooling_rates.len() == objectives.len(), "must provide cooling_rate for each phase");
        for (i, &rate) in phase_cooling_rates.iter().enumerate() {
            assert!(rate > 0.0 && rate < 1.0, "cooling_rate for phase {} must be in (0, 1)", i);
        }
        assert!(batch_size > 0, "batch_size must be > 0");
        assert!(temp_search_batch_size > 0, "temp_search_batch_size must be > 0");

        let mut params = OptimizationParams {
            max_iter,
            init_temp,
            cooling_rate: 0.0,  // Will be set per-phase
            early_stop_iters,
            temp_search_batch_size,
            batch_size,
        };

        let first_objective = &objectives[0];
        let initial_score = first_objective.compute(self);
        let mut state = OptimizationState {
            rng: rand::rng(),
            current_score: initial_score,
            current_iter: 0,
            best_score: initial_score,  // Initialize to actual score, not 0
            best_assignments: self.assignments(),
            best_iter: 0,
            temperature: params.init_temp,
        };

        // Run each phase
        for phase_idx in 0..objectives.len() {
            let objective = &objectives[phase_idx];
            let phase_num = phase_idx + 1;  // Display as 1-indexed
            
            // Set cooling rate for this phase
            params.cooling_rate = phase_cooling_rates[phase_idx];
            println!("DEBUG: Phase {} using cooling_rate = {}", phase_num, params.cooling_rate);
            
            // Recompute score for new objective (if not first phase)
            if phase_idx > 0 {
                state.current_score = objective.compute(self);
                state.best_score = state.current_score;
                state.best_assignments = self.assignments();
                state.best_iter = state.current_iter;
            }
            
            // Step 1: Tune temperature to reach start_prob
            let start_prob = phase_start_probs[phase_idx];
            self.tune_initial_temperature(objective, &params, &mut state, start_prob);
            
            // Step 2: Cool to end_prob (or use early stopping)
            match phase_end_probs[phase_idx] {
                Some(end_prob) => {
                    // Cool until probability threshold
                    self.cool_to_probability_threshold(objective, &params, &mut state, phase_num, end_prob);
                }
                None => {
                    // Use early stopping
                    self.cool_with_early_stopping(objective, &params, &mut state, phase_num);
                }
            }
        }

        // Restore the best solution found
        if state.current_score < state.best_score {
            self.set_assignments(state.best_assignments);
        }
    }

    /// Find initial temperature where average acceptance probability reaches target.
    /// Uses binary search to adaptively find the right temperature.
    /// The map state evolves during the search and is NOT restored.
    fn tune_initial_temperature(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
        target_prob: f64,
    ) {
        let mut min_temp = state.temperature * 1e-10;  // Lower bound for binary search (very low)
        let mut max_temp = state.temperature * 1e10; // Upper bound for binary search

        // Binary search for the right temperature - keep going until we find it or hit max_iter
        for _ in 0..100 {
            if state.current_iter >= params.max_iter { break }
            
            // Run a batch to measure average acceptance probability at current temperature
            let (_, avg_prob, final_prob) = self.anneal_batch(objective, state, params.temp_search_batch_size);
            
            // Print progress during temp search
            self.print_progress_with_avg_prob_and_curr(objective, avg_prob, final_prob, state, "Temp Search");

            // Check if we're close enough to target (within 5%)
            if (avg_prob - target_prob).abs() < 0.05 { break }

            // Adjust temperature bounds
            if avg_prob < target_prob { min_temp = (state.temperature * min_temp).sqrt() }
            else { max_temp = (state.temperature * max_temp).sqrt() }

            // Binary search midpoint
            state.temperature = (min_temp * max_temp).sqrt();
        }
    }

    /// Intermediate cooling phase: Cool until average acceptance probability drops below threshold
    fn cool_to_probability_threshold(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
        phase_num: usize,
        target_prob: f64,
    ) {
        while state.current_iter < params.max_iter {
            let prev_best = state.best_score;

            // Perform batch of iterations
            let (_, avg_prob, final_prob) = self.anneal_batch(objective, state, params.batch_size);
            
            // Check if we improved the best objective
            if state.best_score > prev_best { state.best_iter = state.current_iter; }

            // Print progress after each batch
            let phase_label = format!("Phase {}", phase_num);
            self.print_progress_with_avg_prob_and_curr(objective, avg_prob, final_prob, state, &phase_label);

            // Check if average probability has dropped below threshold
            if avg_prob < target_prob { return }

            // Cool temperature
            let old_temp = state.temperature;
            state.temperature *= 1.0 - params.cooling_rate;
            if state.current_iter % 10000 == 0 {
                println!("DEBUG: Iter {} cooling: {} -> {} (rate={})", state.current_iter, old_temp, state.temperature, params.cooling_rate);
            }
        }
    }

    /// Final cooling phase: Cool with early stopping based on no improvement
    fn cool_with_early_stopping(
        &mut self,
        objective: &Objective,
        params: &OptimizationParams,
        state: &mut OptimizationState<impl Rng>,
        phase_num: usize,
    ) {
        let mut iters_since_change = 0;
        
        while state.current_iter < params.max_iter {
            let prev_best = state.best_score;

            // Perform batch of iterations
            let (any_accepted, avg_prob, final_prob) = self.anneal_batch(objective, state, params.batch_size);

            if any_accepted { iters_since_change = 0; } else { iters_since_change += params.batch_size; }
            
            // Check if we improved the best objective
            if state.best_score > prev_best { state.best_iter = state.current_iter; }

            // Print progress after each batch
            let phase_label = format!("Phase {}", phase_num);
            self.print_progress_with_avg_prob_and_curr(objective, avg_prob, final_prob, state, &phase_label);

            // Early stopping check
            if iters_since_change >= params.early_stop_iters { return }

            // Cool temperature
            state.temperature *= 1.0 - params.cooling_rate;
        }
    }



    /// Perform a batch of annealing iterations.
    /// Simply calls anneal_iteration n times without modifying temperature or handling stopping logic.
    /// Returns (any_accepted, avg_prob, final_prob) where:
    /// - any_accepted: true if any move was accepted in the batch
    /// - avg_prob: average acceptance probability across all moves in the batch
    /// - final_prob: acceptance probability of the last move in the batch
    fn anneal_batch(
        &mut self,
        objective: &Objective,
        state: &mut OptimizationState<impl Rng>,
        n: usize,
    ) -> (bool, f64, f64) {
        let mut any_accepted = false;
        let mut prob_sum = 0.0;
        let mut final_prob = 0.0;
        
        for _ in 0..n {
            let (accepted, delta) = self.anneal_iteration(objective, state);
            if accepted {
                any_accepted = true;
            }
            let prob = acceptance_probability(delta, state.temperature);
            prob_sum += prob;
            final_prob = prob;
        }
        
        let avg_prob = prob_sum / n as f64;
        (any_accepted, avg_prob, final_prob)
    }

    /// Perform a single annealing iteration (propose move, accept/reject)
    /// Returns (accepted, delta) tuple
    fn anneal_iteration(
        &mut self,
        objective: &Objective,
        state: &mut OptimizationState<impl Rng>,
    ) -> (bool, f64) {
        // Pick random source part, weighted by frontier size
        let src = self.random_part_weighted_by_frontier(&mut state.rng).unwrap();

        // Pick random node on part boundary
        let candidates = self.frontiers.get(src as usize);
        let node = candidates[state.rng.random_range(0..candidates.len())];

        // Pick random destination part (that neighbors node)
        let dest = self.random_neighboring_part(node, &mut state.rng).unwrap();

        // Collect articulation bundle (if necessary to maintain contiguity)
        let bundle = if !self.check_node_contiguity(node, dest) { 
            self.cut_subgraph_within_part(node)
        } else { vec![] };

        // Apply the move temporarily to compute new objective
        let prev_src = src;
        if bundle.is_empty() {
            self.move_node(node, dest, false);
        } else {
            let subgraph = bundle.iter().chain(std::iter::once(&node)).copied().collect::<Vec<_>>();
            self.move_subgraph(&subgraph, dest, false);
        }

        // Compute new objective value
        let new_score = objective.compute(self);
        
        // Delta: negative of improvement (for minimization in Metropolis criterion)
        let delta = new_score - state.current_score;
        
        let accept = accept_metropolis(delta, state.temperature, &mut state.rng);

        if accept {
            // Keep the move
            state.current_score = new_score;
            
            // Update best if this is better
            if new_score > state.best_score {
                state.best_score = new_score;
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
        state: &OptimizationState<impl Rng>,
        phase: &str,
    ) {
        let comp_str = objective.metrics().iter()
            .map(|metric| { format!("{}={:.4}", metric.short_name(), metric.compute_score(self)) })
            .collect::<Vec<_>>()
            .join(" ");
        
        println!("Iter {}: phase {} | obj {:.12e} | {} | best {:.12e} @ {} | temp {:.12e} | prob {:.8} | curr_prob {:.8}",
            state.current_iter,
            phase,
            state.current_score,
            comp_str,
            state.best_score,
            state.best_iter,
            state.temperature,
            avg_prob,
            curr_prob,
        );
    }
}