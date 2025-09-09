use rand::Rng;

use crate::partition::Partition;

/// Geometric cooling schedule for `beta` (inverse temperature).
/// `beta_init` = initial beta value
/// `beta_max` = maximum beta value
/// `gamma` = cooling rate (0 < gamma < 1)
/// `k` = iteration number (0-based)
#[inline]
fn beta_geometric(beta_initial: f64, beta_final: f64, iter: usize, iters: usize) -> f64 {
    let gamma = (beta_final / beta_initial).powf(1.0 / iters as f64);
    (beta_initial * gamma.powi(iter as i32)).min(beta_final)
}

/// Metropolis acceptance criterion for simulated annealing.
/// Accept if `delta <= 0` or with probability `exp(-beta * delta)`.
#[inline]
fn accept_metropolis_beta<R: Rng + ?Sized>(delta: f64, beta: f64, rng: &mut R) -> bool {
    delta <= 0.0 || rng.random::<f64>() < (-beta * delta).exp()
}

impl Partition {
    /// Run a short annealing pass to reduce a 2-district imbalance while minimizing cut length.
    /// `series` is the name of the balanced column in node weights.
    /// `alpha` is the weight on cut change relative to population change.
    pub fn anneal_balance_two(&mut self,
        series: &str,
        mut src: u32,
        mut dest: u32,
        alpha: f64,
        iters: usize
    ) {
        assert!(src < self.num_parts() && dest < self.num_parts() && src != dest, 
            "src and dest must be distinct parts in range [0, {})", self.num_parts());

        let mut rng = rand::rng();

        let src_total = self.part_weights.get_as_f64(series, src as usize).unwrap();
        let dest_total = self.part_weights.get_as_f64(series, dest as usize).unwrap();
        let delta = src_total - dest_total;
        let mut remaining = delta / 2.0;
        let mut boundary = 0 as f64;

        for i in 0..iters {
            // Proposed move (direction depends on remaining).
            (src, dest, remaining) = if remaining > 0.0 { (src, dest, remaining) } else { (dest, src, -remaining) };

            // Pick a random node in the frontier of src adjacent to dest.
            let candidates = self.frontiers.get(src);
            let node = loop {
                let node = candidates[rng.random_range(0..candidates.len())];
                if self.part_is_empty(dest) || self.node_borders_part(node, dest) { break node }
            };

            // Collect articulation bundle (if necessary)
            let bundle =
                if self.check_node_contiguity(node, dest) { vec![] }
                else { self.cut_subgraph_within_part(node) };

            // Score: weight change and perimeter change for u (+ bundle).
            let weight_delta = self.graph().node_weights().get_as_f64(series, node).unwrap()
            + bundle.iter()
                .map(|&u| self.graph().node_weights().get_as_f64(series, u).unwrap())
                .sum::<f64>();

            let boundary_delta = self.graph().edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignments[v] == src).then_some(w))
                .sum::<f64>()
            - self.graph().edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignments[v] == dest).then_some(w))
                .sum::<f64>()
            - if bundle.len() > 0 {
                self.graph().edges_with_weights(node)
                    .filter(|&(v, _)| self.assignments[v] == src)
                    .filter_map(|(v, w)| bundle.contains(&v).then_some(w))
                    .sum::<f64>()
            } else { 0.0 };

            let beta = beta_geometric(0.01, 1.0, i, iters);
            let cost = (remaining - weight_delta).abs() - remaining.abs() + alpha * boundary_delta;
            let accept = accept_metropolis_beta(cost, beta, &mut rng);

            if true {
                println!("iter {}: beta {:.5} src {:.0} dest {:.0} remaining {:.0} boundary {:.0} candidates {} cost {} accept {}",
                    i,
                    beta,
                    self.part_weights.get_as_f64(series, src as usize).unwrap(),
                    self.part_weights.get_as_f64(series, dest as usize).unwrap(),
                    remaining,
                    boundary,
                    candidates.len(),
                    cost,
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
                remaining -= weight_delta;
                boundary += boundary_delta;
            }

        }
    }

    pub fn anneal_balance(&mut self, series: &str) { todo!() }

    pub fn anneal_optimize_two(&mut self, series: &str) { todo!() }

    /// Implement simulated annealing with energy function, hard constraints
    pub fn anneal_optimize(&mut self, series: &str) { todo!() }
}