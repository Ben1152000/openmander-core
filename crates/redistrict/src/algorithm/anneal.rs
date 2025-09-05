use rand::Rng;

use crate::partition::GraphPartition;

pub struct AnnealConfig {
    pub sweeps: usize,           // outer loops
    pub steps_per_sweep: usize,  // proposals per sweep
    pub t0: f64,                 // initial temperature
    pub cooling: f64,            // per-sweep factor, e.g. 0.95
    pub alpha: f64,              // perimeter weight
    pub beta: f64,               // population weight
    pub k_peel: usize,           // try small peels up to k nodes
}

impl GraphPartition {
    // Implement simulated annealing with energy function, hard constraints
    pub fn anneal(&mut self) { todo!() }
}

impl GraphPartition {
    /// Run a short annealing pass to reduce a 2-district imbalance while controlling cut.
    /// `series` is the name of the balanced column in node weights.
    /// `alpha` is the weight on cut change relative to population change.
    pub fn anneal_balance_two(&mut self, series: &str, mut src: u32, mut dest: u32, alpha: f64, iters: usize) {
        assert!(src < self.num_parts && dest < self.num_parts && src != dest, 
            "src and dest must be distinct parts in range [0, {})", self.num_parts);

        let mut rng = rand::rng();

        let src_total = self.part_weights.get_as_f64(series, src as usize).unwrap();
        let dest_total = self.part_weights.get_as_f64(series, dest as usize).unwrap();
        let delta = src_total - dest_total;
        let mut remaining = delta / 2.0;

        for i in 0..iters {
            // Proposed move (direction depends on remaining).
            (src, dest, remaining) = if remaining > 0.0 { (src, dest, remaining) } else { (dest, src, -remaining) };

            // Candidate pool: 1-ring or 2-ring around the src/dest boundary.
            let candidates = self.frontiers.get(src);

            // Pick a random candidate on the side that needs to move.
            let node = loop {
                let u = candidates[rng.random_range(0..candidates.len())];
                if self.assignments[u] == src { break u }
            };

            // Skip if not adjacent.
            if !(self.part_is_empty(dest) || self.node_borders_part(node, dest)) { continue }

            // Collect articulation bundle (if necessary)
            let bundle =
                if self.check_node_contiguity(node, dest) { vec![] }
                else { self.cut_subgraph_within_part(node) };

            // Score: weight change and perimeter change for u (+ bundle).
            let mut weight_delta = self.graph.node_weights.get_as_f64(series, node).unwrap();

            let mut boundary_delta = self.graph.edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignments[v] == src).then_some(w))
                .sum::<f64>()
            - self.graph.edges_with_weights(node)
                .filter_map(|(v, w)| (self.assignments[v] == dest).then_some(w))
                .sum::<f64>();

            if bundle.len() > 0 {
                weight_delta += bundle.iter()
                    .map(|&u| self.graph.node_weights.get_as_f64(series, u).unwrap())
                    .sum::<f64>();

                boundary_delta -= self.graph.edges_with_weights(node)
                    .filter(|&(v, _)| self.assignments[v] == src)
                    .filter_map(|(v, w)| bundle.contains(&v).then_some(w))
                    .sum::<f64>();
            }

            let temperature = (1.0 as f64).max(10.0 * (0.95 as f64).powi(i as i32));
            let cost = (remaining - weight_delta).abs() - remaining.abs() + alpha * boundary_delta;

            if i % 100 == 0 {
                println!("iter {}: temperature {:.5} src {:.0} dest {:.0} remaining {:.0} candidates {}",
                    i,
                    temperature,
                    self.part_weights.get_as_f64(series, src as usize).unwrap(),
                    self.part_weights.get_as_f64(series, dest as usize).unwrap(),
                    remaining,
                    candidates.len()
                );
            }

            if cost <= 0.0 || rng.random::<f64>() < (-cost / temperature).exp() {
                if bundle.is_empty() {
                    self.move_node(node, dest, false)
                } else {
                    let subgraph = bundle.iter().chain(std::iter::once(&node)).copied().collect::<Vec<_>>();
                    self.move_subgraph(&subgraph, dest, false);
                }
                remaining -= weight_delta;
            }

        }
    }
}