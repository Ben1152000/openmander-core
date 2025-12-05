use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, ensure, Ok, Result};

use crate::{Metric, Objective, map::{GeoId, GeoType, Map}, partition::Partition};

/// A districting plan, assigning blocks to districts.
#[derive(Clone, Debug)]
pub struct Plan {
    map: Arc<Map>,
    num_districts: u32, // number of districts (excluding unassigned 0)
    pub(super) partition: Partition,
}

impl Plan {
    /// Create a new empty plan with a set number of districts.
    pub fn new(map: impl Into<Arc<Map>>, num_districts: u32) -> Self {
        let map: Arc<Map> = map.into();
        let partition = Partition::new(
            num_districts as usize + 1,
            map.get_layer(GeoType::Block).graph_handle(),
            map.get_layer(GeoType::State).graph_handle(),
        );

        Self { map, num_districts, partition }
    }

    /// Get a immutable reference to the map.
    #[inline] pub(super) fn map(&self) -> &Map { &self.map }

    /// Get the number of districts in this plan (excluding unassigned 0).
    #[inline] pub fn num_districts(&self) -> u32 { self.num_districts }

    /// Get the list of weight series available in the map's node weights.
    #[inline] pub fn series(&self) -> Vec<&str> { self.partition.graph().node_weights().series() }

    /// Set the block assignments for the plan.
    #[inline]
    pub fn set_assignments(&mut self, assignments: HashMap<GeoId, u32>) -> Result<()> {
        // map the list of geo_ids to their value in assignments, using 0 if not found
        self.partition.set_assignments(
            self.map.get_layer(GeoType::Block).geo_ids().iter()
                .map(|geo_id| assignments.get(geo_id).copied().unwrap_or(0))
                .collect()
        );

        Ok(())
    }

    /// Get the block assignments for the plan.
    #[inline]
    pub fn get_assignments(&self) -> Result<HashMap<GeoId, u32>> {
        let assignments = self.map.get_layer(GeoType::Block).index().clone().into_iter()
            .map(|(geo_id, i)| (geo_id, self.partition.assignment(i as usize)))
            .collect();

        Ok(assignments)
    }

    /// Sum of a given series for each district (including unassigned 0).
    #[inline]
    pub fn district_totals(&self, series: &str) -> Result<Vec<f64>> {
        ensure!(
            self.partition.graph().node_weights().contains(series),
            "part_weights missing series '{series}'"
        );

        Ok(self.partition.part_totals(series))
    }

    /// Compute a given metric for the current partition (per-district scores).
    pub fn compute_metric(&self, metric: &Metric) -> Vec<f64> {
        metric.compute(&self.partition)
    }

    /// Compute the aggregated score for a given metric for the current partition.
    pub fn compute_metric_score(&self, metric: &Metric) -> f64 {
        metric.compute_score(&self.partition)
    }

    /// Compute the objective value for the current partition.
    pub fn compute_objective(&self, objective: &Objective) -> f64 {
        objective.compute(&self.partition)
    }

    /// Randomly assign all blocks to contiguous districts.
    #[inline] pub fn randomize(&mut self) -> Result<()> { Ok(self.partition.randomize()) }

    /// Equalize total weights across all districts using greedy swaps.
    #[inline]
    pub fn equalize(&mut self, series: &str, tolerance: f64, max_iter: usize) -> Result<()> {
        if !self.partition.graph().node_weights().contains(series) {
            bail!("[Plan.equalize] Population column '{}' not found in node weights", series);
        }

        Ok(self.partition.equalize(series, tolerance, max_iter))
    }

    /// Anneal to balance total weights across all districts.
    #[inline]
    pub fn anneal_balance(&mut self, series: &str, max_iter: usize, initial_temp: f64, final_temp: f64, boundary_factor: f64) -> Result<()> {
        self.partition.anneal_balance(series, max_iter, initial_temp, final_temp, boundary_factor);

        Ok(())
    }

    /// Run simulated annealing to optimize a generic objective function.
    /// 
    /// The algorithm maximizes the objective value (higher is better).
    /// 
    /// Three-phase adaptive annealing:
    /// 1. Find initial temperature where acceptance rate ≈ 0.9
    /// 2. Cool geometrically at specified rate until acceptance rate ≈ 0.1
    /// 3. Run at final temperature with early stopping (stops after N iters without improvement)
    /// 
    /// Parameters:
    /// - `objective`: The objective to maximize
    /// - `max_iter`: Safety maximum iterations (prevents infinite loops)
    /// - `init_temp`: Initial temperature guess for phase 1
    /// - `cooling_rate`: Geometric cooling rate (temp *= rate each iteration)
    /// - `early_stop_iters`: Stop phase 3 after this many iterations without improvement
    /// - `window_size`: Rolling window size for measuring acceptance rates
    #[inline]
    pub fn anneal(&mut self, objective: &Objective, max_iter: usize, init_temp: f64, cooling_rate: f64, early_stop_iters: usize, window_size: usize, log_every: usize) -> Result<()> {
        self.partition.anneal(objective, max_iter, init_temp, cooling_rate, early_stop_iters, window_size, log_every);

        Ok(())
    }

    #[inline]
    pub fn recombine(&mut self, a: u32, b: u32) -> Result<()> {
        self.partition.recombine_parts(a, b);

        Ok(())
    }

    /// Improve balance of `series` across districts using a Tabu search heuristic.
    #[inline]
    pub fn tabu_balance(
        &mut self,
        series: &str,
        max_iter: usize,
        tabu_tenure: usize,
        boundary_factor: f64,
        candidates_per_iter: usize,
    ) -> Result<()> {
        self.partition.tabu_balance(
            series,
            max_iter,
            tabu_tenure,
            boundary_factor,
            candidates_per_iter
        );

        Ok(())
    }
}
