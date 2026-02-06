use std::{collections::{HashMap, HashSet}, sync::Arc};

use anyhow::{ensure, Ok, Result};

use crate::{Metric, Objective, io::wkb::multipolygon_to_wkb, map::{GeoId, Map}, partition::Partition};

/// A districting plan, assigning blocks to districts.
#[derive(Clone, Debug)]
pub struct Plan {
    map: Arc<Map>,
    num_districts: u32, // number of districts (excluding unassigned 0)
    pub(super) partition: Partition,
}

impl Plan {
    /// Create a new empty plan with a set number of districts.
    pub fn new(map: impl Into<Arc<Map>>, num_districts: u32) -> Result<Self> {
        let map: Arc<Map> = map.into();
        let partition = Partition::new(
            num_districts as usize + 1,
            map.base()?.get_graph_ref(),
            map.region()?.get_graph_ref(),
        );

        Ok(Self { map, num_districts, partition })
    }

    /// Get a immutable reference to the map.
    #[inline] pub(super) fn map(&self) -> &Map { &self.map }

    /// Get the number of districts in this plan (excluding unassigned 0).
    #[inline] pub fn num_districts(&self) -> u32 { self.num_districts }

    /// Get the list of weight series available in the map's node weights.
    #[inline] pub fn series(&self) -> HashSet<String> { self.partition.series() }

    /// Set the block assignments for the plan.
    #[inline]
    pub fn set_assignments(&mut self, assignments: HashMap<GeoId, u32>) -> Result<()> {
        // map the list of geo_ids to their value in assignments, using 0 if not found
        self.partition.set_assignments(
            self.map.base()?.geo_ids().iter()
                .map(|geo_id| assignments.get(geo_id).copied().unwrap_or(0))
                .collect()
        );

        Ok(())
    }

    /// Get the block assignments for the plan.
    #[inline]
    pub fn get_assignments(&self) -> Result<HashMap<GeoId, u32>> {
        let assignments = self.map.base()?.index().clone().into_iter()
            .map(|(geo_id, i)| (geo_id, self.partition.assignment(i as usize)))
            .collect();

        Ok(assignments)
    }

    /// Get assignments as a vector of district IDs (index-based, for efficient WASM bindings).
    /// Returns a vector where index corresponds to node index in the base layer.
    #[inline]
    pub fn get_assignments_vec(&self) -> Result<Vec<u32>> {
        Ok(self.partition.assignments())
    }

    /// Set assignments from a vector of district IDs (index-based, for efficient WASM bindings).
    /// The vector length must match the number of nodes in the base layer.
    #[inline]
    pub fn set_assignments_vec(&mut self, assignments: Vec<u32>) -> Result<()> {
        self.partition.set_assignments(assignments);
        Ok(())
    }

    /// Sum of a given series for each district (including unassigned 0).
    #[inline]
    pub fn district_totals(&self, series: &str) -> Result<Vec<f64>> {
        ensure!(
            self.partition.series().contains(series),
            "[Plan.district_totals] part_weights missing series '{series}'"
        );

        Ok(self.partition.part_totals(series))
    }

    /// Compute a given metric for the current partition (per-district scores).
    #[inline]
    pub fn compute_metric(&self, metric: &Metric) -> Vec<f64> {
        metric.compute(&self.partition)
    }

    /// Compute the aggregated score for a given metric for the current partition.
    #[inline]
    pub fn compute_metric_score(&self, metric: &Metric) -> f64 {
        metric.compute_score(&self.partition)
    }

    /// Compute the objective value for the current partition.
    #[inline]
    pub fn compute_objective(&self, objective: &Objective) -> f64 {
        objective.compute(&self.partition)
    }

    /// Randomly assign all blocks to contiguous districts.
    #[inline]
    pub fn randomize(&mut self) -> Result<()> {
        Ok(self.partition.randomize())
    }

    /// Equalize total weights across all districts using greedy swaps.
    #[inline]
    pub fn equalize(&mut self, series: &str, tolerance: f64, max_iter: usize) -> Result<()> {
        ensure!(
            self.partition.series().contains(series),
            "[Plan.equalize] Population column '{series}' not found in node weights"
        );

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
    /// Multi-phase adaptive annealing.
    /// Each phase: (1) tune temp to start_prob, (2) cool to end_prob
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
    #[inline]
    pub fn anneal(&mut self, objectives: &[Objective], max_iter: usize, init_temp: f64, phase_start_probs: &[f64], phase_end_probs: &[Option<f64>], phase_cooling_rates: &[f64], early_stop_iters: usize, temp_search_batch_size: usize, batch_size: usize) -> Result<()> {
        self.partition.anneal(objectives, max_iter, init_temp, phase_start_probs, phase_end_probs, phase_cooling_rates, early_stop_iters, temp_search_batch_size, batch_size);

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

    /// Compute the geometry for each district and return as WKB bytes.
    ///
    /// Returns a vector of (district_id, wkb_bytes) pairs for districts 1..=num_districts.
    /// District 0 (unassigned) is excluded.
    /// Districts with no assigned units will have an empty WKB (empty MultiPolygon).
    ///
    /// Debug info about frontiers for each district.
    /// Returns (district, boundary_edges, vertices, deg1, deg2, deg3+, walks, closed, stuck, max_len)
    pub fn debug_frontier_info(&self) -> Result<Vec<(u32, usize, usize, usize, usize, usize, usize, usize, usize, usize)>> {
        let base_layer = self.map.base()?;
        let shapes = base_layer.shapes()
            .ok_or_else(|| anyhow::anyhow!("No shapes"))?;
        let adjacencies = base_layer.adjacencies();
        let graph = base_layer.get_graph_ref();
        let num_blocks = shapes.len();

        // Build state-border filter from outer_perimeter_m
        let is_state_border: Vec<bool> = (0..num_blocks)
            .map(|i| graph.node_weights().get_as_f64("outer_perimeter_m", i).unwrap_or(0.0) > 0.0)
            .collect();

        let assignments = self.partition.assignments();

        Ok((1..=self.num_districts)
            .map(|d| {
                let edges = self.partition.frontier_edge_endpoints(d);
                let (_, debug) = super::boundary::extract_district_boundary_with_debug(shapes, adjacencies, &edges, &assignments, d, &is_state_border);
                let s = &debug.stitch;
                (d, debug.boundary_edges_found, s.num_vertices, s.degree_1_count,
                 s.degree_2_count, s.degree_3_plus_count, s.walks_attempted,
                 s.walks_closed, s.walks_stuck, s.max_walk_len)
            })
            .collect())
    }

    /// Extract district boundaries by connecting frontier block centroids.
    ///
    /// Walks the frontier blocks in angular order and connects their centroids
    /// to form a closed polygon for each district.
    pub fn district_geometries_wkb(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        let base_layer = self.map.base()?;
        let adjacencies = base_layer.adjacencies();

        // Get centroids as Coord<f64> for boundary extraction
        let centroids: Vec<geo::Coord<f64>> = base_layer.centroids()
            .into_iter()
            .map(|p| geo::Coord { x: p.x(), y: p.y() })
            .collect();

        let mut results = Vec::with_capacity(self.num_districts as usize);

        for district in 1..=self.num_districts {
            let frontier_edges = self.partition.frontier_edge_endpoints(district);

            let boundary = super::boundary::extract_district_boundary_centroids(
                &centroids,
                adjacencies,
                &frontier_edges,
            );

            let unique_frontier_blocks: std::collections::HashSet<usize> =
                frontier_edges.iter().map(|&(src, _)| src).collect();
            let ring_points = boundary.0.first()
                .map(|p| p.exterior().0.len())
                .unwrap_or(0);
            eprintln!(
                "[D{}] frontier_blocks={}, ring_points={}, polygons={}",
                district, unique_frontier_blocks.len(), ring_points, boundary.0.len()
            );

            let wkb = multipolygon_to_wkb(&boundary)?;
            results.push((district, wkb));
        }

        Ok(results)
    }
}
