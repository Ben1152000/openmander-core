use std::{collections::{HashMap, HashSet}, str::FromStr, sync::Arc};

use anyhow::{Result};

use crate::{
    Metric, Objective,
    io::wkb::multipolygon_to_wkb,
    map::{GeoId, GeoType, Map},
    partition::Partition,
};
use geograph::UnitId;

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
        let base = map.base()?;
        let unit_graph = base.get_unit_graph();
        let unit_weights = base.get_unit_weights();
        let region_weights = map.region()?.get_unit_weights();
        let partition = Partition::new(
            num_districts as usize + 1,
            unit_graph,
            unit_weights,
            region_weights,
        );

        Ok(Self { map, num_districts, partition })
    }

    /// Get an immutable reference to the map.
    #[inline] pub(super) fn map(&self) -> &Map { &self.map }

    /// Get the number of districts in this plan (excluding unassigned 0).
    #[inline] pub fn num_districts(&self) -> u32 { self.num_districts }

    /// Get the list of weight series available in the map's node weights.
    #[inline] pub fn series(&self) -> HashSet<String> { self.partition.series() }

    /// Set the block assignments for the plan.
    #[inline]
    pub fn set_assignments(&mut self, assignments: HashMap<GeoId, u32>) -> Result<()> {
        self.partition.set_assignments(
            self.map.base()?.geo_ids().iter()
                .map(|geo_id| assignments.get(geo_id).copied().unwrap_or(0))
                .collect()
        );
        Ok(())
    }

    /// Get raw block assignments as a flat `Vec<u32>` (index-aligned with units).
    pub fn get_assignments_vec(&self) -> Result<Vec<u32>> {
        Ok(self.partition.assignments())
    }

    /// Set assignments directly from a flat `Vec<u32>` (index-aligned with units).
    pub fn set_assignments_vec(&mut self, assignments: Vec<u32>) -> Result<()> {
        self.partition.set_assignments(assignments);
        Ok(())
    }

    /// Get block assignments as `Vec<(GeoId, u32)>`.
    pub fn get_assignments(&self) -> Result<Vec<(GeoId, u32)>> {
        let assignments = self.partition.assignments();
        let geo_ids = self.map.base()?.geo_ids();
        Ok(geo_ids.iter().zip(assignments.iter())
            .map(|(geo_id, &part)| (geo_id.clone(), part))
            .collect())
    }

    /// Sum of a weight series for each district (excluding unassigned 0).
    pub fn district_totals(&self, series: &str) -> Result<Vec<f64>> {
        Ok((1..=self.num_districts)
            .map(|d| self.partition.part_total(series, d))
            .collect())
    }

    /// Sum of a weight series across all parts including unassigned (part 0).
    /// Index 0 = unassigned units, indices 1..=num_districts = districts.
    pub fn all_part_totals(&self, series: &str) -> Result<Vec<f64>> {
        Ok(self.partition.part_totals(series))
    }

    /// Compute metric values for the current partition (per-district scores).
    pub fn compute_metric(&self, metric: &Metric) -> Vec<f64> {
        metric.compute(&self.partition)
    }

    /// Compute the aggregated score for a metric for the current partition.
    pub fn compute_metric_score(&self, metric: &Metric) -> f64 {
        metric.compute_score(&self.partition)
    }

    /// Compute objective value for the current partition.
    pub fn compute_objective(&self, objective: &Objective) -> f64 {
        objective.compute(&self.partition)
    }

    /// Randomize partition into contiguous districts.
    pub fn randomize(&mut self) -> Result<()> {
        self.partition.random_seed_fill();
        Ok(())
    }

    /// Randomize partition using a minimum spanning tree that minimizes county splits.
    ///
    /// Builds an MST with random edge weights plus penalties for precinct- and county-crossing
    /// edges, then greedily cuts subtrees from the leaves inward to form districts sized by `series`.
    pub fn randomize_minimize_county_splits(&mut self, series: &str) -> Result<()> {
        use std::collections::HashMap;
        use crate::map::GeoType;

        let base = self.map.base()?;
        let parents = base.parents();

        let make_ids = |ty: GeoType| {
            let mut id_map: HashMap<&str, u32> = HashMap::new();
            let mut next_id = 0u32;
            parents.iter()
                .map(|refs| {
                    let key = refs.get(ty).map(|g| g.id()).unwrap_or("");
                    *id_map.entry(key).or_insert_with(|| { let id = next_id; next_id += 1; id })
                })
                .collect::<Vec<u32>>()
        };

        let vtd_ids    = make_ids(GeoType::VTD);
        let county_ids = make_ids(GeoType::County);

        self.partition.random_minimize_county_splits(series, &vtd_ids, &county_ids);
        Ok(())
    }

    /// Perform exact population equalization using ILP block swaps.
    ///
    /// Builds the equalization graph, solves for a consistent excess assignment,
    /// and applies the resulting block transfers so every district reaches
    /// exactly T or T+1 people.
    ///
    /// Returns `(blocks_moved, fallback_edges)` where `fallback_edges` is the
    /// number of spanning-tree edges for which no ILP solution was found.
    pub fn equalize_exact(&mut self, series: &str) -> Result<(usize, usize)> {
        let base    = self.map.base()?;
        let parents = base.parents();

        let mut id_map: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
        let mut next_id = 0u32;
        let county_ids: Vec<u32> = parents.iter()
            .map(|refs| {
                let key = refs.get(GeoType::County).map(|g| g.id()).unwrap_or("");
                *id_map.entry(key).or_insert_with(|| { let id = next_id; next_id += 1; id })
            })
            .collect();

        let stats = self.partition.equalize_exact(series, &county_ids);
        Ok((stats.blocks_moved, stats.fallback_edges))
    }

    /// Build the equalization graph and spanning tree, print a structured
    /// summary of all edges and feasible net_flows to stdout, and return a
    /// one-line summary string.  Intended for development/debugging.
    pub fn equalize_exact_debug(&mut self, series: &str) -> Result<String> {
        let base    = self.map.base()?;
        let parents = base.parents();

        let mut id_map: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
        let mut next_id = 0u32;
        let county_ids: Vec<u32> = parents.iter()
            .map(|refs| {
                let key = refs.get(GeoType::County).map(|g| g.id()).unwrap_or("");
                *id_map.entry(key).or_insert_with(|| { let id = next_id; next_id += 1; id })
            })
            .collect();

        Ok(self.partition.debug_equalization_graph(series, &county_ids))
    }

    /// Run one outer iteration of equalization. Returns `true` if all districts are within tolerance.
    pub fn equalize_step(&mut self, series: &str, tolerance: f64) -> Result<bool> {
        Ok(self.partition.equalize_step(series, tolerance))
    }

    /// Equalize a weight series across districts using greedy swaps.
    pub fn equalize(&mut self, series: &str, tolerance: f64, max_iter: usize) -> Result<()> {
        self.partition.equalize(series, tolerance, max_iter);
        Ok(())
    }

    pub fn anneal_balance(&mut self, series: &str, max_iter: usize, initial_temp: f64, final_temp: f64, boundary_factor: f64) -> Result<()> {
        self.partition.anneal_balance(series, max_iter, initial_temp, final_temp, boundary_factor);
        Ok(())
    }

    pub fn tune_temperature(
        &mut self,
        objective: &Objective,
        target_prob: f64,
        init_temp: f64,
        batch_size: usize,
    ) -> f64 {
        self.partition.tune_temperature(objective, target_prob, init_temp, batch_size)
    }

    /// Returns `(new_temp, avg_prob, any_accepted)`.
    pub fn anneal_raw_chunk(
        &mut self,
        objective: &Objective,
        temperature: f64,
        cooling_rate: f64,
        chunk_size: usize,
    ) -> (f64, f64, bool) {
        self.partition.anneal_raw_chunk(objective, temperature, cooling_rate, chunk_size)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn anneal(
        &mut self,
        objectives: &[Objective],
        max_iter: usize,
        init_temp: f64,
        phase_start_probs: &[f64],
        phase_end_probs: &[Option<f64>],
        phase_cooling_rates: &[f64],
        early_stop_iters: usize,
        temp_search_batch_size: usize,
        batch_size: usize,
    ) -> Result<()> {
        self.partition.anneal(
            objectives, max_iter, init_temp,
            phase_start_probs, phase_end_probs, phase_cooling_rates,
            early_stop_iters, temp_search_batch_size, batch_size,
        );
        Ok(())
    }

    pub fn tabu_balance(
        &mut self,
        series: &str,
        max_iter: usize,
        tabu_tenure: usize,
        boundary_factor: f64,
        candidates_per_iter: usize,
    ) -> Result<()> {
        self.partition.tabu_balance(series, max_iter, tabu_tenure, boundary_factor, candidates_per_iter);
        Ok(())
    }

    pub fn recombine(&mut self, a: u32, b: u32) -> Result<()> {
        self.partition.recombine_parts(a, b);
        Ok(())
    }

    /// Assign all blocks belonging to a geographic unit to a given district.
    ///
    /// `layer` is the geographic level ("block", "vtd", "group", "tract", "county", "state").
    /// `geo_id` is the FIPS identifier for the unit at that level.
    /// `district` is the target district (1-indexed; 0 = unassigned).
    /// Contiguity is not enforced.
    pub fn assign_unit(&mut self, layer: &str, geo_id: &str, district: u32) -> Result<()> {
        anyhow::ensure!(
            district <= self.num_districts,
            "district {} out of range [0, {}]", district, self.num_districts
        );
        let ty = GeoType::from_str(layer)?;

        let base = self.map.base()?;

        let nodes: Vec<usize> = if ty == GeoType::Block {
            base.geo_ids().iter()
                .enumerate()
                .filter(|(_, gid)| gid.id() == geo_id)
                .map(|(i, _)| i)
                .collect()
        } else {
            base.parents().iter()
                .enumerate()
                .filter(|(_, refs)| refs.get(ty).map(|id| id.id() == geo_id).unwrap_or(false))
                .map(|(i, _)| i)
                .collect()
        };

        anyhow::ensure!(!nodes.is_empty(), "no blocks found for {} geo_id '{}'", layer, geo_id);

        for node in nodes {
            self.partition.move_node(node, district, false);
        }

        Ok(())
    }

    /// Assign all blocks belonging to multiple geographic units to a given district in one pass.
    ///
    /// Equivalent to calling `assign_unit` for each geo_id, but iterates the block table only
    /// once, making it O(blocks) rather than O(blocks × n). Contiguity is not enforced.
    pub fn assign_units_batch(&mut self, layer: &str, geo_ids: &[&str], district: u32) -> Result<()> {
        anyhow::ensure!(
            district <= self.num_districts,
            "district {} out of range [0, {}]", district, self.num_districts
        );
        if geo_ids.is_empty() { return Ok(()); }
        let ty = GeoType::from_str(layer)?;

        let id_set: std::collections::HashSet<&str> = geo_ids.iter().copied().collect();
        let base = self.map.base()?;

        let nodes: Vec<usize> = if ty == GeoType::Block {
            base.geo_ids().iter()
                .enumerate()
                .filter(|(_, gid)| id_set.contains(gid.id()))
                .map(|(i, _)| i)
                .collect()
        } else {
            base.parents().iter()
                .enumerate()
                .filter(|(_, refs)| refs.get(ty).map(|id| id_set.contains(id.id())).unwrap_or(false))
                .map(|(i, _)| i)
                .collect()
        };

        for node in nodes {
            self.partition.move_node(node, district, false);
        }

        Ok(())
    }

    /// Extract district boundaries as WKB using the DCEL Region.
    ///
    /// For each district, collects all assigned units and calls `Region::union_of`
    /// to produce the exact merged polygon via DCEL boundary tracing — no stitching
    /// or coordinate matching required.
    pub fn district_geometries_wkb(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        let base = self.map.base()?;
        let region = base.region();

        let mut results = Vec::with_capacity(self.num_districts as usize);

        for district in 1..=self.num_districts {
            let frontier = self.partition.frontier(district)
                .iter()
                .map(|&i| UnitId(i as u32));
            let boundary = region.union_of_frontier(
                frontier,
                |u| self.partition.assignment(u.0 as usize) == district,
            );
            let wkb = multipolygon_to_wkb(&boundary)?;
            results.push((district, wkb));
        }

        Ok(results)
    }
}
