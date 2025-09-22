use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Ok, Result};
use openmander_map::{GeoId, GeoType, Map};
use openmander_partition::Partition;

/// A districting plan, assigning blocks to districts.
#[derive(Debug)]
pub struct Plan {
    map: Arc<Map>,
    num_districts: u32, // number of districts (excluding unassigned 0)
    pub(crate) partition: Partition,
}

impl Plan {
    /// Create a new empty plan with a set number of districts.
    pub fn new(map: impl Into<Arc<Map>>, num_districts: u32) -> Self {
        let map: Arc<Map> = map.into();
        let partition = Partition::new(
            num_districts as usize + 1,
            map.get_layer(GeoType::Block).graph_handle()
        );

        Self { map, num_districts, partition }
    }

    /// Get a immutable reference to the map.
    #[inline] pub(crate) fn map(&self) -> &Map { &self.map }

    /// Get the number of districts in this plan (excluding unassigned 0).
    #[inline] pub fn num_districts(&self) -> u32 { self.num_districts }

    /// Get the list of weight series available in the map's node weights.
    #[inline]
    pub fn get_series(&self) -> Vec<&str> {
        self.partition.graph().node_weights().series()
    }

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
}
