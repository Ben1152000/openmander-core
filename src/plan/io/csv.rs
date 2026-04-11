use std::{collections::HashMap, path::Path};

use anyhow::Result;

use crate::{map::GeoId, plan::Plan};

impl Plan {
    /// Load a plan from a CSV block assignment file.
    pub fn read_from_csv(&mut self, csv_path: &Path) -> Result<()> {
        let table = crate::io::csv::read_csv(csv_path)?;
        let block_layer = self.map().base()?;
        let assignments_vec = crate::io::csv::read_plan_assignments(table, block_layer)?;
        let assignments: HashMap<GeoId, u32> = assignments_vec.into_iter().collect();
        self.set_assignments(assignments)
    }

    /// Load a plan from CSV text (for browser/WASM use).
    pub fn load_csv(&mut self, csv: &str) -> Result<()> {
        let table = crate::io::csv::read_csv_string(csv)?;
        let block_layer = self.map().base()?;
        let assignments_vec = crate::io::csv::read_plan_assignments(table, block_layer)?;
        let assignments: HashMap<GeoId, u32> = assignments_vec.into_iter().collect();
        self.set_assignments(assignments)
    }

    /// Generate a CSV block assignment file.
    pub fn write_to_csv(&self, path: &Path) -> Result<()> {
        let assignments_map = self.get_assignments()?;
        let assignments: Vec<(GeoId, u32)> = assignments_map.into_iter().collect();
        crate::io::csv::write_plan_assignments(&assignments, path)
    }

    /// Generate a CSV block assignment as a string (for browser/WASM use).
    pub fn to_csv(&self) -> Result<String> {
        let assignments_map = self.get_assignments()?;
        let assignments: Vec<(GeoId, u32)> = assignments_map.into_iter().collect();
        crate::io::csv::write_plan_assignments_string(&assignments)
    }
}
