use std::{collections::HashMap, fs::File, iter::once, path::Path, vec};

use anyhow::{anyhow, bail, Context, Result};
use openmander_map::{GeoId, GeoType, Map};
use polars::{frame::DataFrame, io::SerReader, prelude::{col, lit, CsvReader, DataType, IntoLazy, NamedFrom}, series::Series};

/// A districting plan, assigning blocks to districts.
#[derive(Debug)]
pub struct Plan<'a> {
    pub map: &'a Map,
    pub num_districts: u32, // number of districts (excluding unassigned 0)
    pub assignments: HashMap<GeoId, u32>, // district assignments
    pub on_boundary: HashMap<GeoId, bool>, // whether each block is on a district boundary
    pub data: DataFrame, // district stats (cached for quicker updates)
    pub boundary_lengths: Vec<f64>, // total boundary length of each district
}

impl<'a> Plan<'a> {
    /// Create a new empty plan with a set number of districts.
    pub fn empty(map: &'a Map, num_districts: u32) -> Result<Self> {
        let mut plan = Self {
            map,
            num_districts,
            assignments: map.blocks.index.iter().map(|(id, _)| (id.clone(), 0)).collect(),
            on_boundary: map.blocks.index.iter().map(|(id, _)| (id.clone(), false)).collect(),
            data: DataFrame::empty(),
            boundary_lengths: vec![0.0; (num_districts + 1) as usize],
        };

        plan.compute_boundaries();
        plan.compute_data()?;

        Ok(plan)
    }

    /// Load a plan from a CSV block assignment file.
    pub fn from_csv(map: &'a Map, num_districts: u32, csv_path: &Path) -> Result<Self> {
        let mut plan = Self {
            map,
            num_districts,
            assignments: map.blocks.index.iter().map(|(id, _)| (id.clone(), 0)).collect(),
            on_boundary: map.blocks.index.iter().map(|(id, _)| (id.clone(), false)).collect(),
            data: DataFrame::empty(),
            boundary_lengths: vec![0.0; (num_districts + 1) as usize],
        };

        // Read the CSV file into a Polars DataFrame, throwing an error if the file isn't found
        let df = CsvReader::new(File::open(csv_path)
            .with_context(|| format!("[Plan.from_csv] Failed to open CSV file: {}", csv_path.display()))?)
            .finish()
            .with_context(|| format!("[Plan.from_csv] Failed to read CSV file: {}", csv_path.display()))?;

        // assert CSV has at least two columns
        if df.width() < 2 { bail!("[Plan.from_csv] CSV file must have two columns: geo_id,district"); }

        // assert CSV has correct number of rows
        if df.height() != plan.assignments.len() {
            bail!("[Plan.from_csv] CSV file has {} rows, expected {}", df.height(), plan.assignments.len());
        }

        // Populate plan.assignments from CSV
        let blocks = df.column(df.get_column_names()[0])?.cast(&DataType::String)?;
        let districts = df.column(df.get_column_names()[1])?.cast(&DataType::UInt32)?;
        for (block, district) in blocks.str()?.into_no_null_iter().zip(districts.u32()?.into_no_null_iter()) {
            let geo_id = GeoId { ty: GeoType::Block, id: block.into() };
            if !map.blocks.geo_ids.contains(&geo_id) {
                bail!("[Plan.from_csv] GeoId {} in CSV not found in map", geo_id.id);
            }
            plan.assignments.insert(geo_id, district);
        }

        plan.compute_boundaries();
        plan.compute_data()?;

        Ok(plan)
    }

    /// Create a randomized plan with num districts, with approximately equal populations.
    pub fn randomize(map: &'a Map, num_districts: u32) -> Result<Self> {
        let plan = Self::empty(map, num_districts)?;

        // 1) Seed districts with random starting blocks
        for i in 1..num_districts+1 { }
        // 2) Expand districts until all blocks are assigned
        // 3) Equalize populations in each district
        todo!()
    }

    /// Select a random block from the map
    pub fn random_block(&self) -> GeoId {
        use rand::Rng;

        let n = self.assignments.len();
        let idx = rand::rng().random_range(0..n);
        self.assignments.iter().nth(idx).unwrap().0.clone()
    }

    /// Select a random block from the map that is on a district boundary
    fn random_boundary_block(&self) -> GeoId {
        use rand::Rng;

        let boundary_blocks: Vec<&GeoId> = self.on_boundary.iter()
            .filter(|(_, is_boundary)| **is_boundary)
            .map(|(geo_id, _)| geo_id)
            .collect();

        if boundary_blocks.is_empty() {
            // If no boundary blocks, fall back to any random block
            return self.random_block();
        }

        let idx = rand::rng().random_range(0..boundary_blocks.len());
        boundary_blocks[idx].clone()
    }

    /// Check if moving block to district would disconnect either the block's current district or the target district.
    fn would_disconnect(&self, block: &GeoId, district: u32) -> bool { todo!() }

    /// Move block to district: subtract block row from `prev`, add to `district`.
    pub fn set_district(&mut self, block: &GeoId, district: u32) -> Result<()> {
        let prev = self.assignments[block];
        if prev == district { return Ok(()); }

        let &i = self.map.blocks.index.get(block)
            .ok_or_else(|| anyhow!("[Plan.set_district] Invalid geo_id: {:?}", block))?;

        // Numeric columns to update (all columns in self.data except "idx")
        let columns = self.data.get_column_names()
            .into_iter()
            .filter_map(|c| (c.as_str() != "idx").then_some(c.as_str()))
            .collect::<Vec<_>>();

        // Build a one-row DataFrame of the block's values (fill nulls with 0)
        let mut row = self.map.blocks.data.clone().lazy()
            .select(columns.iter().map(|&name| col(name).alias(name)).collect::<Vec<_>>())
            .slice(i as i64, 1)
            .collect()?;

        // Build 2-row delta DataFrames: one row for `prev` with -values, one for `district` with +values
        let mut neg_row = row.clone().lazy()
            .select(columns.iter().map(|&name| (-col(name)).alias(name)).collect::<Vec<_>>())
            .collect()?;

        let delta = neg_row.with_column(Series::new("idx".into(), [prev]))?
            .vstack(row.with_column(Series::new("idx".into(), [district]))?)?;

        // Add delta to self.data by idx (left join + vectorized add), then drop the right-side columns
        //    Polars adds a suffix like `_right` for overlapping names; use that to sum & clean.
        let updates = columns.iter().map(|&name| {
            // col(n) + rhs (treat missing delta as 0)
            (col(name) + col(&format!("{name}_right")).fill_null(lit(0))).alias(name)
        }).collect::<Vec<_>>();

        self.data = self.data.clone().lazy()
            .left_join(delta.lazy(), col("idx"), col("idx"))
            .with_columns(updates)
            // Keep only original columns (drop *_right)
            .select(once(col("idx")).chain(columns.iter().map(|&name| col(name))).collect::<Vec<_>>())
            .collect()?;

        self.assignments.insert(block.clone(), district);
        Ok(())
    }

    /// Equalize population (given by column) of all current districts, within a given tolerance
    fn equalize_districts(&mut self, column: &str, tolerance: u32) { todo!() }

    /// Recompute cached data for each district
    fn compute_data(&mut self) -> Result<()> {
        let blocks = &self.map.blocks.data;

        // Build assignments DF: geo_id (Utf8), idx (u32)
        let mut geo_ids: Vec<String> = Vec::with_capacity(self.assignments.len());
        let mut idxs:    Vec<u32>    = Vec::with_capacity(self.assignments.len());
        for (geo_id, &district) in &self.assignments {
            geo_ids.push(geo_id.id.as_ref().to_string());
            idxs.push(district);
        }
        let assignments = DataFrame::new(vec![
            Series::new("geo_id".into(), geo_ids).into(),
            Series::new("district".into(), idxs).into(),
        ])?;

        // Choose numeric columns to aggregate (exclude "idx" and "geo_id")
        let aggr_cols: Vec<String> = blocks.get_columns().iter()
            .filter(|c| {
                c.name() != "geo_id" && c.name() != "idx" && matches!(c.dtype(),
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                    DataType::Float32 | DataType::Float64)
            }).map(|c| c.name().to_string())
            .collect();

        // Lazy: join blocks→assignments on "geo_id", group by idx, sum all numeric cols
        let blocks_lf = blocks.clone().lazy()
            .with_columns([col("geo_id").cast(DataType::String)]);
        let assign_lf = assignments.lazy()
            .with_columns([col("geo_id").cast(DataType::String)]);

        let grouped_df = blocks_lf
            .inner_join(assign_lf, col("geo_id"), col("geo_id"))
            .group_by([col("district")])
            .agg(aggr_cols.iter().map(|n| col(n).sum().alias(n)).collect::<Vec<_>>())
            .collect()?;

        // Base frame with idx 0..N-1 so every district has a row
        let base = DataFrame::new(vec![
            Series::new("idx".into(), (0..(self.num_districts + 1) as u32).collect::<Vec<u32>>()).into(),
        ])?;

        // Align to full idx range (0..N-1) and rebuild self.data from scratch
        self.data = base.lazy()
            .left_join(grouped_df.lazy(), col("idx"), col("district"))
            .with_columns(aggr_cols.iter()
                .map(|n| col(n).fill_null(lit(0)).alias(n))
                .collect::<Vec<_>>()
            )
            .collect()?;

        Ok(())
    }

    /// Recompute boundaries for each district
    fn compute_boundaries(&mut self) {
        // Clear existing data
        self.on_boundary = self.map.blocks.index.iter().map(|(id, _)| (id.clone(), false)).collect();
        self.boundary_lengths = vec![0.0; (self.num_districts + 1) as usize];

        for (geo_id, &district) in &self.assignments {
            let i = self.map.blocks.index[geo_id] as usize;
            let neighbors = &self.map.blocks.adjacencies[i];
            self.on_boundary.insert(
                geo_id.clone(),
                neighbors.iter().any(|&n| self.assignments[&self.map.blocks.geo_ids[n as usize]] != district)
            );
            self.boundary_lengths[district as usize] += neighbors.iter().enumerate()
                .filter_map(|(j, &n)| (self.assignments[&self.map.blocks.geo_ids[n as usize]] != district)
                    .then_some(self.map.blocks.shared_perimeters[i][j]))
                .sum::<f64>();
        }
    }
}
