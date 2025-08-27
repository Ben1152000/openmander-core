use std::{collections::HashMap, fs::File, path::Path, vec};

use anyhow::{bail, Result};
use openmander_map::{GeoId, GeoType, Map};
use polars::{frame::DataFrame, io::SerReader, prelude::{col, lit, CsvReader, DataType, IntoLazy, NamedFrom}, series::Series};

/// A districting plan, assigning blocks to districts.
#[derive(Debug)]
pub struct Plan<'a> {
    pub map: &'a Map,
    pub num_districts: u32, // number of districts (excluding unassigned 0)
    pub assignments: HashMap<GeoId, u32>, // district assignments Vec<>
    pub data: DataFrame, // district stats (cached for quicker updates)
}

impl<'a> Plan<'a> {
    /// Create a new empty districting map with a set number of districts.
    pub fn empty(map: &'a Map, num_districts: u32) -> Result<Self> { 
        let mut districting = Self {
            map,
            num_districts,
            assignments: map.blocks.index.iter().map(|(id, _)| (id.clone(), 0)).collect(),
            data: DataFrame::empty(),
        };

        districting.compute_data()?;

        Ok(districting)
    }

    /// Load a plan from a CSV block assignment file.
    pub fn from_csv(map: &'a Map, num_districts: u32, csv_path: &Path) -> Result<Self> {
        let mut districting = Self::empty(map, num_districts)?;

        // Read the CSV file into a Polars DataFrame
        let df = CsvReader::new(File::open(csv_path)?).finish()?;

        // assert CSV has at least two columns
        if df.width() < 2 { bail!("[from_csv] CSV file must have two columns: geo_id,district") }

        // Convert the DataFrame into a HashMap of GeoId to district number
        districting.assignments = df.column(df.get_column_names().get(0).unwrap())?
            .cast(&DataType::String)?.str()?.into_no_null_iter()
            .zip(df.column(df.get_column_names().get(1).unwrap())?.i64()?.into_no_null_iter())
            .map(|(g, d)| (GeoId { ty: GeoType::Block, id: g.into() }, d as u32))
            .collect();

        districting.compute_data()?;

        Ok(districting)
    }

    /// Create a randomized plan with num districts, with approximately equal populations.
    pub fn random(map: &'a Map, num_districts: u32) -> Result<Self> {
        // 1) Seed districts with random starting blocks
        // 2) Expand districts until all blocks are assigned
        // 3) Equalize populations in each district
        todo!()
    }

    /// Check if moving block to district would disconnect either the block's current district or the target district.
    fn would_disconnect(&self, block: &GeoId, district: u32) -> bool { todo!() }

    /// Move block to district
    fn set_district(&mut self, block: &GeoId, district: u32) {
        let prev = self.assignments.get(block).unwrap_or(&0);
        self.assignments.insert(block.clone(), district);

        // recompute data for district change
    }

    /// Equalize population (given by column) of all current districts, within a given tolerance
    fn equalize_districts(&mut self, column: &str, tolerance: u32) { todo!() }

    /// Recompute cached data for each district
    pub fn compute_data(&mut self) -> Result<()> {
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

}
