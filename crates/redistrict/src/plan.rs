use std::{collections::HashMap, fs::File, iter::once, path::Path, vec};

use anyhow::{anyhow, bail, Context, Ok, Result};
use openmander_map::{GeoId, GeoType, Map};
use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::{col, lit, CsvReader, CsvWriter, DataType, IntoLazy, NamedFrom}, series::Series};
use rand::distr;

use crate::graph::WeightedGraphPartition;

/// A districting plan, assigning blocks to districts.
#[derive(Debug)]
pub struct Plan<'a> {
    pub map: &'a Map,
    pub num_districts: u32, // number of districts (excluding unassigned 0)
    pub partition: WeightedGraphPartition,
}

impl<'a> Plan<'a> {
    /// Create a new empty plan with a set number of districts.
    pub fn new(map: &'a Map, num_districts: u32) -> Self {
        Self {
            map,
            num_districts,
            partition: WeightedGraphPartition::new(
                num_districts as usize,
                map.blocks.len(),
                map.blocks.data.get_columns().iter()
                    .filter(|&column| column.name() != "idx")
                    .map(|column| (column.name().to_string(), column.as_series().unwrap()))
                    .filter_map(|(name, series)| match series.dtype() {
                        DataType::Int64  => Some((name, series.i64().unwrap().into_no_null_iter().collect())),
                        DataType::Int32  => Some((name, series.i32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::Int16  => Some((name, series.i16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::Int8   => Some((name, series.i8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::UInt64 => Some((name, series.u64().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::UInt32 => Some((name, series.u32().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::UInt16 => Some((name, series.u16().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        DataType::UInt8  => Some((name, series.u8().unwrap().into_no_null_iter().map(|v| v as i64).collect())),
                        _ => None,
                    }).collect(),
                map.blocks.data.get_columns().iter()
                    .map(|column| (column.name().to_string(), column.as_series().unwrap()))
                    .filter_map(|(name, series)| match series.dtype() {
                        DataType::Float64 => Some((name, series.f64().unwrap().into_no_null_iter().collect())),
                        DataType::Float32 => Some((name, series.f32().unwrap().into_no_null_iter().map(|v| v as f64).collect())),
                        _ => None,
                    }).collect(),
                map.blocks.adjacencies.clone(),
                map.blocks.shared_perimeters.clone(),
            )
        }
    }

    /// Load a plan from a CSV block assignment file.
    pub fn load_csv(&mut self, csv_path: &Path) -> Result<()> {
        // Read the CSV file into a Polars DataFrame, throwing an error if the file isn't found
        let df = CsvReader::new(File::open(csv_path)
            .with_context(|| format!("[Plan.from_csv] Failed to open CSV file: {}", csv_path.display()))?)
            .finish()
            .with_context(|| format!("[Plan.from_csv] Failed to read CSV file: {}", csv_path.display()))?;

        // assert CSV has at least two columns
        if df.width() < 2 { bail!("[Plan.from_csv] CSV file must have two columns: geo_id,district"); }

        // assert CSV has correct number of rows
        if df.height() != self.map.blocks.len() {
            bail!("[Plan.from_csv] CSV file has {} rows, expected {}", df.height(), self.map.blocks.len());
        }

        // Populate plan.assignments from CSV
        let blocks = df.column(df.get_column_names()[0])?.cast(&DataType::String)?;
        let districts = df.column(df.get_column_names()[1])?.cast(&DataType::UInt32)?;

        let assignments: HashMap<GeoId, u32> = blocks.str()?.into_no_null_iter()
            .zip(districts.u32()?.into_no_null_iter())
            .map(|(block, district)| {
                let geo_id = GeoId { ty: GeoType::Block, id: block.into() };
                if !self.map.blocks.geo_ids.contains(&geo_id) {
                    bail!("[Plan.from_csv] GeoId {} in CSV not found in map", geo_id.id);
                }
                Ok((geo_id, district))
            })
            .collect::<Result<HashMap<GeoId, u32>>>()?;

        // map the list of geo_ids to their value in assignments, using 0 if not found
        self.partition.set_assignments(
            self.map.blocks.geo_ids.iter()
                .map(|geo_id| assignments.get(geo_id).copied().unwrap_or(0))
                .collect::<Vec<u32>>()
        );

        Ok(())
    }

/*
    /// Create a randomized plan with num districts, with approximately equal populations.
    pub fn randomize(map: &'a Map, num_districts: u32) -> Result<Self> {
        let mut plan = Self::empty(map, num_districts)?;

        // 1) Seed districts with random starting blocks
        for d in 1..num_districts+1 {
            plan.set_district(&plan.random_unassigned_block(), d)?;
        }

        // 2) Expand districts until all blocks are assigned
        // 3) Equalize populations in each district
        todo!()
    }

    /// Select a random block from the map
    pub fn random_block(&self) -> GeoId {
        use rand::Rng;

        self.assignments.iter()
            .nth(rand::rng().random_range(0..self.assignments.len()))
            .unwrap().0.clone()
    }

    fn random_unassigned_block(&self) -> GeoId {
        use rand::Rng;
        
        let assigned = self.assignments.iter()
            .filter(|&(_, &district)| district != 0)
            .collect::<Vec<_>>();

        assigned[rand::rng().random_range(0..assigned.len())].0.clone()
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
 */
    /// Check if moving block to district would disconnect either the block's current district or the target district.
    fn would_disconnect(&self, block: &GeoId, district: u32) -> bool { todo!() }

    /// Equalize population (given by column) of all current districts, within a given tolerance
    fn equalize_districts(&mut self, column: &str, tolerance: u32) { todo!() }

    /// Move block to district: subtract block row from `prev`, add to `district`.
    pub fn set_district(&mut self, block: &GeoId, district: u32) -> Result<()> {
        todo!()
    }

    /// Generate a CSV block assignment
    pub fn to_csv(&self, path: &Path) -> Result<()> {
        let assignments: HashMap<GeoId, u32> = self.map.blocks.index.clone().into_iter()
            .map(|(geo_id, i)| (geo_id, self.partition.assignments[i as usize]))
            .collect();

        let (geo_ids, districts): (Vec<String>, Vec<u32>) = assignments.iter()
            .filter_map(|(geo_id, &district)| (district != 0)
                .then_some((geo_id.id.as_ref().to_string(), district)))
            .unzip();

        CsvWriter::new(File::create(path)?).finish(
            &mut DataFrame::new(vec![
                Series::new("geo_id".into(), geo_ids).into(),
                Series::new("district".into(), districts).into(),
            ])?
        )?;

        Ok(())
    }
}
