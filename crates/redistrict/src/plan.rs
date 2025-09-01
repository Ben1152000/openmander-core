use std::{collections::HashMap, fs::File, path::Path, sync::Arc, vec};

use anyhow::{anyhow, bail, Context, Ok, Result};
use openmander_map::{GeoId, GeoType, Map};
use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::{CsvReader, CsvWriter, DataType, NamedFrom}, series::Series};

use crate::graph::{WeightedGraph, WeightedGraphPartition};

/// A districting plan, assigning blocks to districts.
#[derive(Debug)]
pub struct Plan<'a> {
    pub map: &'a Map,
    pub num_districts: u32, // number of districts (excluding unassigned 0)
    pub graph: Arc<WeightedGraph>,
    pub partition: WeightedGraphPartition,
}

impl<'a> Plan<'a> {
    /// Create a new empty plan with a set number of districts.
    pub fn new(map: &'a Map, num_districts: u32) -> Self {
        let graph = Arc::new(WeightedGraph::new(
            map.blocks.len(),
            map.blocks.adjacencies.clone(),
            map.blocks.shared_perimeters.clone(),
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
        ));

        let partition = WeightedGraphPartition::new(
            num_districts as usize + 1,
            Arc::clone(&graph)
        );

        Self { map, num_districts, graph, partition }
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
