use std::{collections::HashMap, fs::File, path::Path};

use anyhow::{Result};
use openmander_map::{GeoType, GeoId, Map};
use polars::{frame::DataFrame, io::SerReader, prelude::{CsvReader, DataType}};

#[derive(Debug)]
pub struct DistrictData {

}

#[derive(Debug)]
pub struct Districting<'a> {
    map: &'a Map,
    assignments: HashMap<GeoId, u32>, // district assignments Vec<>
    data: Option<DistrictData>, // district stats (cached for quicker updates)
}

impl<'a> Districting<'a> {
    pub fn len() { todo!() } // return number of districts

    /// Create a new empty districting map.
    /// This will initialize the districting with all blocks assigned to unassigned (0).
    pub fn new(map: &'a Map) -> Self{
        Self {
            map: map,
            assignments: map.blocks.index.iter()
                .map(|(id, _)| (id.clone(), 0)) 
                .collect(),
            data: None,
        }
    }

    /// Load a districting from a CSV block assignment file.
    pub fn load_csv(&mut self, csv_path: &Path) -> Result<()> {
        /// Reads a CSV file from `path` into a Polars DataFrame.
        fn read_from_csv(path: &Path) -> Result<DataFrame> {
            let file = File::open(&path)?;
            let df = CsvReader::new(file).finish()?;
            Ok(df)
        }

        let df = read_from_csv(csv_path)?;

        self.assignments = df.column("GEOID20")?.cast(&DataType::String)?.str()?.into_no_null_iter()
            .zip(df.column("District")?.u32()?.into_no_null_iter())
            .map(|(g, d)| (GeoId { ty: GeoType::Block, id: g.into() }, d))
            .collect();

        // compute data?
        Ok(())
    }

    pub fn compute_data(&mut self) -> Result<()> { todo!() }

    pub fn randomize(&mut self, num_districts: u32) -> Result<()> { todo!() }
}
