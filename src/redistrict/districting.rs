use std::{collections::HashMap, path::Path};

use anyhow::{Result};
use polars::prelude::DataType;

use crate::{common::data::read_from_csv, map::{GeoId, Map}};

#[derive(Debug)]
pub struct DistrictData {

}

#[derive(Debug)]
pub struct Districting<'a> {
    map: &'a Map,
    assignments: HashMap<&'a GeoId, u32>, // district assignments Vec<>
    data: Option<DistrictData>, // district stats (cached for quicker updates)
}

impl<'a> Districting<'a> {
    pub fn len() { todo!() } // return number of districts

    /// Create a new empty districting map.
    /// This will initialize the districting with all blocks assigned to unassigned (0).
    pub fn new(map: &'a Map) -> Self{
        Self {
            map,
            assignments: map.blocks.index.iter()
                .map(|(id, idx)| (id, 0)) 
                .collect(),
            data: None,
        }
    } // create empty districting map

    /// Load a districting from a CSV block assignment file.
    pub fn load_csv(&mut self, csv_path: &Path) -> Result<()> {
        // let df = read_from_csv(csv_path)?;
        // let geo_ids: &polars::prelude::ChunkedArray<polars::prelude::StringType> = df.column("GEOID20")?.cast(&DataType::String)?.str()?;
        // let assignments = df.column("District")?.u32()?;

        // // convert geo_ids and assignments arrays to hashmap
        // self.assignments = geo_ids.iter().zip(assignments.iter())
        //     .map(|(id, district)| 
        //         id.and_then(|id| Some((id, district.unwrap_or(0)))))
            
        todo!()
        // Ok(())
    }
}

