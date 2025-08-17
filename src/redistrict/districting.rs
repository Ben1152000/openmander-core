use std::{collections::HashMap, path::Path};

use anyhow::{Result};

use crate::{common::data::read_from_csv, map::{GeoId, MapData}};

#[derive(Debug)]
pub struct DistrictData {

}

#[derive(Debug)]
pub struct Districting<'a> {
    map: &'a MapData,
    assignments: HashMap<&'a GeoId, u32>, // district assignments Vec<>
    data: Option<DistrictData>, // district stats (cached for quicker updates)
}

impl<'a> Districting<'a> {
    pub fn len() { todo!() } // return number of districts

    /// Create a new empty districting map.
    /// This will initialize the districting with all blocks assigned to unassigned (0).
    pub fn new(map: &'a MapData) -> Self{
        Self {
            map,
            assignments: map.blocks.index.iter()
                .map(|(id, idx)| (id, 0)) 
                .collect(),
            data: None,
        }
    } // create empty districting map

    /// Load a districting from a CSV block assignment file.
    pub fn load_csv(&mut self, map: &MapData, csv_path: &Path) -> Result<()> {
        let df = read_from_csv(csv_path)?;
        let geo_id_col = df.column("geoid")?;
        let district_col = df.column("district")?;

        Ok(())
    }
}

