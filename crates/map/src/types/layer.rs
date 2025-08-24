use std::{collections::HashMap};

use anyhow::{bail, Ok, Result};
use polars::{frame::DataFrame};

use super::{geo_id::GeoId, geo_type::GeoType, geom::PlanarPartition};

/// Quick way to access parent entities across levels.
#[derive(Debug, Clone, Default)]
pub struct ParentRefs {
    pub state: Option<GeoId>,
    pub county: Option<GeoId>,
    pub tract: Option<GeoId>,
    pub group: Option<GeoId>,
    pub vtd: Option<GeoId>,
}

impl ParentRefs {
    pub fn get(&self, ty: GeoType) -> Result<&Option<GeoId>> {
        match ty {
            GeoType::State => Ok(&self.state),
            GeoType::County => Ok(&self.county),
            GeoType::Tract => Ok(&self.tract),
            GeoType::Group => Ok(&self.group),
            GeoType::VTD => Ok(&self.vtd),
            GeoType::Block => bail!("Blocks cannot be a parent reference")
        }
    }

    pub fn set(&mut self, ty: GeoType, value: Option<GeoId>) -> Result<()> {
        match ty {
            GeoType::State => self.state = value,
            GeoType::County => self.county = value,
            GeoType::Tract => self.tract = value,
            GeoType::Group => self.group = value,
            GeoType::VTD => self.vtd = value,
            GeoType::Block => bail!("Blocks cannot be a parent reference")
        }
        Ok(())
    }
}

/// A single planar partition Layer of the map, containing entities and their relationships.
#[derive(Debug)]
pub struct MapLayer {
    pub ty: GeoType,
    pub geo_ids: Vec<GeoId>,
    pub index: HashMap<GeoId, u32>, // Map between geo_ids and per-level contiguous indices
    pub parents: Vec<ParentRefs>, // References to parent entities (higher level types)
    pub data: DataFrame, // Entity data (incl. name, centroid, geographic data, election data)
    pub geoms: Option<PlanarPartition>, // Per-level geometry store, indexed by entities (incl. adjacency)
}

impl MapLayer {
    pub fn new(ty: GeoType) -> Self {
        Self {
            ty,
            geo_ids: Vec::new(),
            index: HashMap::new(),
            parents: Vec::new(),
            data: DataFrame::empty(),
            geoms: None,
        }
    }
}
