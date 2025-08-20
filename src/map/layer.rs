use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use polars::frame::DataFrame;

use crate::geometry::PlanarPartition;
use super::{geo_type::GeoType, geo_id::GeoId};

#[derive(Debug, Clone)]
pub struct Entity {
    pub geo_id: GeoId,
    pub name: Option<Arc<str>>,  // Common name
    pub area_m2: Option<f64>,
    pub centroid: Option<geo::Point<f64>>,  // Interior point (lon, lat)
}

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
    pub index: HashMap<GeoId, u32>, // Map between geo_ids and per-level contiguous indices.
    pub entities: Vec<Entity>,
    pub parents: Vec<ParentRefs>,
    pub demo_data: Option<DataFrame>, // Demographic data
    pub elec_data: Option<DataFrame>, // Election data

    // Per-level geometry store, indexed by entities (incl. adjacency).
    pub geoms: Option<PlanarPartition>,
}

impl MapLayer {
    pub fn new(ty: GeoType) -> Self {
        Self {
            ty,
            index: HashMap::new(),
            entities: Vec::new(),
            parents: Vec::new(),
            demo_data: None,
            elec_data: None,
            geoms: None,
        }
    }

    pub fn compute_adjacencies(&mut self) -> Result<()> { 
        self.geoms
            .as_mut()
            .ok_or_else(|| anyhow!("Cannot compute adjacencies on empty geometry!"))?
            .compute_adjacencies_fast(1e8)?;
        Ok(())
    }
}
