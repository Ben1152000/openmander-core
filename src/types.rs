use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use polars::frame::DataFrame;

use crate::geometry::PlanarPartition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoType {
    State,      // Highest-level entity
    County,     // County -> State
    Tract,      // Tract -> County
    Group,      // Group -> Tract
    VTD,        // VTD -> County
    Block,      // Lowest-level entity
}

/// Stable key for any entity across levels.
/// Keep the original GEOID text (with leading zeros) but avoid repeated owned Strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeoId {
    pub ty: GeoType,
    pub id: Arc<str>, // e.g., "31001" for county, "310010001001001" for block
}

impl GeoId {
    /// Returns a new `GeoId` corresponding to the higher-level `GeoType`
    /// by truncating this GeoId's string to the correct prefix length.
    pub fn to_parent(&self, parent_ty: GeoType) -> GeoId {
        let len = match parent_ty {
            GeoType::State  => 2,
            GeoType::County => 5,
            GeoType::Tract  => 11,
            GeoType::Group  => 12,
            GeoType::VTD    => 11,
            GeoType::Block  => 15,
        };

        // If the id is shorter than expected, just take the full id.
        let prefix: Arc<str> = Arc::from(&self.id[..self.id.len().min(len)]);

        GeoId {
            ty: parent_ty,
            id: prefix,
        }
    }
}


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

#[derive(Debug)]
pub struct MapLayer {
    pub ty: GeoType,
    pub index: HashMap<GeoId, u32>, // Map between geo_id and per-level contiguous indices.
    pub entities: Vec<Entity>,
    pub parents: Vec<ParentRefs>,
    pub demo_data: Option<DataFrame>, // Demographic data
    pub elec_data: Option<DataFrame>, // Election data

    // Per-level geometry store, indexed by entities.
    pub geoms: Option<PlanarPartition>,

    // CSR adjacency within the layer.
    // pub adj: (),
}

impl MapLayer {
    pub fn new(ty: GeoType) -> Self {
        Self {
            ty,
            entities: Vec::new(),
            parents: Vec::new(),
            demo_data: None,
            elec_data: None,
            index: HashMap::new(),
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

#[derive(Debug)]
pub struct MapData {
    pub states: MapLayer,
    pub counties: MapLayer,
    pub tracts: MapLayer,
    pub groups: MapLayer,
    pub vtds: MapLayer,
    pub blocks: MapLayer,
}

impl Default for MapData {
    fn default() -> Self {
        Self {
            states: MapLayer::new(GeoType::State),
            counties: MapLayer::new(GeoType::County),
            tracts: MapLayer::new(GeoType::Tract),
            groups: MapLayer::new(GeoType::Group),
            vtds: MapLayer::new(GeoType::VTD),
            blocks: MapLayer::new(GeoType::Block),
        }
    }
}
