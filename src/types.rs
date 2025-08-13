use std::{collections::HashMap, sync::Arc};

use geo::{Point};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityType {
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
pub struct EntityKey {
    pub ty: EntityType,
    pub id: Arc<str>, // e.g., "31001" for county, "310010001001001" for block
}

/// Quick way to access parent entities across levels.
#[derive(Debug, Clone, Default)]
pub struct ParentRefs {
    pub state: Option<EntityKey>,
    pub county: Option<EntityKey>,
    pub tract: Option<EntityKey>,
    pub group: Option<EntityKey>,
    pub vtd: Option<EntityKey>,
}

#[derive(Debug, Clone)]
pub struct Entity {
    pub key: EntityKey,
    pub parents: ParentRefs,

    // Common name (state/county/VTD etc.)
    pub name: Option<Arc<str>>,

    // Optional derived metrics
    pub area_m2: Option<f64>,

    // Interior point (lon, lat)
    pub centroid: Option<Point<f64>>,
}

/// Compact CSR adjacency for a single level.
/// All indices are into a per-level contiguous array of entities (no strings).
#[derive(Debug, Clone)]
pub struct Adjacency {
    pub ty: EntityType,
    pub indptr: Vec<u32>,  // len = n_entities + 1
    pub indices: Vec<u32>, // concatenated neighbor lists
}


#[derive(Debug)]
pub struct MapLayer {
    pub ty: EntityType,
    pub entities: Vec<Entity>,
    // pub demographics: (),
    // pub elections: (),

    // Maps between global entity keys and per-level contiguous indices.
    pub index: HashMap<EntityKey, u32>,

    // Per-level geometry store, indexed by entities.
    pub geoms: Vec<shapefile::Polygon>,

    // CSR adjacency within the layer.
    // pub adj: (),
}

impl MapLayer {
    pub fn new(ty: EntityType) -> Self {
        Self {
            ty,
            entities: Vec::new(),
            index: HashMap::new(),
            geoms: Vec::new(),
        }
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
            states: MapLayer::new(EntityType::State),
            counties: MapLayer::new(EntityType::County),
            tracts: MapLayer::new(EntityType::Tract),
            groups: MapLayer::new(EntityType::Group),
            vtds: MapLayer::new(EntityType::VTD),
            blocks: MapLayer::new(EntityType::Block),
        }
    }
}
