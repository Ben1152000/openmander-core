use std::{collections::HashMap, sync::Arc};

use geo::{MultiPolygon, Point};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityType {
    State,      // Highest-level entity
    County,     // County -> State
    Tract,      // Tract -> County
    Group,      // Group -> Tract
    Vtd,        // VTD -> County
    Block,      // Lowest-level entity
}

/// Stable key for any entity across levels.
/// Keep the original GEOID text (with leading zeros) but avoid repeated owned Strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityKey {
    pub ty: EntityType,
    pub geoid: Arc<str>, // e.g., "31001" for county, "310010001001001" for block
}

/// Minimal row kept in the “all-entities” table.
/// Geometry + adjacency live in level stores.
#[derive(Debug, Clone)]
pub struct Entity {
    pub key: EntityKey,

    // Optional human-friendly names (store only what exists for the level)
    pub name: Option<Arc<str>>,        // common name (state/county/VTD etc.)
    pub state_name: Option<Arc<str>>,
    pub county_name: Option<Arc<str>>,

    // Parent pointer (immediate parent only), or None for top level.
    pub parent: Option<EntityKey>,

    // Optional derived metrics (all in meters / m²)
    pub area_m2: Option<f64>,
    pub water_m2: Option<f64>,
    pub perimeter_m: Option<f64>,

    // Interior point (lon, lat).
    pub centroid: Option<Point<f64>>,
}

/// Maps between global entity keys and per-level contiguous indices.
#[derive(Debug)]
pub struct EntityIndex {
    pub ty: EntityType,
    pub keys: Vec<EntityKey>,               // index -> key
    pub inv_keys: HashMap<EntityKey, u32>   // key -> index
}

/// Per-level geometry store, separate from the entity table.
/// Index order must align with a level-specific index mapping (see EntityIndex).
#[derive(Debug)]
pub struct LevelGeometry {
    pub ty: EntityType,
    /// CRS for the stored geometry.
    /// must be computed after reprojection.
    pub crs_epsg: i32,
    /// Entity index -> geometry
    pub geoms: Vec<MultiPolygon<f64>>,
}

/// Compact CSR adjacency for a single level.
/// All indices are into a per-level contiguous array of entities (no strings).
#[derive(Debug, Default, Clone)]
pub struct CsrAdjacency {
    pub indptr: Vec<u32>,  // len = n_entities + 1
    pub indices: Vec<u32>, // concatenated neighbor lists
}
