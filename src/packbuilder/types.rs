use geo::{MultiPolygon, Polygon};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct TigerLayers {
    pub state: Vec<Feature>,
    pub counties: Vec<Feature>,
    pub tracts: Vec<Feature>,
    pub groups: Vec<Feature>,
    pub vtds: Vec<Feature>,
    pub blocks: Vec<Feature>,
}

#[derive(Debug, Clone)]
pub struct Feature {
    // minimal common attrs; add more as needed
    pub state_id: String,      // "31"
    pub county_id: Option<String>,
    pub tract_id: Option<String>,
    pub bg_id: Option<String>,
    pub vtd_id: Option<String>,
    pub block_geoid: Option<String>, // 15-digit
    pub name: Option<String>,
    pub geom: MultiPolygon<f64>,     // projected later for metrics
}

#[derive(Debug)]
pub struct Crosswalk {            // normalized edge form
    pub child_level: String,     // "block"
    pub child_id: String,        // block geoid
    pub parent_level: String,    // "vtd" | "bg" | "tract" | "county" | "state"
    pub parent_id: String,
    pub weight: f32,             // 1.0 if strict
}

#[derive(Debug)]
pub struct CSR {
    pub n: u32,
    pub m: u32,
    pub row_ptr: Vec<u32>,
    pub col_idx: Vec<u32>,
    pub edge_w: Option<Vec<f32>>,   // shared boundary length (m)
}