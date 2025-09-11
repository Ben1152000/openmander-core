use anyhow::{anyhow, Context, Result};
use geo::{Coord, MapCoords, MultiPolygon};
use proj::Proj;

use crate::Geometries;

impl Geometries {
    /// Select a UTM EPSG from a lon/lat center and source datum.
    /// - WGS84: 326zz (north) / 327zz (south)
    /// - NAD83: 269zz (north only; if lat < 0, fall back to WGS84 UTM)
    fn select_metric_epsg(&self) -> u32 {
        let center = if let Some(bounds) = self.bounds() { bounds.center() }
            else { Coord { x: -104.0, y: 45.0 }};  // Fallback to US Geographic Center

        let zone = (((center.x + 180.0) / 6.0).floor() as i32 + 1)
            .clamp(1, 60) as u32;

        let north = center.y >= 0.0;

        // NAD83: UTM North only exists for NAD83 (EPSG:269xx). If southern, fall back to WGS84.
        match self.epsg() {
            4269 | 4937 => { if north { 26900 + zone } else { 32700 + zone } }
            _ => { if north { 32600 + zone } else { 32700 + zone } } // WGS84 UTM N/S 
        }
    }

    /// Reproject shapes from lon/lat to a metric CRS for Euclidean distance calculations.
    /// - Picks a UTM zone from the dataset center.
    pub(crate) fn reproject_to_metric(&self) -> Result<Vec<MultiPolygon<f64>>> {
        let from = format!("EPSG:{}", self.epsg());
        let to   = format!("EPSG:{}", self.select_metric_epsg());

        let proj = Proj::new_known_crs(&from, &to, None)
            .with_context(|| anyhow!("failed to build PROJ pipeline {from} -> {to}"))?;

        // Map coords into a fresh Vec<MultiPolygon<f64>>
        let projected = self.shapes().iter()
            .map(|shape| shape.map_coords(|coord: Coord<f64>| {
                let (x, y) = proj.convert((coord.x, coord.y))
                    .expect("CRS transform failed"); // safe if inputs are valid for src CRS
                Coord { x, y }
            }))
            .collect();

        Ok(projected)
    }
}
