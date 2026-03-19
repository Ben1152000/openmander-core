use crate::map::{GeoType, MapLayer};

use anyhow::{anyhow, Result};

/// Map struct that contains geographic data and geometries for redistricting.
#[derive(Debug, Default)]
pub struct Map {
    layers: [Option<MapLayer>; GeoType::COUNT],
}

impl Map {
    /// Get a reference to a specific map layer by geographic type.
    #[inline]
    pub fn layer(&self, ty: GeoType) -> Option<&MapLayer> {
        self.layers[ty as usize].as_ref()
    }

    /// Get a reference to the lowest-level (basic unit) layer in the map.
    #[inline]
    pub fn base(&self) -> Result<&MapLayer> {
        self.layer(GeoType::BOTTOM)
            .ok_or_else(|| anyhow!("[Map] Missing base layer {:?}", GeoType::BOTTOM))
    }

    /// Get a reference to the highest-level (region) layer in the map.
    #[inline]
    pub fn region(&self) -> Result<&MapLayer> {
        self.layer(GeoType::TOP)
            .ok_or_else(|| anyhow!("[Map] Missing region layer {:?}", GeoType::TOP))
    }

    /// Get a mutable reference to a specific map layer by geographic type.
    #[cfg(feature = "download")]
    #[inline]
    pub(super) fn layer_mut(&mut self, ty: GeoType) -> Option<&mut MapLayer> {
        self.layers[ty as usize].as_mut()
    }

    /// Get all non-null map layers as an iterator.
    #[inline]
    pub fn layers_iter(&self) -> impl Iterator<Item = &MapLayer> {
        self.layers.iter().filter_map(|layer| layer.as_ref())
    }

    /// Get all non-null map layers as an iterator.
    #[inline]
    pub fn layers_iter_mut(&mut self) -> impl Iterator<Item = &mut MapLayer> {
        self.layers.iter_mut().filter_map(|layer| layer.as_mut())
    }

    /// Set a specific map layer, replacing any existing data for that geographic type.
    pub(crate) fn insert(&mut self, layer: MapLayer) {
        let ty = layer.ty();
        self.layers[ty as usize] = Some(layer);
    }

    /// Return per-unit geometry statistics for a given layer.
    ///
    /// Each entry is `(geo_id, idx, num_polygons, holes_per_polygon, is_exterior)`.
    pub fn geometry_stats(&self, ty: GeoType) -> Result<Vec<(String, usize, Vec<usize>, bool)>> {
        let map_layer = self.layer(ty)
            .ok_or_else(|| anyhow!("Layer {:?} not present in this map.", ty))?;
        let region = map_layer.region();
        let geo_ids = map_layer.geo_ids();

        let stats = region.unit_ids().map(|uid| {
            let geom = region.geometry(uid);
            let idx = uid.0 as usize;
            let geo_id_str = geo_ids.get(idx).map(|g| g.id().to_string()).unwrap_or_default();
            let holes: Vec<usize> = geom.0.iter().map(|poly| poly.interiors().len()).collect();
            let is_ext = region.is_exterior(uid);
            (geo_id_str, idx, holes, is_ext)
        }).collect();

        Ok(stats)
    }

}
