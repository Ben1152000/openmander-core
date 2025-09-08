use crate::{GeoType, MapLayer};

/// Map struct that contains geographic data and geometries for redistricting.
#[derive(Debug)]
pub struct Map {
    pub layers: [MapLayer; GeoType::COUNT],
}

impl Default for Map {
    fn default() -> Self {
        Self { layers: GeoType::ALL.map(|ty| MapLayer::new(ty)) }
    }
}

impl Map {
    /// Get a reference to a specific map layer by geographic type.
    pub fn get_layer(&self, ty: GeoType) -> &MapLayer {
        &self.layers[ty as usize]
    }

    /// Get a mutable reference to a specific map layer by geographic type.
    pub fn get_layer_mut(&mut self, ty: GeoType) -> &mut MapLayer {
        &mut self.layers[ty as usize]
    }

    /// Set a specific map layer, replacing any existing data for that geographic type.
    pub fn set_layer(&mut self, layer: MapLayer) {
        let ty = layer.ty;
        self.layers[ty as usize] = layer;
    }
}
