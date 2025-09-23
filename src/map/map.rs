use crate::map::{GeoType, MapLayer};

/// Map struct that contains geographic data and geometries for redistricting.
#[derive(Debug)]
pub struct Map {
    layers: [MapLayer; GeoType::COUNT],
}

impl Default for Map {
    fn default() -> Self {
        Self { layers: GeoType::ALL.map(|ty| MapLayer::new(ty)) }
    }
}

impl Map {
    /// Get a reference to a specific map layer by geographic type.
    #[inline] pub fn get_layer(&self, ty: GeoType) -> &MapLayer { &self.layers[ty as usize] }

    /// Get all map layers as a slice.
    #[inline] pub fn get_layers(&self) -> &[MapLayer; GeoType::COUNT] { &self.layers }

    /// Get a mutable reference to a specific map layer by geographic type.
    #[inline] pub(super) fn get_layer_mut(&mut self, ty: GeoType) -> &mut MapLayer { &mut self.layers[ty as usize] }

    /// Get all map layers as a mutable slice.
    #[inline] pub(super) fn get_layers_mut(&mut self) -> &mut [MapLayer; GeoType::COUNT] { &mut self.layers }

    /// Set a specific map layer, replacing any existing data for that geographic type.
    #[inline]
    pub(super) fn set_layer(&mut self, layer: MapLayer) {
        let ty = layer.ty();
        self.layers[ty as usize] = layer;
    }
}
