use super::{geo_type::GeoType, layer::MapLayer};

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

impl MapData {
    pub fn get_layer(&self, ty: GeoType) -> &MapLayer {
        match ty {
            GeoType::State => &self.states,
            GeoType::County => &self.counties,
            GeoType::Tract => &self.tracts,
            GeoType::Group => &self.groups,
            GeoType::VTD => &self.vtds,
            GeoType::Block => &self.blocks,
        }
    }

    pub fn get_layer_mut(&mut self, ty: GeoType) -> &mut MapLayer {
        match ty {
            GeoType::State => &mut self.states,
            GeoType::County => &mut self.counties,
            GeoType::Tract => &mut self.tracts,
            GeoType::Group => &mut self.groups,
            GeoType::VTD => &mut self.vtds,
            GeoType::Block => &mut self.blocks,
        }
    }
}
