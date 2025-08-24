use std::{path::Path};

use anyhow::{Ok, Result};
use polars::prelude::*;

use openmander_geometry::PlanarPartition;

use crate::{common::{data::*, geom::*}, types::*};

impl MapLayer {
    fn read_from_pack(&mut self, path: &Path) -> Result<()> {
        let name = self.ty.to_str();
        let entity_path = path.join(format!("entities/{}.parquet", name));
        let geom_path = path.join(format!("geometries/{name}.fgb"));
        let adj_path = path.join(format!("geometries/adjacencies/{name}.fgb"));

        if entity_path.exists() {
            self.data = read_from_parquet(&entity_path)?;

            self.geo_ids = self.data.column("geo_id")?.str()?
                .into_no_null_iter()
                .map(|val| GeoId { ty: self.ty, id: Arc::from(val) })
                .collect();

            self.index = self.geo_ids.iter().enumerate()
                .map(|(i, geo_id)| (geo_id.clone(), i as u32))
                .collect();
        }

        if geom_path.exists() { 
            self.geoms = Some(PlanarPartition::new(read_from_geoparquet(&geom_path)?));
            self.geoms.as_mut().unwrap().adjacencies = read_from_adjacency_csr(&adj_path)?
        }

        Ok(())
    }
}

impl Map {
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        let mut map = Self::default();

        for ty in GeoType::order() {
            map.get_layer_mut(ty).read_from_pack(path)?;
        }

        Ok(map)
    }
}
