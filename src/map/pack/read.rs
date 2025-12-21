use std::{path::Path};

use anyhow::{Context, Ok, Result};
use polars::frame::DataFrame;

use crate::{common, geom::Geometries, map::{GeoId, GeoType, Map, MapLayer, ParentRefs}};

impl MapLayer {
    /// Extract parent refs from the data DataFrame, returning (data, parents).
    fn unpack_data(&self, data: DataFrame) -> Result<(DataFrame, Vec<ParentRefs>)> {
        // split off final 5 columns of data
        let data_only = data.select_by_range(0..data.width()-5)
            .with_context(|| format!("Expected at least 6 columns in data, got {}", data.width()))?;

        let parents = (0..data_only.height()).map(|i| {
            Ok(ParentRefs::new([
                data.column("parent_state").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::State, s))),
                data.column("parent_county").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::County, s))),
                data.column("parent_tract").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Tract, s))),
                data.column("parent_group").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Group, s))),
                data.column("parent_vtd").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::VTD, s))),
            ]))
        }).collect::<Result<_>>()?;

        Ok((data_only, parents))
    }

    fn read_from_pack(&mut self, path: &Path) -> Result<()> {
        let layer_name = self.ty().to_str();
        let adj_path = path.join(format!("adj/{layer_name}.csr.bin"));
        let data_path = path.join(format!("data/{layer_name}.parquet"));
        let geom_path = path.join(format!("geom/{layer_name}.geoparquet"));
        let hull_path = path.join(format!("hull/{layer_name}.geoparquet"));

        (self.unit_data, self.parents) = self.unpack_data(common::read_from_parquet(&data_path)?)?;

        self.geo_ids = self.unit_data.column("geo_id")?.str()?
            .into_no_null_iter()
            .map(|val| GeoId::new(self.ty(), val))
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        // In the case of the state layer, populate adjacencies with dummy graph.
        (self.adjacencies, self.edge_lengths) = if self.ty() != GeoType::State {
            common::read_from_weighted_csr(&adj_path)?
        } else { (vec![vec![]], vec![vec![]]) };

        if hull_path.exists() {
            self.hulls = Some(
                common::read_from_geoparquet(&hull_path)?.into_iter()
                    .flat_map(|mp| mp.0)
                    .collect()
            );
        }

        self.construct_graph();

        if geom_path.exists() { 
            self.geoms = Some(Geometries::new(
                &common::read_from_geoparquet(&geom_path)?,
                None,
            ));
        }

        Ok(())
    }
}

impl Map {
    /// Read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        common::require_dir_exists(path)?;

        let mut map = Self::default();

        for ty in GeoType::ALL {
            let mut layer = MapLayer::new(ty);
            layer.read_from_pack(path)?;
            map.insert(layer);
        }

        Ok(map)
    }
}
