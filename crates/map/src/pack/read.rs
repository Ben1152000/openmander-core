use std::{path::Path};

use anyhow::{Context, Ok, Result};
use openmander_common as common;
use openmander_geom::Geometries;
use polars::frame::DataFrame;

use crate::{GeoId, GeoType, Map, MapLayer, ParentRefs};

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

        (self.data, self.parents) = self.unpack_data(common::read_from_parquet(&data_path)?)?;

        self.geo_ids = self.data.column("geo_id")?.str()?
            .into_no_null_iter()
            .map(|val| GeoId::new(self.ty(), val))
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        if self.ty() != GeoType::State {
            (self.adjacencies, self.edge_lengths) = common::read_from_weighted_csr(&adj_path)?;
            self.construct_graph();
        }

        if hull_path.exists() {
            self.hulls = Some(
                common::read_from_geoparquet(&hull_path)?.into_iter()
                    .flat_map(|mp| mp.0)
                    .collect()
            );
        }

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
        for layer in map.get_layers_mut() {
            layer.read_from_pack(path)?;
        }

        Ok(map)
    }
}
