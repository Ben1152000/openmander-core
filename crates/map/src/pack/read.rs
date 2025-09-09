use std::{path::Path};

use anyhow::{Context, Ok, Result};
use polars::prelude::*;

use crate::{common::*, GeoId, GeoType, Geometries, Map, MapLayer, ParentRefs};

impl MapLayer {
    /// Extract parent refs from the data DataFrame, returning (data, parents).
    fn unpack_data(&self, data: DataFrame) -> Result<(DataFrame, Vec<ParentRefs>)> {
        // split off final 5 columns of data
        let data_only = data.select_by_range(0..data.width()-5)
            .with_context(|| format!("Expected at least 6 columns in data, got {}", data.width()))?;

        let state_refs = data.column("parent_state").ok().map(|c| c.str()).transpose()?;
        let county_refs = data.column("parent_county").ok().map(|c| c.str()).transpose()?;
        let tract_refs = data.column("parent_tract").ok().map(|c| c.str()).transpose()?;
        let group_refs = data.column("parent_group").ok().map(|c| c.str()).transpose()?;
        let vtd_refs = data.column("parent_vtd").ok().map(|c| c.str()).transpose()?;

        let parents = (0..data_only.height()).map(|i| {
            Ok(ParentRefs::new([
                state_refs.and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::State, s))),
                county_refs.and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::County, s))),
                tract_refs.and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Tract, s))),
                group_refs.and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Group, s))),
                vtd_refs.and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::VTD, s))),
            ]))
        }).collect::<Result<Vec<_>>>()?;

        Ok((data_only, parents))
    }

    fn read_from_pack(&mut self, path: &Path) -> Result<()> {
        let entity_path = path.join(format!("data/{}.parquet", self.ty.to_str()));
        let geom_path = path.join(format!("geom/{}.geoparquet", self.ty.to_str()));
        let adj_path = path.join(format!("adj/{}.csr.bin", self.ty.to_str()));

        (self.data, self.parents) = self.unpack_data(read_from_parquet(&entity_path)?)?;

        self.geo_ids = self.data.column("geo_id")?.str()?
            .into_no_null_iter()
            .map(|val| GeoId::new(self.ty, val))
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        if self.ty != GeoType::State {
            (self.adjacencies, self.shared_perimeters) = read_from_weighted_csr(&adj_path)?;
            self.construct_graph();
        }

        if geom_path.exists() { 
            self.geoms = Some(Geometries::new(read_from_geoparquet(&geom_path)?));
        }

        Ok(())
    }
}

impl Map {
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        require_dir_exists(path)?;

        let mut map = Self::default();
        for layer in map.get_layers_mut() {
            layer.read_from_pack(path)?;
        }

        Ok(map)
    }
}
