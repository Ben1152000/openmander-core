use std::{path::Path};

use anyhow::{Context, Result};
use polars::frame::DataFrame;

use crate::{common, geom::Geometries, map::{GeoId, GeoType, Map, MapLayer, ParentRefs}, pack::{DiskPack, PackSource, PackFormat}};

impl MapLayer {
    /// Extract parent refs from the data DataFrame, returning (data, parents).
    fn unpack_data(&self, data: DataFrame) -> Result<(DataFrame, Vec<ParentRefs>)> {
        // split off final 5 columns of data
        let data_only = data.select_by_range(0..data.width() - 5)
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

    /// New API: read layer from any PackSource (disk/memory/http).
    fn read_from_pack_source(&mut self, src: &dyn PackSource, format: PackFormat) -> Result<()> {
        let layer_name = self.ty().to_str();

        let adj_file = format!("adj/{layer_name}.csr.bin");
        let data_file = format!("data/{}.{}", layer_name, format.data_extension());
        let geom_file = format!("geom/{}.{}", layer_name, format.geometry_extension());
        let hull_file = format!("hull/{}.{}", layer_name, format.geometry_extension());

        // data
        let data_bytes = src.get(&data_file)?;
        let df = match format {
            #[cfg(feature = "parquet")]
            PackFormat::Parquet => common::read_from_parquet_bytes(&data_bytes)?,
            PackFormat::Json => common::read_from_json_bytes(&data_bytes)?,
            #[cfg(not(feature = "parquet"))]
            PackFormat::Parquet => {
                return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
            }
        };
        (self.unit_data, self.parents) = self.unpack_data(df)?;

        // geo ids / index
        self.geo_ids = self.unit_data.column("geo_id")?.str()?.into_no_null_iter()
            .map(|val| GeoId::new(self.ty(), val))
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        // adjacency
        (self.adjacencies, self.edge_lengths) = if self.ty() != GeoType::State {
            let adj_bytes = src.get(&adj_file)?;
            common::read_from_weighted_csr_bytes(&adj_bytes)?
        } else { (vec![vec![]], vec![vec![]]) };

        // hulls (optional)
        if src.has(&hull_file) {
            let hull_bytes = src.get(&hull_file)?;
            let hull_geoms = match format {
                #[cfg(feature = "parquet")]
                PackFormat::Parquet => common::read_from_geoparquet_bytes(&hull_bytes)?,
                PackFormat::Json => common::read_from_geojson_bytes(&hull_bytes)?,
                #[cfg(not(feature = "parquet"))]
                PackFormat::Parquet => {
                    return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
                }
            };
            self.hulls = Some(
                hull_geoms.into_iter()
                    .flat_map(|mp| mp.0)
                    .collect(),
            );
        }

        self.construct_graph();

        // geometry (optional)
        if src.has(&geom_file) {
            let geom_bytes = src.get(&geom_file)?;
            let geoms = match format {
                #[cfg(feature = "parquet")]
                PackFormat::Parquet => common::read_from_geoparquet_bytes(&geom_bytes)?,
                PackFormat::Json => common::read_from_geojson_bytes(&geom_bytes)?,
                #[cfg(not(feature = "parquet"))]
                PackFormat::Parquet => {
                    return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
                }
            };
            self.geoms = Some(Geometries::new(&geoms, None));
        }

        Ok(())
    }
}

impl Map {
    /// Old API: read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        common::require_dir_exists(path)?;
        let src = DiskPack::new(path);
        let format = Self::detect_pack_format(&src)?;
        Self::read_from_pack_source(&src, format)
    }

    /// Detect pack format by checking for parquet or json files
    pub fn detect_pack_format(src: &dyn PackSource) -> Result<PackFormat> {
        // Check for parquet files first (if parquet feature is enabled)
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let parquet_file = format!("data/{}.parquet", ty.to_str());
                if src.has(&parquet_file) {
                    return Ok(PackFormat::Parquet);
                }
            }
        }
        // Check for json files
        for ty in GeoType::ALL {
            let json_file = format!("data/{}.json", ty.to_str());
            if src.has(&json_file) {
                return Ok(PackFormat::Json);
            }
        }
        // If no files found, return error with helpful message
        Err(anyhow::anyhow!(
            "No pack data files found. Expected files like 'data/block.parquet' or 'data/block.json'"
        ))
    }

    /// New API: read map from any PackSource (disk/memory/http).
    pub fn read_from_pack_source(src: &dyn PackSource, format: PackFormat) -> Result<Self> {
        let mut map = Self::default();

        for ty in GeoType::ALL {
            let mut layer = MapLayer::new(ty);

            // If your Map now stores layers as Option, you likely want:
            // - require top/bottom exist, but allow middle to be absent.
            //
            // This reader assumes presence is implied by the file existing.
            // If the data file for a layer is missing, we skip inserting it.
            let data_file = format!("data/{}.{}", ty.to_str(), format.data_extension());
            if !src.has(&data_file) { 
                continue;
            }

            // Load the layer - if it fails, return the error (don't silently skip)
            layer.read_from_pack_source(src, format)
                .with_context(|| format!("Failed to load layer {}", ty.to_str()))?;
            map.insert(layer);
        }

        // Enforce your current invariant: top + bottom must exist.
        // State is always the top layer for single-state packs.
        map.layer(GeoType::State)
            .ok_or_else(|| anyhow::anyhow!("Pack missing required top layer: state"))?;
        map.layer(GeoType::Block)
            .ok_or_else(|| anyhow::anyhow!("Pack missing required bottom layer: block"))?;

        Ok(map)
    }
}
