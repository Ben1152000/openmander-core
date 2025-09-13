use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Context, Ok, Result};
use openmander_common as common;
use polars::prelude::*;

use crate::{pack::manifest::{FileHash, Manifest}, GeoType, Map, MapLayer, ParentRefs};

impl MapLayer {
    /// Prepare entity data (with parent refs) for writing to a parquet file.
    fn pack_data(&self) -> Result<DataFrame> {
        /// Helper to extract parent IDs as strings
        fn get_parents(parents: &Vec<ParentRefs>, ty: GeoType) -> Vec<Option<&str>> {
            parents.iter()
                .map(|parents| parents.get(ty).map(|geo_id| geo_id.id()))
                .collect()
        }

        let parents_df = df![
            "geo_id" => self.geo_ids.iter().map(|geo_id| geo_id.id()).collect::<Vec<_>>(),
            "parent_state" => get_parents(&self.parents, GeoType::State),
            "parent_county" => get_parents(&self.parents, GeoType::County),
            "parent_tract" => get_parents(&self.parents, GeoType::Tract),
            "parent_group" => get_parents(&self.parents, GeoType::Group),
            "parent_vtd" => get_parents(&self.parents, GeoType::VTD),
        ]?;

        Ok(
            self.data
                .inner_join(&parents_df, ["geo_id"], ["geo_id"])
                .context("inner_join on 'geo_id' failed when preparing parquet")?
        )
    }

    fn write_to_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let layer_name = self.ty().to_str();
        let entity_path = &format!("data/{layer_name}.parquet");
        let adj_path = &format!("adj/{layer_name}.csr.bin");
        let geom_path = &format!("geom/{layer_name}.geoparquet");

        counts.insert(layer_name.into(), self.geo_ids.len());

        // entities
        common::write_to_parquet(&path.join(entity_path), &self.pack_data()?)?;
        let (k, h) = common::sha256_file(entity_path, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // adjacencies (CSR)
        if self.ty() != GeoType::State {
            common::write_to_weighted_csr(&path.join(adj_path), &self.adjacencies, &self.shared_perimeters)?;
            let (k, h) = common::sha256_file(&adj_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // geometries
        if let Some(geom) = &self.geoms {
            common::write_to_geoparquet(&path.join(geom_path), &geom.shapes())?;
            let (k, h) = common::sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        Ok(())
    }
}

impl Map {
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        let dirs = ["data", "adj", "geom"];
        common::ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for layer in self.get_layers() {
            layer.write_to_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        let mut f = File::create(&meta_path)?;
        serde_json::to_writer_pretty(&mut f, &manifest)?;

        Ok(())
    }
}
