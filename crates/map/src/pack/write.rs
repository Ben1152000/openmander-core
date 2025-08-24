use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Context, Ok, Result};
use polars::prelude::*;

use crate::{common::{data::*, fs::*, geom::*}, types::*};
use super::manifest::{Manifest, FileHash};

impl MapLayer {
    /// Prepare entity data (with parent refs) for writing to a parquet file.
    fn pack_data(&self) -> Result<DataFrame> {
        /// Helper to extract parent IDs as strings
        fn get_parents(parents: &Vec<ParentRefs>, ty: GeoType) -> Vec<Option<String>> {
            parents.iter()
                .map(|parents| parents.get(ty).map(|geo_id| geo_id.id.to_string()))
                .collect()
        }

        let parents_df = df![
            "geo_id" => self.geo_ids.iter().map(|geo_id| geo_id.id.to_string()).collect::<Vec<_>>(),
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
        let name: &'static str = self.ty.to_str();
        let entity_path = &format!("data/{name}.parquet");
        let adj_path = &format!("adj/{name}.csr.bin");
        let geom_path = &format!("geom/{name}.geoparquet");

        counts.insert(name.into(), self.geo_ids.len());

        // entities
        write_to_parquet(&path.join(entity_path), &self.pack_data()?)?;
        let (k, h) = sha256_file(entity_path, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // adjacencies (CSR)
        if self.ty != GeoType::State {
            write_to_weighted_csr(&path.join(adj_path), &self.adjacencies, &self.shared_perimeters)?;
            let (k, h) = sha256_file(&adj_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // geometries
        if let Some(geom) = &self.geoms {
            write_to_geoparquet(&path.join(geom_path), &geom.shapes)?;
            let (k, h) = sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        Ok(())
    }
}

impl Map {
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        let dirs = ["data", "adj", "geom"];
        ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for ty in GeoType::order() {
            self.get_layer(ty).write_to_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        let mut f = File::create(&meta_path)?;
        serde_json::to_writer_pretty(&mut f, &manifest)?;

        Ok(())
    }
}
