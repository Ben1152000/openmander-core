use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Ok, Result};
use polars::prelude::*;

use crate::{common::{data::*, fs::*, geom::*}, types::*};
use super::manifest::{Manifest, FileHash};

impl MapLayer {
    fn entities_to_df(&self) -> Result<DataFrame> {
        let parent = |pick: fn(&ParentRefs) -> Result<&Option<GeoId>>| -> Vec<Option<String>> {
            self.parents.iter()
                .map(|p| pick(p).ok().and_then(|g| g.as_ref().map(|x| x.id.to_string())))
                .collect()
        };

        let df = df![
            "geo_id" => self.geo_ids.iter().map(|geo_id| geo_id.id.to_string()).collect::<Vec<_>>(),
            "parent_state" => parent(|p| p.get(GeoType::State)),
            "parent_county" => parent(|p| p.get(GeoType::County)),
            "parent_tract" => parent(|p| p.get(GeoType::Tract)),
            "parent_group" => parent(|p| p.get(GeoType::Group)),
            "parent_vtd" => parent(|p| p.get(GeoType::VTD)),
        ]?;

        Ok(self.data.inner_join(&df, ["geo_id"], ["geo_id"])?)
    }

    fn write_to_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let name: &'static str = self.ty.to_str();
        let entity_path = &format!("entities/{name}.parquet");
        let geom_path = &format!("geometries/{name}.geoparquet");
        let adj_path = &format!("geometries/adjacencies/{name}.csr.bin");

        counts.insert(name.into(), self.geo_ids.len());

        // entities
        write_to_parquet(&path.join(entity_path), &self.entities_to_df()?)?;
        let (k, h) = sha256_file(entity_path, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // geometries
        if let Some(geom) = &self.geoms {
            write_to_geoparquet(&path.join(geom_path), &geom.shapes)?;
            let (k, h) = sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });

            // adjacencies (CSR)
            if self.ty != GeoType::State {
                write_to_adjacency_csr(&path.join(adj_path), &geom.adjacencies)?;
                let (k, h) = sha256_file(&adj_path, path)?;
                hashes.insert(k, FileHash { sha256: h });
            }
        }

        Ok(())
    }
}

impl Map {
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        let dirs = [
            "entities",
            "geometries",
            "geometries/adjacencies",
        ];
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
