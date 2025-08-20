use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Ok, Result};
use polars::prelude::*;

use crate::{common::{data::*, fs::*, polygon::*}, types::*};
use super::manifest::{Manifest, FileHash};

impl MapLayer {
    fn entities_to_df(&self) -> Result<DataFrame> {
        let parent = |pick: fn(&ParentRefs) -> Result<&Option<GeoId>>| -> Vec<Option<String>> {
            self.parents.iter()
                .map(|p| pick(p).ok().and_then(|g| g.as_ref().map(|x| x.id.to_string())))
                .collect()
        };
    
        Ok(df![
            "geotype" => (0..self.entities.len()).map(|_| self.ty.to_str()).collect::<Vec<_>>(),
            "geoid" => self.entities.iter().map(|e| e.geo_id.id.to_string()).collect::<Vec<_>>(),
            "name" => self.entities.iter().map(|e| e.name.as_ref().map(|s| s.to_string())).collect::<Vec<_>>(),
            "area_m2" => self.entities.iter().map(|e| e.area_m2).collect::<Vec<_>>(),
            "lon" => self.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.x())).collect::<Vec<_>>(),
            "lat" => self.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.y())).collect::<Vec<_>>(),
            "parent_state" => parent(|p| p.get(GeoType::State)),
            "parent_county" => parent(|p| p.get(GeoType::County)),
            "parent_tract" => parent(|p| p.get(GeoType::Tract)),
            "parent_group" => parent(|p| p.get(GeoType::Group)),
            "parent_vtd" => parent(|p| p.get(GeoType::VTD)),
        ]?)
    }

    fn write_to_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let name: &'static str = self.ty.to_str();
        let entity_path = &format!("entities/{name}.parquet");
        let elec_path = &format!("elections/{name}.parquet");
        let demo_path = &format!("demographics/{name}.parquet");
        let geom_path = &format!("geometries/{name}.geoparquet");
        let adj_path = &format!("geometries/adjacencies/{name}.csr.bin");

        counts.insert(name.into(), self.entities.len());

        // entities
        write_to_parquet(&path.join(entity_path), &self.entities_to_df()?)?;
        let (k, h) = sha256_file(entity_path, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // elections
        if let Some(df) = &self.elec_data {
            write_to_parquet(&path.join(elec_path), df)?;
            let (k, h) = sha256_file(elec_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // demographics
        if let Some(df) = &self.demo_data {
            write_to_parquet(&path.join(demo_path), df)?;
            let (k, h) = sha256_file(demo_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // geometries
        if let Some(geom) = &self.geoms {
            write_to_geoparquet(&path.join(geom_path), &geom.geoms)?;
            let (k, h) = sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });

            // adjacencies (CSR)
            if self.ty != GeoType::State {
                write_to_adjacency_csr(&path.join(adj_path), &geom.adj_list)?;
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
            "elections",
            "demographics",
            "geometries",
            "geometries/adjacencies",
            "meta",
        ];
        ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for ty in GeoType::order() {
            self.get_layer(ty).write_to_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("meta/manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        let mut f = File::create(&meta_path)?;
        serde_json::to_writer_pretty(&mut f, &manifest)?;

        Ok(())
    }
}
