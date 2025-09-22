use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Context, Ok, Result};
use geo::MultiPolygon;
use openmander_common as common;
use polars::{df, frame::DataFrame, prelude::DataFrameJoinOps};

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

        Ok(self.data
            .inner_join(&parents_df, ["geo_id"], ["geo_id"])
            .context("inner_join on 'geo_id' failed when preparing parquet")?)
    }

    fn write_to_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let layer_name = self.ty().to_str();
        let adj_file = &format!("adj/{layer_name}.csr.bin");
        let data_file = &format!("data/{layer_name}.parquet");
        let geom_file = &format!("geom/{layer_name}.geoparquet");
        let hull_file = &format!("hull/{layer_name}.geoparquet");

        counts.insert(layer_name.into(), self.geo_ids.len());

        // entities
        common::write_to_parquet(&path.join(data_file), &self.pack_data()?)?;
        let (k, h) = common::sha256_file(data_file, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // adjacencies (CSR)
        if self.ty() != GeoType::State {
            common::write_to_weighted_csr(&path.join(adj_file), &self.adjacencies, &self.edge_lengths)?;
            let (k, h) = common::sha256_file(&adj_file, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // convex hulls
        if let Some(hulls) = &self.hulls {
            common::write_to_geoparquet(&path.join(hull_file), &hulls.iter()
                .map(|poly| MultiPolygon(vec![poly.clone()]))
                .collect()
            )?;
        }

        // geometries
        if let Some(geom) = &self.geoms {
            common::write_to_geoparquet(&path.join(geom_file), &geom.shapes())?;
            let (k, h) = common::sha256_file(geom_file, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        Ok(())
    }
}

impl Map {
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        let dirs = ["adj", "data", "geom", "hull"];
        common::ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for layer in self.get_layers() {
            layer.write_to_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        serde_json::to_writer_pretty(File::create(&meta_path)?, &manifest)?;

        Ok(())
    }
}
