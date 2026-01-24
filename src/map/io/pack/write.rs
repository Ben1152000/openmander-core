use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Context, Result};
use geo::MultiPolygon;
use polars::{df, frame::DataFrame, prelude::DataFrameJoinOps};

use crate::{common, map::{GeoType, Map, MapLayer, ParentRefs}, pack::{DiskPack, FileHash, Manifest, PackSink, PackFormat}};

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

        Ok(self.unit_data
            .inner_join(&parents_df, ["geo_id"], ["geo_id"])
            .context("inner_join on 'geo_id' failed when preparing parquet")?)
    }

    fn write_to_pack_sink(
        &self,
        sink: &mut dyn PackSink,
        format: PackFormat,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>,
    ) -> Result<()> {
        let layer_name = self.ty().to_str();
        let adj_file = format!("adj/{layer_name}.csr.bin");
        let data_file = format!("data/{}.{}", layer_name, format.data_extension());
        let geom_file = format!("geom/{}.{}", layer_name, format.geometry_extension());
        let hull_file = format!("hull/{}.{}", layer_name, format.geometry_extension());

        counts.insert(layer_name.into(), self.geo_ids.len());

        // entities -> data bytes (parquet or json)
        let data_bytes = match format {
            #[cfg(feature = "parquet")]
            PackFormat::Parquet => common::write_to_parquet_bytes(&mut self.pack_data()?)?,
            PackFormat::Json => common::write_to_json_bytes(&mut self.pack_data()?)?,
            #[cfg(not(feature = "parquet"))]
            PackFormat::Parquet => {
                return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
            }
        };
        sink.put(&data_file, &data_bytes)?;
        // hash the bytes (instead of reading from disk)
        hashes.insert(
            data_file.clone(),
            FileHash {
                sha256: common::sha256_bytes(&data_bytes),
            },
        );

        // adjacency (CSR)
        if self.ty() != GeoType::State {
            let adj_bytes = common::write_to_weighted_csr_bytes(&self.adjacencies, &self.edge_lengths)?;
            sink.put(&adj_file, &adj_bytes)?;
            hashes.insert(
                adj_file.clone(),
                FileHash {
                    sha256: common::sha256_bytes(&adj_bytes),
                },
            );
        }

        // convex hulls (optional)
        if let Some(hulls) = &self.hulls {
            let hull_mps = hulls.iter()
                .map(|poly| MultiPolygon(vec![poly.clone()]))
                .collect::<Vec<_>>();
            let hull_bytes = match format {
                #[cfg(feature = "parquet")]
                PackFormat::Parquet => common::write_to_geoparquet_bytes(&hull_mps)?,
                PackFormat::Json => common::write_to_geojson_bytes(&hull_mps)?,
                #[cfg(not(feature = "parquet"))]
                PackFormat::Parquet => {
                    return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
                }
            };
            sink.put(&hull_file, &hull_bytes)?;
            hashes.insert(
                hull_file.clone(),
                FileHash {
                    sha256: common::sha256_bytes(&hull_bytes),
                },
            );
        }

        // geometries (optional)
        if let Some(geom) = &self.geoms {
            let geom_bytes = match format {
                #[cfg(feature = "parquet")]
                PackFormat::Parquet => common::write_to_geoparquet_bytes(&geom.shapes())?,
                PackFormat::Json => common::write_to_geojson_bytes(&geom.shapes())?,
                #[cfg(not(feature = "parquet"))]
                PackFormat::Parquet => {
                    return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
                }
            };
            sink.put(&geom_file, &geom_bytes)?;
            hashes.insert(
                geom_file.clone(),
                FileHash {
                    sha256: common::sha256_bytes(&geom_bytes),
                },
            );
        }

        Ok(())
    }

    /// Old path-based helper (kept for compatibility).
    fn write_to_pack(
        &self,
        path: &Path,
        format: PackFormat,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>,
    ) -> Result<()> {
        let mut sink = DiskPack::new(path);
        self.write_to_pack_sink(&mut sink, format, counts, hashes)
    }
}

impl Map {
    /// Old API: write pack to disk directory (uses default format).
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        self.write_to_pack_with_format(path, PackFormat::default())
    }

    /// Write pack to disk directory with specified format.
    pub fn write_to_pack_with_format(&self, path: &Path, format: PackFormat) -> Result<()> {
        let dirs = ["adj", "data", "geom", "hull"];
        common::ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for layer in self.layers_iter() {
            layer.write_to_pack(path, format, &mut counts, &mut file_hashes)?;
        }

        // Manifest (disk): keep your current behavior
        let meta_path = path.join("manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        serde_json::to_writer_pretty(File::create(&meta_path)?, &manifest)?;

        Ok(())
    }

    /// New API: write pack into any sink (memory, disk, etc.) with default format.
    pub fn write_to_pack_sink(&self, sink: &mut dyn PackSink, pack_root_for_manifest: &Path) -> Result<()> {
        self.write_to_pack_sink_with_format(sink, pack_root_for_manifest, PackFormat::default())
    }

    /// New API: write pack into any sink (memory, disk, etc.) with specified format.
    pub fn write_to_pack_sink_with_format(&self, sink: &mut dyn PackSink, pack_root_for_manifest: &Path, format: PackFormat) -> Result<()> {
        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for layer in self.layers_iter() {
            layer.write_to_pack_sink(sink, format, &mut counts, &mut file_hashes)?;
        }

        // Manifest: you currently call Manifest::new(path, ...).
        // Keep that as-is by passing a "virtual root" for relative keys.
        // If Manifest::new reads from disk, you'll want a Manifest::new_in_memory variant.
        let manifest = Manifest::new(pack_root_for_manifest, counts, file_hashes);
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;

        Ok(())
    }
}
