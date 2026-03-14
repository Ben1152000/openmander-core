use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use polars::{df, frame::DataFrame, prelude::DataFrameJoinOps};
use geo::MultiPolygon;
use sha2::{Digest, Sha256};

use crate::{
    map::{GeoType, Map, MapLayer, ParentRefs, util},
    map::pack::{DiskPack, FileHash, Manifest, PackSink, PackFormat, PackFormats},
};

/// Computes the SHA-256 hash of the given bytes and returns it as a hex string.
fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Get the recommended PMTiles zoom range for a given layer type.
fn pmtiles_zoom_range_for_layer(ty: GeoType) -> (u8, u8) {
    match ty {
        GeoType::State => (4, 14),
        GeoType::County => (4, 10),
        GeoType::Tract => (8, 12),
        GeoType::VTD => (4, 12),  // Start at 4 to enable preloading
        GeoType::Group => (8, 12),
        GeoType::Block => (12, 14),
    }
}

impl MapLayer {
    /// Prepare entity data (with parent refs) for writing to a pack file.
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

    fn write_to_pack_sink_with_formats(
        &self,
        sink: &mut dyn PackSink,
        formats: &PackFormats,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>,
    ) -> Result<()> {
        let layer_name = self.ty().to_str();

        let data_ext = match formats.data.as_str() {
            "parquet" => "parquet",
            "csv" => "csv",
            _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
        };
        let data_file = format!("data/{layer_name}.{data_ext}");

        counts.insert(layer_name.into(), self.geo_ids.len());

        // data (parquet or csv)
        let data_bytes = match formats.data.as_str() {
            #[cfg(feature = "parquet")]
            "parquet" => crate::io::parquet::write_parquet_bytes(&mut self.pack_data()?)?,
            "csv" => crate::io::csv::write_csv_bytes(&mut self.pack_data()?)?,
            #[cfg(not(feature = "parquet"))]
            "parquet" => return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled")),
            _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
        };
        sink.put(&data_file, &data_bytes)?;
        hashes.insert(data_file, FileHash { sha256: sha256_bytes(&data_bytes) });

        // region — geom/{layer_name}.region.gz
        let region_file = format!("geom/{layer_name}.region.gz");
        let mut region_bytes: Vec<u8> = Vec::new();
        {
            let mut gz = flate2::write::GzEncoder::new(&mut region_bytes, flate2::Compression::best());
            geograph::io::write(&*self.region, &mut gz)
                .map_err(|e| anyhow::anyhow!("Failed to serialize region for {layer_name}: {e:?}"))?;
            gz.finish().context("Failed to finish gzip encoding for region")?;
        }
        sink.put(&region_file, &region_bytes)?;
        hashes.insert(region_file, FileHash { sha256: sha256_bytes(&region_bytes) });

        Ok(())
    }
}

impl Map {
    /// Write pack to disk directory using the default format.
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        self.write_to_pack_with_format(path, PackFormat::default())
    }

    /// Write pack to disk directory with the specified format.
    pub fn write_to_pack_with_format(&self, path: &Path, format: PackFormat) -> Result<()> {
        for dir in ["data", "geom"] {
            util::ensure_dir_exists(&path.join(dir))?;
        }

        let mut sink = DiskPack::new(path);
        self.write_to_pack_sink_with_format(&mut sink, path, format)?;

        Ok(())
    }

    /// Write pack into any [`PackSink`] using the default format.
    pub fn write_to_pack_sink(&self, sink: &mut dyn PackSink, pack_root_for_manifest: &Path) -> Result<()> {
        self.write_to_pack_sink_with_format(sink, pack_root_for_manifest, PackFormat::default())
    }

    /// Write pack into any [`PackSink`] with the specified format.
    pub fn write_to_pack_sink_with_format(&self, sink: &mut dyn PackSink, pack_root_for_manifest: &Path, format: PackFormat) -> Result<()> {
        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        let formats = PackFormats::from_pack_format(format);
        
        // Special handling for PMTiles: write all layers to a single file
        #[cfg(feature = "pmtiles")]
        if format == PackFormat::Pmtiles {
            return self.write_to_pack_sink_with_multilayer_pmtiles(sink, pack_root_for_manifest, &formats, &mut counts, &mut file_hashes);
        }
        
        for layer in self.layers_iter() {
            layer.write_to_pack_sink_with_formats(sink, &formats, &mut counts, &mut file_hashes)?;
        }

        // Create manifest with format information
        let manifest = Manifest::new(pack_root_for_manifest, counts, file_hashes, formats);
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;

        Ok(())
    }
    
    /// Write all layers to a single PMTiles file (geom/geometries.pmtiles)
    #[cfg(feature = "pmtiles")]
    fn write_to_pack_sink_with_multilayer_pmtiles(
        &self,
        sink: &mut dyn PackSink,
        pack_root_for_manifest: &Path,
        formats: &PackFormats,
        counts: &mut BTreeMap<&'static str, usize>,
        file_hashes: &mut BTreeMap<String, FileHash>,
    ) -> Result<()> {
        // Write data and region files for each layer
        for layer in self.layers_iter() {
            let layer_name = layer.ty().to_str();
            let data_file = format!("data/{layer_name}.csv");

            counts.insert(layer_name.into(), layer.geo_ids.len());

            // Write data file
            let data_bytes = crate::io::csv::write_csv_bytes(&mut layer.pack_data()?)?;
            sink.put(&data_file, &data_bytes)?;
            file_hashes.insert(data_file.clone(), FileHash { sha256: sha256_bytes(&data_bytes) });

            // Write region file
            let region_file = format!("geom/{layer_name}.region.gz");
            let mut region_bytes: Vec<u8> = Vec::new();
            {
                let mut gz = flate2::write::GzEncoder::new(&mut region_bytes, flate2::Compression::best());
                geograph::io::write(&*layer.region, &mut gz)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize region for {layer_name}: {e:?}"))?;
                gz.finish().context("Failed to finish gzip encoding for region")?;
            }
            sink.put(&region_file, &region_bytes)?;
            file_hashes.insert(region_file, FileHash { sha256: sha256_bytes(&region_bytes) });
        }

        // Collect all layers for the combined multi-layer PMTiles file
        let mut geo_id_vecs: Vec<Vec<String>> = Vec::new();
        let mut layer_info: Vec<(&str, Vec<MultiPolygon<f64>>, u8, u8, usize)> = Vec::new();

        for layer in self.layers_iter() {
            let region = &*layer.region;
            let shapes: Vec<MultiPolygon<f64>> = region.unit_ids()
                .map(|u| region.geometry(u).clone())
                .collect();
            if !shapes.is_empty() {
                let layer_name = layer.ty().to_str();
                let (min_zoom, max_zoom) = pmtiles_zoom_range_for_layer(layer.ty());
                let geo_ids: Vec<String> = layer.geo_ids.iter()
                    .map(|g| g.id().to_string())
                    .collect();
                let idx = geo_id_vecs.len();
                geo_id_vecs.push(geo_ids);
                layer_info.push((layer_name, shapes, min_zoom, max_zoom, idx));
            }
        }

        // Build the pmtiles_layers vec with references into the owned shapes
        let pmtiles_layers: Vec<(&str, &[MultiPolygon<f64>], Option<&[String]>, u8, u8)> = layer_info.iter()
            .map(|(name, shapes, min_zoom, max_zoom, idx)| {
                (*name, shapes.as_slice(), Some(geo_id_vecs[*idx].as_slice()), *min_zoom, *max_zoom)
            })
            .collect();
        
        // Write single multi-layer PMTiles file
        if !pmtiles_layers.is_empty() {
            let geom_file = "geom/geometries.pmtiles";
            let geom_bytes = crate::io::pmtiles::write_to_pmtiles_bytes(pmtiles_layers)?;
            sink.put(geom_file, &geom_bytes)?;
            file_hashes.insert(geom_file.to_string(), FileHash { sha256: sha256_bytes(&geom_bytes) });
        }
        
        // Create manifest
        let manifest = Manifest::new(pack_root_for_manifest, (*counts).clone(), (*file_hashes).clone(), (*formats).clone());
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;
        
        Ok(())
    }
}
