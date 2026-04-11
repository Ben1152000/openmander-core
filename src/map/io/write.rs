use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::{
    map::{GeoType, Map, MapLayer, util},
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
        GeoType::State  =>  (4, 14),
        GeoType::County =>  (4, 10),
        GeoType::Tract  =>  (4, 12),
        GeoType::VTD    =>  (4, 14),
        GeoType::Group  =>  (4, 14),
        GeoType::Block  => (10, 14),
    }
}

impl MapLayer {
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
            "csv"     => "csv",
            _ => return Err(anyhow::anyhow!(
                "Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data
            )),
        };
        let data_file = format!("data/{layer_name}.{data_ext}");

        counts.insert(layer_name, self.geo_ids.len());

        let data_bytes = match formats.data.as_str() {
            "csv" => crate::io::csv::pack::write_pack_csv(
                &self.geo_ids, &self.unit_names, &self.parents, &self.unit_weights,
            )?,
            #[cfg(feature = "parquet")]
            "parquet" => crate::io::parquet::write_parquet_bytes(
                &self.geo_ids, &self.unit_names, &self.parents, &self.unit_weights,
            )?,
            #[cfg(not(feature = "parquet"))]
            "parquet" => return Err(anyhow::anyhow!(
                "Parquet format requires the 'parquet' feature to be enabled"
            )),
            _ => return Err(anyhow::anyhow!(
                "Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data
            )),
        };
        sink.put(&data_file, &data_bytes)?;
        hashes.insert(data_file, FileHash { sha256: sha256_bytes(&data_bytes) });

        // region — geom/{layer_name}.region.gz
        let region_file = format!("geom/{layer_name}.region.gz");
        let mut region_bytes: Vec<u8> = Vec::new();
        {
            let mut gz = flate2::write::GzEncoder::new(&mut region_bytes, flate2::Compression::best());
            geograph::io::write(&self.region, &mut gz)
                .map_err(|e| anyhow::anyhow!(
                    "Failed to serialize region for {layer_name}: {e:?}"
                ))?;
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
    pub fn write_to_pack_sink_with_format(
        &self,
        sink: &mut dyn PackSink,
        pack_root_for_manifest: &Path,
        format: PackFormat,
    ) -> Result<()> {
        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        let formats = PackFormats::from_pack_format(format);

        #[cfg(feature = "pmtiles")]
        if format == PackFormat::Pmtiles {
            return self.write_to_pack_sink_with_multilayer_pmtiles(
                sink, pack_root_for_manifest, &formats, &mut counts, &mut file_hashes,
            );
        }

        for layer in self.layers_iter() {
            layer.write_to_pack_sink_with_formats(sink, &formats, &mut counts, &mut file_hashes)?;
        }

        let manifest = Manifest::new(pack_root_for_manifest, counts, file_hashes, formats);
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;

        Ok(())
    }

    #[cfg(feature = "pmtiles")]
    fn write_to_pack_sink_with_multilayer_pmtiles(
        &self,
        sink: &mut dyn PackSink,
        pack_root_for_manifest: &Path,
        formats: &PackFormats,
        counts: &mut BTreeMap<&'static str, usize>,
        file_hashes: &mut BTreeMap<String, FileHash>,
    ) -> Result<()> {
        for layer in self.layers_iter() {
            let layer_name = layer.ty().to_str();
            let data_file = format!("data/{layer_name}.csv");

            counts.insert(layer_name, layer.geo_ids.len());

            let data_bytes = crate::io::csv::pack::write_pack_csv(
                &layer.geo_ids, &layer.unit_names, &layer.parents, &layer.unit_weights,
            )?;
            sink.put(&data_file, &data_bytes)?;
            file_hashes.insert(data_file.clone(), FileHash { sha256: sha256_bytes(&data_bytes) });

            let region_file = format!("geom/{layer_name}.region.gz");
            let mut region_bytes: Vec<u8> = Vec::new();
            {
                let mut gz = flate2::write::GzEncoder::new(&mut region_bytes, flate2::Compression::best());
                geograph::io::write(&layer.region, &mut gz)
                    .map_err(|e| anyhow::anyhow!(
                        "Failed to serialize region for {layer_name}: {e:?}"
                    ))?;
                gz.finish().context("Failed to finish gzip encoding for region")?;
            }
            sink.put(&region_file, &region_bytes)?;
            file_hashes.insert(region_file, FileHash { sha256: sha256_bytes(&region_bytes) });
        }

        let mut geo_id_vecs: Vec<Vec<String>> = Vec::new();
        let mut layer_info: Vec<(&str, &geograph::Region, u8, u8, usize)> = Vec::new();

        for layer in self.layers_iter() {
            let region = &*layer.region;
            if region.num_units() > 0 {
                let layer_name = layer.ty().to_str();
                let (min_zoom, max_zoom) = pmtiles_zoom_range_for_layer(layer.ty());
                let geo_ids: Vec<String> = layer.geo_ids.iter()
                    .map(|g| g.id().to_string())
                    .collect();
                let idx = geo_id_vecs.len();
                geo_id_vecs.push(geo_ids);
                layer_info.push((layer_name, region, min_zoom, max_zoom, idx));
            }
        }

        // High-latitude states (above the Arctic Circle, ~66.5°N) have extremely
        // complex coastal/tundra blocks that cause OOM at high zoom levels.
        // Cap all layer max zooms to 12 for such states.
        const POLAR_CIRCLE_LAT: f64 = 66.5;
        const HIGH_LAT_MAX_ZOOM: u8 = 12;

        let (state_min_lat, state_max_lat) = layer_info.iter()
            .find(|(name, ..)| *name == "state")
            .map(|(_, region, ..)| {
                region.unit_ids()
                    .flat_map(|unit| {
                        region.geometry(unit).0.iter()
                            .flat_map(|poly| poly.exterior().coords().map(|c| c.y))
                            .collect::<Vec<_>>()
                    })
                    .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), y| (mn.min(y), mx.max(y)))
            })
            .unwrap_or((0.0, 0.0));

        if state_max_lat > POLAR_CIRCLE_LAT || state_min_lat < -POLAR_CIRCLE_LAT {
            let desc = if state_max_lat > POLAR_CIRCLE_LAT {
                format!("above the Arctic Circle ({state_max_lat:.1}°N)")
            } else {
                format!("below the Antarctic Circle ({state_min_lat:.1}°S)")
            };
            eprintln!(
                "[write_pack] state extends {desc} \
                 — capping PMTiles max zoom to {HIGH_LAT_MAX_ZOOM} and raising min zoom by 2 for all layers"
            );
            for (_, _, min_zoom, max_zoom, _) in layer_info.iter_mut() {
                *max_zoom = (*max_zoom).min(HIGH_LAT_MAX_ZOOM);
                *min_zoom = (*min_zoom + 2).max(4);
            }
        }

        let pmtiles_layers: Vec<(&str, &geograph::Region, Option<&[String]>, u8, u8)> = layer_info.iter()
            .map(|(name, region, min_zoom, max_zoom, idx)| {
                (*name, *region, Some(geo_id_vecs[*idx].as_slice()), *min_zoom, *max_zoom)
            })
            .collect();

        if !pmtiles_layers.is_empty() {
            let geom_file = "geom/geometries.pmtiles";
            let geom_bytes = crate::io::pmtiles::write_to_pmtiles_bytes(pmtiles_layers)?;
            sink.put(geom_file, &geom_bytes)?;
            file_hashes.insert(geom_file.to_string(), FileHash { sha256: sha256_bytes(&geom_bytes) });
        }

        let manifest = Manifest::new(
            pack_root_for_manifest,
            (*counts).clone(),
            (*file_hashes).clone(),
            (*formats).clone(),
        );
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;

        Ok(())
    }
}
