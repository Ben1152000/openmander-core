use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::{
    map::{GeoType, Map, MapLayer, util},
    map::pack::{Bounds, DiskPack, FileHash, Manifest, PackSink, PackFormat, PackFormats},
};
#[cfg(feature = "pmtiles")]
use crate::io::pmtiles::{PmtilesLayer, write_to_pmtiles_bytes};

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
    /// Look up the full state name and FIPS code from `state_abbr`, if set.
    fn state_name_and_fips(&self) -> (Option<String>, Option<String>) {
        let Some(abbr) = &self.state_abbr else { return (None, None) };
        let name = util::state_abbr_to_name(abbr).map(|s| s.to_string());
        let fips = util::state_abbr_to_fips(abbr).map(|s| s.to_string());
        (name, fips)
    }

    /// Compute the bounding box from the state layer geometry.
    ///
    /// Handles antimeridian-crossing states (e.g. Alaska): if the naive longitude
    /// spread exceeds 180°, positive longitudes are normalized by subtracting 360°
    /// so they become values west of -180° (e.g. 173°E → -187°). MapLibre's
    /// fitBounds accepts out-of-range longitudes and handles this correctly.
    pub(crate) fn compute_bounds(&self) -> Option<Bounds> {
        let state_layer = self.layers_iter().find(|l| l.ty() == GeoType::State)?;
        let mut lons: Vec<f64> = Vec::new();
        let mut min_lat = f64::INFINITY;
        let mut max_lat = f64::NEG_INFINITY;

        for unit in state_layer.region.unit_ids() {
            for poly in &state_layer.region.geometry(unit).0 {
                for coord in poly.exterior().coords() {
                    if coord.x.is_finite() && coord.y.is_finite() {
                        lons.push(coord.x);
                        min_lat = min_lat.min(coord.y);
                        max_lat = max_lat.max(coord.y);
                    }
                }
            }
        }

        if lons.is_empty() { return None; }

        let mut min_lon = lons.iter().cloned().fold(f64::INFINITY, f64::min);
        let mut max_lon = lons.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Antimeridian correction: a spread > 180° indicates the state crosses
        // the antimeridian. Normalize by shifting positive longitudes west of -180°.
        if max_lon - min_lon > 180.0 {
            for lon in &mut lons {
                if *lon > 0.0 { *lon -= 360.0; }
            }
            min_lon = lons.iter().cloned().fold(f64::INFINITY, f64::min);
            max_lon = lons.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        }

        let r = |v: f64| (v * 1e12).round() / 1e12;
        Some(Bounds { west: r(min_lon), south: r(min_lat), east: r(max_lon), north: r(max_lat) })
    }

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

        let (name, fips) = self.state_name_and_fips();
        let manifest = Manifest::new(pack_root_for_manifest, counts, file_hashes, formats, self.compute_bounds(), name, fips);
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

        let mut pmtiles_layers: Vec<PmtilesLayer<'_>> = Vec::new();

        for layer in self.layers_iter() {
            let region = &*layer.region;
            if region.num_units() > 0 {
                let (min_zoom, max_zoom) = pmtiles_zoom_range_for_layer(layer.ty());
                pmtiles_layers.push(PmtilesLayer {
                    name: layer.ty().to_str(),
                    region,
                    min_zoom,
                    max_zoom,
                });
            }
        }

        // High-latitude states (above the Arctic Circle, ~66.5°N) have extremely
        // complex coastal/tundra blocks that cause OOM at high zoom levels.
        // Cap all layer max zooms to 12 for such states.
        const POLAR_CIRCLE_LAT: f64 = 66.5;
        const HIGH_LAT_MAX_ZOOM: u8 = 12;

        let (state_min_lat, state_max_lat) = pmtiles_layers.iter()
            .find(|l| l.name == "state")
            .map(|l| {
                l.region.unit_ids()
                    .flat_map(|unit| {
                        l.region.geometry(unit).0.iter()
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
                 — capping PMTiles max zoom to {HIGH_LAT_MAX_ZOOM} for all layers"
            );
            for layer in pmtiles_layers.iter_mut() {
                layer.max_zoom = layer.max_zoom.min(HIGH_LAT_MAX_ZOOM);
            }
        }

        if !pmtiles_layers.is_empty() {
            let geom_file = "geom/geometries.pmtiles";
            let geom_bytes = write_to_pmtiles_bytes(pmtiles_layers)?;
            sink.put(geom_file, &geom_bytes)?;
            file_hashes.insert(geom_file.to_string(), FileHash { sha256: sha256_bytes(&geom_bytes) });
        }

        let (name, fips) = self.state_name_and_fips();
        let manifest = Manifest::new(
            pack_root_for_manifest,
            (*counts).clone(),
            (*file_hashes).clone(),
            (*formats).clone(),
            self.compute_bounds(),
            name,
            fips,
        );
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;

        Ok(())
    }
}
