use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use polars::{df, frame::DataFrame, prelude::DataFrameJoinOps};
use geo::MultiPolygon;

use crate::{common, map::{GeoType, Map, MapLayer, ParentRefs}, pack::{DiskPack, FileHash, Manifest, PackSink, PackFormat, PackFormats}};

/// Get the recommended PMTiles zoom range for a given layer type.
fn pmtiles_zoom_range_for_layer(ty: GeoType) -> (u8, u8) {
    match ty {
        GeoType::State => (4, 8),
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
        let adj_file = format!("adj/{layer_name}.csr.bin");
        
        // Determine file extensions from formats
        let data_ext = match formats.data.as_str() {
            "parquet" => "parquet",
            "csv" => "csv",
            _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
        };
        let geom_ext = match formats.geometry.as_str() {
            "geoparquet" => "geoparquet",
            "pmtiles" => "pmtiles",
            _ => return Err(anyhow::anyhow!("Unsupported geometry format: {}. Use 'geoparquet' or 'pmtiles'.", formats.geometry)),
        };
        
        let data_file = format!("data/{layer_name}.{data_ext}");
        let geom_file = format!("geom/{layer_name}.{geom_ext}");
        let hull_ext = match formats.hull.as_str() {
            "geoparquet" => "geoparquet",
            _ => "wkb",
        };
        let hull_file = format!("hull/{layer_name}.{hull_ext}");

        counts.insert(layer_name.into(), self.geo_ids.len());

        // entities -> data bytes (parquet or csv)
        let data_bytes = match formats.data.as_str() {
            #[cfg(feature = "parquet")]
            "parquet" => crate::io::parquet::write_parquet_bytes(&mut self.pack_data()?)?,
            "csv" => crate::io::csv::write_csv_bytes(&mut self.pack_data()?)?,
            #[cfg(not(feature = "parquet"))]
            "parquet" => {
                return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data));
            }
        };
        sink.put(&data_file, &data_bytes)?;
        hashes.insert(
            data_file.clone(),
            FileHash {
                sha256: common::sha256_bytes(&data_bytes),
            },
        );

        // adjacency (CSR)
        if self.ty() != GeoType::State {
            let adj_bytes = crate::io::csr::write_weighted_csr_bytes(&self.adjacencies, &self.edge_lengths)?;
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
            if !hulls.is_empty() {
                let hull_bytes = match formats.hull.as_str() {
                    #[cfg(feature = "parquet")]
                    "geoparquet" => crate::io::geoparquet::write_hulls_to_geoparquet_bytes(hulls)?,
                    "wkb" => crate::io::wkb::write_hulls_to_wkb_bytes(hulls, true)?,
                    #[cfg(not(feature = "parquet"))]
                    "geoparquet" => {
                        return Err(anyhow::anyhow!("GeoParquet hull format requires 'parquet' feature to be enabled"));
                    }
                    _ => {
                        return Err(anyhow::anyhow!("Unsupported hull format: {}. Use 'geoparquet' or 'wkb'.", formats.hull));
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
        }

        // geometries (optional)
        if let Some(geom) = &self.geoms {
            let shapes = geom.shapes();
            if !shapes.is_empty() {
                let geom_bytes: Vec<u8> = match formats.geometry.as_str() {
                    #[cfg(feature = "parquet")]
                    "geoparquet" => crate::io::geoparquet::write_geoparquet_bytes(shapes)?,
                    #[cfg(feature = "pmtiles")]
                    "pmtiles" => {
                        let (min_zoom, max_zoom) = pmtiles_zoom_range_for_layer(self.ty());
                        let geo_ids: Vec<String> = self.geo_ids.iter()
                            .map(|g| g.id().to_string())
                            .collect();
                        crate::io::pmtiles::write_to_pmtiles_bytes(shapes, Some(&geo_ids), min_zoom, max_zoom)?
                    },
                    _ => {
                        #[cfg(not(feature = "parquet"))]
                        if formats.geometry == "geoparquet" {
                            return Err(anyhow::anyhow!("GeoParquet format requires 'parquet' feature to be enabled"));
                        }
                        #[cfg(not(feature = "pmtiles"))]
                        if formats.geometry == "pmtiles" {
                            return Err(anyhow::anyhow!("PMTiles format requires 'pmtiles' feature to be enabled"));
                        }
                        return Err(anyhow::anyhow!("Unsupported geometry format: {}. Use 'geoparquet' or 'pmtiles'.", formats.geometry));
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
        }

        Ok(())
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

        let mut sink = DiskPack::new(path);
        self.write_to_pack_sink_with_format(&mut sink, path, format)?;

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
        // Write data, adjacency, and hull files for each layer
        for layer in self.layers_iter() {
            let layer_name = layer.ty().to_str();
            let adj_file = format!("adj/{layer_name}.csr.bin");
            let data_file = format!("data/{layer_name}.csv");
            let hull_file = format!("hull/{layer_name}.wkb");
            
            counts.insert(layer_name.into(), layer.geo_ids.len());
            
            // Write data file
            let data_bytes = crate::io::csv::write_csv_bytes(&mut layer.pack_data()?)?;
            sink.put(&data_file, &data_bytes)?;
            file_hashes.insert(data_file.clone(), FileHash { sha256: common::sha256_bytes(&data_bytes) });
            
            // Write adjacency file (skip for state layer)
            if layer.ty() != GeoType::State {
                let adj_bytes = crate::io::csr::write_weighted_csr_bytes(&layer.adjacencies, &layer.edge_lengths)?;
                sink.put(&adj_file, &adj_bytes)?;
                file_hashes.insert(adj_file.clone(), FileHash { sha256: common::sha256_bytes(&adj_bytes) });
            }
            
            // Write hull file (if exists)
            if let Some(hulls) = &layer.hulls {
                if !hulls.is_empty() {
                    let hull_bytes = crate::io::wkb::write_hulls_to_wkb_bytes(hulls, true)?;
                    sink.put(&hull_file, &hull_bytes)?;
                    file_hashes.insert(hull_file.clone(), FileHash { sha256: common::sha256_bytes(&hull_bytes) });
                }
            }
        }
        
        // Write individual per-layer PMTiles and collect layers for multi-layer file
        let mut geo_id_vecs: Vec<Vec<String>> = Vec::new();
        let mut layer_info: Vec<(&str, &[MultiPolygon<f64>], u8, u8, usize)> = Vec::new();

        for layer in self.layers_iter() {
            if let Some(geom) = &layer.geoms {
                let shapes = geom.shapes();
                if !shapes.is_empty() {
                    let layer_name = layer.ty().to_str();
                    let (min_zoom, max_zoom) = pmtiles_zoom_range_for_layer(layer.ty());
                    let geo_ids: Vec<String> = layer.geo_ids.iter()
                        .map(|g| g.id().to_string())
                        .collect();

                    // Write individual layer PMTiles
                    let layer_geom_file = format!("geom/{layer_name}.pmtiles");
                    let layer_geom_bytes = crate::io::pmtiles::write_to_pmtiles_bytes(
                        shapes, Some(&geo_ids), min_zoom, max_zoom,
                    )?;
                    sink.put(&layer_geom_file, &layer_geom_bytes)?;
                    file_hashes.insert(layer_geom_file, FileHash {
                        sha256: common::sha256_bytes(&layer_geom_bytes),
                    });

                    let idx = geo_id_vecs.len();
                    geo_id_vecs.push(geo_ids);
                    layer_info.push((layer_name, shapes, min_zoom, max_zoom, idx));
                }
            }
        }
        
        // Build the pmtiles_layers vec with references
        let pmtiles_layers: Vec<(&str, &[MultiPolygon<f64>], Option<&[String]>, u8, u8)> = layer_info.iter()
            .map(|(name, shapes, min_zoom, max_zoom, idx)| {
                (*name, *shapes, Some(geo_id_vecs[*idx].as_slice()), *min_zoom, *max_zoom)
            })
            .collect();
        
        // Write single multi-layer PMTiles file
        if !pmtiles_layers.is_empty() {
            let geom_file = "geom/geometries.pmtiles";
            let geom_bytes = crate::io::pmtiles::write_multilayer_pmtiles_bytes(pmtiles_layers)?;
            sink.put(geom_file, &geom_bytes)?;
            file_hashes.insert(geom_file.to_string(), FileHash { sha256: common::sha256_bytes(&geom_bytes) });
        }
        
        // Create manifest
        let manifest = Manifest::new(pack_root_for_manifest, (*counts).clone(), (*file_hashes).clone(), (*formats).clone());
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        sink.put("manifest.json", &manifest_bytes)?;
        
        Ok(())
    }
}
