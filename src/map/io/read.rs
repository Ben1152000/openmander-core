use std::{path::Path};

use anyhow::{Context, Result};
use polars::frame::DataFrame;

use crate::{common, geom::Geometries, map::{GeoId, GeoType, Map, MapLayer, ParentRefs}, pack::{DiskPack, PackSource, PackFormat, PackFormats, Manifest, io::{csr, data, geometry}}};

impl MapLayer {
    /// Extract parent refs from the data DataFrame, returning (data, parents).
    fn unpack_data(&self, data: DataFrame) -> Result<(DataFrame, Vec<ParentRefs>)> {
        // split off final 5 columns of data
        let data_only = data.select_by_range(0..data.width() - 5)
            .with_context(|| format!("Expected at least 6 columns in data, got {}", data.width()))?;

        let parents = (0..data_only.height()).map(|i| {
            Ok(ParentRefs::new([
                data.column("parent_state").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::State, s))),
                data.column("parent_county").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::County, s))),
                data.column("parent_tract").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Tract, s))),
                data.column("parent_group").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::Group, s))),
                data.column("parent_vtd").ok()
                    .map(|c| c.str()).transpose()?
                    .and_then(|c| c.get(i).map(|s| GeoId::new(GeoType::VTD, s))),
            ]))
        }).collect::<Result<_>>()?;

        Ok((data_only, parents))
    }

    /// New API: read layer from any PackSource (disk/memory/http) using format information.
    fn read_from_pack_source_with_formats(&mut self, src: &dyn PackSource, formats: &PackFormats) -> Result<()> {
        let layer_name = self.ty().to_str();

        // Determine file extensions from format strings
        let adj_ext = match formats.adjacency.as_str() {
            "csr" => "csr.bin",
            _ => return Err(anyhow::anyhow!("Unsupported adjacency format: {}", formats.adjacency)),
        };
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
        let hull_ext = match formats.hull.as_str() {
            "wkb" => "wkb",
            #[cfg(feature = "parquet")]
            "geoparquet" => "geoparquet",
            _ => return Err(anyhow::anyhow!("Unsupported hull format: {}", formats.hull)),
        };

        let adj_file = format!("adj/{layer_name}.{adj_ext}");
        let data_file = format!("data/{layer_name}.{data_ext}");
        let geom_file = format!("geom/{layer_name}.{geom_ext}");
        let hull_file = format!("hull/{layer_name}.{hull_ext}");

        // data
        let data_bytes = src.get(&data_file)
            .with_context(|| format!("Failed to read data file: {}", data_file))?;
        let df = match formats.data.as_str() {
            #[cfg(feature = "parquet")]
            "parquet" => data::read_from_parquet_bytes(&data_bytes)
                .with_context(|| format!("Failed to parse parquet data file: {}", data_file))?,
            "csv" => data::read_from_csv_bytes(&data_bytes)
                .with_context(|| format!("Failed to parse CSV data file: {}", data_file))?,
            #[cfg(not(feature = "parquet"))]
            "parquet" => {
                return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data));
            }
        };
        (self.unit_data, self.parents) = self.unpack_data(df)
            .with_context(|| format!("Failed to unpack data for layer: {}", layer_name))?;

        // geo ids / index
        self.geo_ids = self.unit_data.column("geo_id")?.str()?.into_no_null_iter()
            .map(|val| GeoId::new(self.ty(), val))
            .collect();

        self.index = self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| (geo_id.clone(), i as u32))
            .collect();

        // adjacency
        (self.adjacencies, self.edge_lengths) = if self.ty() != GeoType::State {
            let adj_bytes = src.get(&adj_file)
                .with_context(|| format!("Failed to read adjacency file: {}", adj_file))?;
            match formats.adjacency.as_str() {
                "csr" => csr::read_from_weighted_csr_bytes(&adj_bytes)
                    .with_context(|| format!("Failed to parse adjacency file: {}", adj_file))?,
                _ => return Err(anyhow::anyhow!("Unsupported adjacency format: {}", formats.adjacency)),
            }
        } else { (vec![vec![]], vec![vec![]]) };

        // hulls (optional)
        if src.has(&hull_file) {
            let hull_bytes = src.get(&hull_file)?;
            let hulls = match formats.hull.as_str() {
                "wkb" => geometry::read_hulls_from_wkb_bytes(&hull_bytes)
                    .with_context(|| format!("Failed to read WKB hull file: {}", hull_file))?,
                #[cfg(feature = "parquet")]
                "geoparquet" => geometry::read_hulls_from_geoparquet_bytes(&hull_bytes)
                    .with_context(|| format!("Failed to read GeoParquet hull file: {}", hull_file))?,
                #[cfg(not(feature = "parquet"))]
                "geoparquet" => {
                    return Err(anyhow::anyhow!("GeoParquet format requires 'parquet' feature to be enabled"));
                }
                _ => return Err(anyhow::anyhow!("Unsupported hull format: {}", formats.hull)),
            };
            // Only create hulls if we have actual geometries
            // Empty hull vectors should result in None (hull file is optional)
            if !hulls.is_empty() {
                self.hulls = Some(hulls);
            }
        }

        self.construct_graph();

        // geometry (optional)
        if src.has(&geom_file) {
            let geom_bytes = src.get(&geom_file)?;
            let geoms = match formats.geometry.as_str() {
                #[cfg(feature = "parquet")]
                "geoparquet" => geometry::read_from_geoparquet_bytes(&geom_bytes)?,
                #[cfg(feature = "pmtiles")]
                "pmtiles" => geometry::read_from_pmtiles_bytes(&geom_bytes)?,
                #[cfg(not(feature = "parquet"))]
                "geoparquet" => {
                    return Err(anyhow::anyhow!("GeoParquet format requires 'parquet' feature to be enabled"));
                }
                #[cfg(not(feature = "pmtiles"))]
                "pmtiles" => {
                    return Err(anyhow::anyhow!("PMTiles format requires 'pmtiles' feature to be enabled"));
                }
                _ => {
                    return Err(anyhow::anyhow!("Unsupported geometry format: {}. Use 'geoparquet' or 'pmtiles'.", formats.geometry));
                }
            };
            // Only create Geometries if we have actual geometries
            // Empty geometry vectors should result in None (geometry file is optional)
            if !geoms.is_empty() {
                // Note: We don't pad geometries here because we don't know which feature IDs
                // are missing. The padding will be handled in to_geojson_with_districts
                // when we know the expected count and can properly index them.
                self.geoms = Some(Geometries::new(&geoms, None));
            }
        }

        Ok(())
    }
}

impl Map {
    /// Old API: read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        common::require_dir_exists(path)?;
        let src = DiskPack::new(path);
        
        // Try to read format from manifest first
        if src.has("manifest.json") {
            match Manifest::from_pack_source(&src) {
                Ok(manifest) => {
                    let manifest_formats = manifest.formats();
                    // Check if formats are using defaults (old manifest without formats field)
                    // If formats match defaults, detect from file extensions instead
                    let default_formats = PackFormats::default();
                    let is_using_defaults = manifest_formats.data == default_formats.data
                        && manifest_formats.geometry == default_formats.geometry
                        && manifest_formats.hull == default_formats.hull
                        && manifest_formats.adjacency == default_formats.adjacency;
                    
                    let formats = if is_using_defaults {
                        // Manifest has default formats (likely old manifest without formats field)
                        // Detect formats from actual file extensions
                        Self::detect_formats_from_files(&src)
                    } else {
                        // Manifest has explicit formats, use them
                        let mut formats = manifest_formats.clone();
                        // Still detect hull format for backward compatibility (in case it's missing)
                        if let Some(hull_format) = Self::detect_hull_format(&src) {
                            formats.hull = hull_format;
                        }
                        formats
                    };
                    
                    return Self::read_from_pack_source_with_formats(&src, &formats);
                }
                Err(_) => {
                    // If manifest parsing fails, fall back to detection
                }
            }
        }
        
        // Fall back to format detection for backward compatibility (no manifest or manifest parse failed)
        let formats = Self::detect_formats_from_files(&src);
        Self::read_from_pack_source_with_formats(&src, &formats)
    }

    /// Detect all formats from file extensions in the pack
    fn detect_formats_from_files(src: &dyn PackSource) -> PackFormats {
        let mut formats = PackFormats::default();
        
        // Detect data format
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let parquet_file = format!("data/{}.parquet", ty.to_str());
                if src.has(&parquet_file) {
                    formats.data = "parquet".to_string();
                    break;
                }
            }
        }
        // Check for CSV files
        if formats.data == "csv" {
            for ty in GeoType::ALL {
                let csv_file = format!("data/{}.csv", ty.to_str());
                if src.has(&csv_file) {
                    formats.data = "csv".to_string();
                    break;
                }
            }
        }
        // Default is CSV if no parquet found
        
        // Detect geometry format
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let geoparquet_file = format!("geom/{}.geoparquet", ty.to_str());
                if src.has(&geoparquet_file) {
                    formats.geometry = "geoparquet".to_string();
                    break;
                }
            }
        }
        #[cfg(feature = "pmtiles")]
        {
            for ty in GeoType::ALL {
                let pmtiles_file = format!("geom/{}.pmtiles", ty.to_str());
                if src.has(&pmtiles_file) {
                    formats.geometry = "pmtiles".to_string();
                    break;
                }
            }
        }
        // Default is pmtiles if no geoparquet found
        
        // Detect hull format
        if let Some(hull_format) = Self::detect_hull_format(src) {
            formats.hull = hull_format;
        }
        // Adjacency format is always "csr" (default)
        
        formats
    }

    /// Detect hull format by checking for .geoparquet or .wkb hull files
    fn detect_hull_format(src: &dyn PackSource) -> Option<String> {
        // Check for geoparquet hull files first (if parquet feature is enabled)
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let geoparquet_hull = format!("hull/{}.geoparquet", ty.to_str());
                if src.has(&geoparquet_hull) {
                    return Some("geoparquet".to_string());
                }
            }
        }
        // Check for wkb hull files (default)
        for ty in GeoType::ALL {
            let wkb_hull = format!("hull/{}.wkb", ty.to_str());
            if src.has(&wkb_hull) {
                return Some("wkb".to_string());
            }
        }
        None
    }

    /// Detect pack format by checking for parquet or pmtiles files
    pub fn detect_pack_format(src: &dyn PackSource) -> Result<PackFormat> {
        // Check for parquet files first (if parquet feature is enabled)
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let parquet_file = format!("data/{}.parquet", ty.to_str());
                if src.has(&parquet_file) {
                    return Ok(PackFormat::Parquet);
                }
            }
        }
        // Check for pmtiles files (if pmtiles feature is enabled)
        #[cfg(feature = "pmtiles")]
        {
            for ty in GeoType::ALL {
                let pmtiles_file = format!("geom/{}.pmtiles", ty.to_str());
                if src.has(&pmtiles_file) {
                    return Ok(PackFormat::Pmtiles);
                }
            }
        }
        // Check for CSV files (pmtiles format uses CSV for data)
        for ty in GeoType::ALL {
            let csv_file = format!("data/{}.csv", ty.to_str());
            if src.has(&csv_file) {
                return Ok(PackFormat::Pmtiles);
            }
        }
        // If no files found, return error with helpful message
        Err(anyhow::anyhow!(
            "No pack data files found. Expected files like 'data/block.parquet', 'data/block.csv', or 'geom/block.pmtiles'"
        ))
    }

    /// New API: read map from any PackSource using format information from manifest.
    pub(crate) fn read_from_pack_source_with_formats(src: &dyn PackSource, formats: &PackFormats) -> Result<Self> {
        let mut map = Self::default();

        // Determine data file extension from format
        let data_ext = match formats.data.as_str() {
            "parquet" => "parquet",
            "csv" => "csv",
            _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
        };

        for ty in GeoType::ALL {
            let mut layer = MapLayer::new(ty);

            // Check if data file exists for this layer
            let data_file = format!("data/{}.{}", ty.to_str(), data_ext);
            if !src.has(&data_file) { 
                continue;
            }

            // Load the layer - if it fails, return the error (don't silently skip)
            layer.read_from_pack_source_with_formats(src, formats)
                .with_context(|| format!("Failed to load layer {}", ty.to_str()))?;
            map.insert(layer);
        }

        // Enforce your current invariant: top + bottom must exist.
        // State is always the top layer for single-state packs.
        map.layer(GeoType::State)
            .ok_or_else(|| anyhow::anyhow!("Pack missing required top layer: state"))?;
        map.layer(GeoType::Block)
            .ok_or_else(|| anyhow::anyhow!("Pack missing required bottom layer: block"))?;

        Ok(map)
    }

    /// Legacy API: read map from any PackSource using a single PackFormat (for backward compatibility)
    pub fn read_from_pack_source(src: &dyn PackSource, format: PackFormat) -> Result<Self> {
        let formats = PackFormats::from_pack_format(format);
        Self::read_from_pack_source_with_formats(src, &formats)
    }
}
