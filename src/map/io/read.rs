use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use polars::frame::DataFrame;

use crate::{
    graph::WeightMatrix,
    map::{GeoId, GeoType, Map, MapLayer, ParentRefs, util},
    map::pack::{DiskPack, PackSource, PackFormat, PackFormats, Manifest},
};

/// Extract parent refs from the data DataFrame, returning (data, parents).
fn unpack_layer_data(data: DataFrame, _ty: GeoType) -> Result<(DataFrame, Vec<ParentRefs>)> {
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

/// Read layer from any PackSource (disk/memory/http) using format information.
fn read_layer_from_pack_source_with_formats(
    ty: GeoType,
    src: &dyn PackSource,
    formats: &PackFormats
) -> Result<MapLayer> {
    let layer_name = ty.to_str();

    // Determine file extension from format string
    let data_ext = match formats.data.as_str() {
        "parquet" => "parquet",
        "csv" => "csv",
        _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
    };

    let data_file = format!("data/{layer_name}.{data_ext}");

    // data
    let data_bytes = src.get(&data_file)
        .with_context(|| format!("Failed to read data file: {}", data_file))?;
    let df = match formats.data.as_str() {
        #[cfg(feature = "parquet")]
        "parquet" => crate::io::parquet::read_parquet_bytes(&data_bytes)
            .with_context(|| format!("Failed to parse parquet data file: {}", data_file))?,
        "csv" => crate::io::csv::read_csv_bytes(&data_bytes)
            .with_context(|| format!("Failed to parse CSV data file: {}", data_file))?,
        #[cfg(not(feature = "parquet"))]
        "parquet" => {
            return Err(anyhow::anyhow!("Parquet format requires 'parquet' feature to be enabled"));
        }
        _ => {
            return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data));
        }
    };

    let (unit_data, parents) = unpack_layer_data(df, ty)
        .with_context(|| format!("Failed to unpack data for layer: {}", layer_name))?;

    // geo ids / index
    let geo_ids: Vec<GeoId> = unit_data.column("geo_id")?.str()?.into_no_null_iter()
        .map(|val| GeoId::new(ty, val))
        .collect();

    let index = geo_ids.iter().enumerate()
        .map(|(i, geo_id)| (geo_id.clone(), i as u32))
        .collect();

    // region — required (geom/{layer_name}.region.gz or legacy .region)
    let region_file = if src.has(&format!("geom/{layer_name}.region.gz")) {
        format!("geom/{layer_name}.region.gz")
    } else {
        format!("geom/{layer_name}.region")
    };
    let region_bytes = src.get(&region_file)
        .with_context(|| format!("Pack missing required region file: {}", region_file))?;
    // Auto-detect: gzip magic = [1f 8b], raw geograph magic = b"OMRP"
    let region = if region_bytes.starts_with(&[0x1f, 0x8b]) {
        let mut gz = flate2::read::GzDecoder::new(region_bytes.as_ref());
        geograph::io::read(&mut gz)
    } else {
        geograph::io::read(&mut region_bytes.as_ref())
    }.map_err(|e| anyhow::anyhow!("Failed to deserialize region for {layer_name}: {e:?}"))?;

    let unit_weights = Arc::new(WeightMatrix::from_dataframe(&unit_data));
    Ok(MapLayer::new(ty, geo_ids, index, parents, unit_data, unit_weights, Arc::new(region)))
}

/// Detect the data format from file extensions in the pack.
fn detect_formats_from_files(src: &dyn PackSource) -> PackFormats {
    #[cfg(feature = "parquet")]
    for ty in GeoType::ALL {
        if src.has(&format!("data/{}.parquet", ty.to_str())) {
            return PackFormats { data: "parquet".to_string() };
        }
    }
    PackFormats::default() // CSV
}

/// Read map from any PackSource using format information from manifest.
fn read_map_from_pack_source_with_formats(src: &dyn PackSource, formats: &PackFormats) -> Result<Map> {
    let mut map = Map::default();

    // Determine data file extension from format
    let data_ext = match formats.data.as_str() {
        "parquet" => "parquet",
        "csv" => "csv",
        _ => return Err(anyhow::anyhow!("Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data)),
    };

    for ty in GeoType::ALL {
        // Check if data file exists for this layer
        let data_file = format!("data/{}.{}", ty.to_str(), data_ext);
        if !src.has(&data_file) {
            continue;
        }

        // Load the layer - if it fails, return the error (don't silently skip)
        let layer = read_layer_from_pack_source_with_formats(ty, src, formats)
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

impl Map {
    /// Detect the format of a pack by inspecting its files.
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

    /// Read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        util::require_dir_exists(path)?;
        let src = DiskPack::new(path);
        
        // Try to read format from manifest first
        if src.has("manifest.json") {
            match Manifest::from_pack_source(&src) {
                Ok(manifest) => {
                    let manifest_formats = manifest.formats();
                    // If the manifest reports the default data format, detect from file extensions
                    // instead in case this is an old manifest without an explicit formats field.
                    let formats = if manifest_formats.data == PackFormats::default().data {
                        detect_formats_from_files(&src)
                    } else {
                        manifest_formats.clone()
                    };
                    return read_map_from_pack_source_with_formats(&src, &formats);
                }
                Err(_) => {
                    // If manifest parsing fails, fall back to detection
                }
            }
        }
        
        // Fall back to format detection for backward compatibility (no manifest or manifest parse failed)
        let formats = detect_formats_from_files(&src);
        read_map_from_pack_source_with_formats(&src, &formats)
    }

    /// Read a map from any [`PackSource`] with the specified format.
    pub fn read_from_pack_source(src: &dyn PackSource, format: PackFormat) -> Result<Self> {
        let formats = PackFormats::from_pack_format(format);
        read_map_from_pack_source_with_formats(src, &formats)
    }
}
