use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};

use crate::{
    map::{GeoId, GeoType, Map, MapLayer, util},
    map::pack::{DiskPack, PackSource, PackFormat, PackFormats, Manifest},
};

/// Read a layer from any PackSource using format information.
fn read_layer_from_pack_source_with_formats(
    ty: GeoType,
    src: &dyn PackSource,
    formats: &PackFormats,
) -> Result<MapLayer> {
    let layer_name = ty.to_str();

    let data_ext = match formats.data.as_str() {
        "parquet" => "parquet",
        "csv"     => "csv",
        _ => return Err(anyhow::anyhow!(
            "Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data
        )),
    };

    let data_file = format!("data/{layer_name}.{data_ext}");
    let data_bytes = src.get(&data_file)
        .with_context(|| format!("Failed to read data file: {}", data_file))?;

    let parsed = match formats.data.as_str() {
        "csv" => crate::io::csv::pack::read_pack_csv(&data_bytes)
            .with_context(|| format!("Failed to parse CSV data file: {}", data_file))?,
        #[cfg(feature = "parquet")]
        "parquet" => crate::io::parquet::read_parquet_bytes(&data_bytes)
            .with_context(|| format!("Failed to parse Parquet data file: {}", data_file))?,
        #[cfg(not(feature = "parquet"))]
        "parquet" => return Err(anyhow::anyhow!(
            "Parquet format requires the 'parquet' feature to be enabled"
        )),
        _ => return Err(anyhow::anyhow!(
            "Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data
        )),
    };

    let geo_ids: Vec<GeoId> = parsed.geo_ids.iter()
        .map(|s| GeoId::new(ty, s))
        .collect();
    let index = geo_ids.iter().enumerate()
        .map(|(i, geo_id)| (geo_id.clone(), i as u32))
        .collect();
    let unit_names   = parsed.unit_names;
    let parents      = parsed.parents;
    let unit_weights = Arc::new(parsed.weights);

    // region — required (geom/{layer_name}.region.gz or legacy .region)
    let region_file = if src.has(&format!("geom/{layer_name}.region.gz")) {
        format!("geom/{layer_name}.region.gz")
    } else {
        format!("geom/{layer_name}.region")
    };
    let region = {
        let mut stream = src.open_read(&region_file)
            .with_context(|| format!("Pack missing required region file: {}", region_file))?;
        geograph::io::read(&mut stream)
    }.map_err(|e| anyhow::anyhow!(
        "Failed to deserialize region for {layer_name}: {e:?}"
    ))?;

    Ok(MapLayer::new(ty, geo_ids, index, parents, unit_names, unit_weights, Arc::new(region)))
}

/// Detect the data format from file extensions in the pack.
fn detect_formats_from_files(_src: &dyn PackSource) -> PackFormats {
    #[cfg(feature = "parquet")]
    for ty in GeoType::ALL {
        if _src.has(&format!("data/{}.parquet", ty.to_str())) {
            return PackFormats { data: "parquet".to_string() };
        }
    }
    PackFormats::default() // CSV
}

/// Read map from any PackSource using format information from manifest.
fn read_map_from_pack_source_with_formats(src: &dyn PackSource, formats: &PackFormats) -> Result<Map> {
    let mut map = Map::default();

    let data_ext = match formats.data.as_str() {
        "parquet" => "parquet",
        "csv"     => "csv",
        _ => return Err(anyhow::anyhow!(
            "Unsupported data format: {}. Use 'parquet' or 'csv'.", formats.data
        )),
    };

    for ty in GeoType::ALL {
        let data_file = format!("data/{}.{}", ty.to_str(), data_ext);
        if !src.has(&data_file) {
            continue;
        }
        let layer = read_layer_from_pack_source_with_formats(ty, src, formats)
            .with_context(|| format!("Failed to load layer {}", ty.to_str()))?;
        map.insert(layer);
    }

    map.layer(GeoType::State)
        .ok_or_else(|| anyhow::anyhow!("Pack missing required top layer: state"))?;
    map.layer(GeoType::Block)
        .ok_or_else(|| anyhow::anyhow!("Pack missing required bottom layer: block"))?;

    Ok(map)
}

impl Map {
    /// Detect the format of a pack by inspecting its files.
    pub fn detect_pack_format(src: &dyn PackSource) -> Result<PackFormat> {
        #[cfg(feature = "parquet")]
        {
            for ty in GeoType::ALL {
                let parquet_file = format!("data/{}.parquet", ty.to_str());
                if src.has(&parquet_file) {
                    return Ok(PackFormat::Parquet);
                }
            }
        }
        #[cfg(feature = "pmtiles")]
        {
            for ty in GeoType::ALL {
                let pmtiles_file = format!("geom/{}.pmtiles", ty.to_str());
                if src.has(&pmtiles_file) {
                    return Ok(PackFormat::Pmtiles);
                }
            }
        }
        for ty in GeoType::ALL {
            let csv_file = format!("data/{}.csv", ty.to_str());
            if src.has(&csv_file) {
                return Ok(PackFormat::Pmtiles);
            }
        }
        Err(anyhow::anyhow!(
            "No pack data files found. Expected files like 'data/block.parquet', \
             'data/block.csv', or 'geom/block.pmtiles'"
        ))
    }

    /// Read a map from a pack directory at `path`.
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        util::require_dir_exists(path)?;
        let src = DiskPack::new(path);

        if src.has("manifest.json") && let Ok(manifest) = Manifest::from_pack_source(&src) {
            let manifest_formats = manifest.formats();
            let formats = if manifest_formats.data == PackFormats::default().data {
                detect_formats_from_files(&src)
            } else {
                manifest_formats.clone()
            };
            return read_map_from_pack_source_with_formats(&src, &formats);
        }

        let formats = detect_formats_from_files(&src);
        read_map_from_pack_source_with_formats(&src, &formats)
    }

    /// Read a map from any [`PackSource`] with the specified format.
    pub fn read_from_pack_source(src: &dyn PackSource, format: PackFormat) -> Result<Self> {
        let formats = PackFormats::from_pack_format(format);
        read_map_from_pack_source_with_formats(src, &formats)
    }
}
