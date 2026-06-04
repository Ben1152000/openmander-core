use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::map::GeoType;
use super::{PackFormat, PackSource};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct FileHash {
    pub sha256: String,
}

/// Format specification for pack data files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct PackFormats {
    /// Format for data files (e.g., "csv", "parquet")
    pub data: String,
}

impl Default for PackFormats {
    fn default() -> Self {
        Self { data: "csv".to_string() }
    }
}

impl PackFormats {
    pub(crate) fn from_pack_format(format: PackFormat) -> Self {
        Self {
            data: match format {
                PackFormat::Parquet => "parquet".to_string(),
                PackFormat::Pmtiles => "csv".to_string(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub(crate) struct Bounds {
    pub west:  f64,
    pub south: f64,
    pub east:  f64,
    pub north: f64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Manifest {
    pack_id: String,
    /// Full state name, e.g. "Colorado".
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// Two-digit FIPS code, e.g. "08".
    #[serde(skip_serializing_if = "Option::is_none")]
    fips: Option<String>,
    version: String,
    crs: String,
    #[serde(default)]
    formats: PackFormats,
    /// Bounding box in WGS84 degrees, derived from the state layer geometry at pack-build time.
    #[serde(skip_serializing_if = "Option::is_none")]
    bounds: Option<Bounds>,
    levels: Vec<String>,
    counts: BTreeMap<String, usize>,
    files: BTreeMap<String, FileHash>,
}

impl Manifest {
    pub(crate) fn new(
        path: &Path,
        counts: BTreeMap<&'static str, usize>,
        files: BTreeMap<String, FileHash>,
        formats: PackFormats,
        bounds: Option<Bounds>,
        name: Option<String>,
        fips: Option<String>,
    ) -> Self {
        Self {
            pack_id: path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown-pack")
                .to_string(),
            name,
            fips,
            version: "2".into(),
            crs: "EPSG:4269".into(),
            formats,
            bounds,
            levels: GeoType::ALL.iter().map(|ty| ty.to_str().into()).collect(),
            counts: counts.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            files,
        }
    }

    pub(crate) fn formats(&self) -> &PackFormats {
        &self.formats
    }

    /// Read manifest from a PackSource
    pub(crate) fn from_pack_source(src: &dyn PackSource) -> Result<Self> {
        let manifest_bytes = src.get("manifest.json")
            .context("Failed to read manifest.json")?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
            .context("Failed to parse manifest.json")?;
        Ok(manifest)
    }
}
