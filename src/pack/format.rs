use std::str::FromStr;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Pack file format for data and geometry storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PackFormat {
    /// Parquet format (requires parquet feature, not available for WASM)
    Parquet,
    /// GeoJSON format with CSV data (WASM-compatible)
    GeoJson,
    /// PMTiles format for geometry storage (WASM-compatible, requires pmtiles feature)
    Pmtiles,
}

impl PackFormat {
    /// Default format (parquet if available, otherwise geojson)
    pub fn default() -> Self {
        #[cfg(feature = "parquet")]
        {
            Self::Parquet
        }
        #[cfg(not(feature = "parquet"))]
        {
            Self::GeoJson
        }
    }

    /// Get file extension for data files
    pub fn data_extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::GeoJson => "csv",
            Self::Pmtiles => "csv",
        }
    }

    /// Get file extension for geometry files
    pub fn geometry_extension(&self) -> &'static str {
        match self {
            Self::Parquet => "geoparquet",
            Self::GeoJson => "geojson",
            Self::Pmtiles => "pmtiles",
        }
    }

    /// Get file extension for hull files (always WKB, regardless of format)
    pub fn hull_extension(&self) -> &'static str {
        "wkb"
    }
}

impl Default for PackFormat {
    fn default() -> Self {
        Self::default()
    }
}

impl FromStr for PackFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(PackFormat::Parquet),
            "geojson" | "json" => Ok(PackFormat::GeoJson),  // Accept both for compatibility
            "pmtiles" => Ok(PackFormat::Pmtiles),
            _ => Err(anyhow!("Unknown pack format: {}. Expected 'parquet', 'geojson', or 'pmtiles'", s)),
        }
    }
}
