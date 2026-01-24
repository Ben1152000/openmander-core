use std::str::FromStr;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Pack file format for data and geometry storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PackFormat {
    /// Parquet format (requires parquet feature, not available for WASM)
    Parquet,
    /// JSON format (WASM-compatible, no compression)
    Json,
}

impl PackFormat {
    /// Default format (parquet if available, otherwise json)
    pub fn default() -> Self {
        #[cfg(feature = "parquet")]
        {
            Self::Parquet
        }
        #[cfg(not(feature = "parquet"))]
        {
            Self::Json
        }
    }

    /// Get file extension for data files
    pub fn data_extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Json => "json",
        }
    }

    /// Get file extension for geometry files
    pub fn geometry_extension(&self) -> &'static str {
        match self {
            Self::Parquet => "geoparquet",
            Self::Json => "geojson",
        }
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
            "json" => Ok(PackFormat::Json),
            _ => Err(anyhow!("Unknown pack format: {}. Expected 'parquet' or 'json'", s)),
        }
    }
}
