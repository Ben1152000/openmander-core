use std::str::FromStr;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Pack file format for data and geometry storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PackFormat {
    /// Parquet format (requires parquet feature, not available for WASM)
    #[cfg_attr(feature = "parquet", default)]
    Parquet,
    /// PMTiles format for geometry storage (WASM-compatible, requires pmtiles feature)
    #[cfg_attr(not(feature = "parquet"), default)]
    Pmtiles,
}

impl PackFormat {
    /// Get file extension for data files
    pub fn data_extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Pmtiles => "csv",
        }
    }
}


impl FromStr for PackFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(PackFormat::Parquet),
            "pmtiles" => Ok(PackFormat::Pmtiles),
            _ => Err(anyhow!("Unknown pack format: {}. Expected 'parquet' or 'pmtiles'", s)),
        }
    }
}
