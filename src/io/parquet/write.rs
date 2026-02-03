//! Parquet writing operations.

use anyhow::{Context, Result};
use polars::{frame::DataFrame, prelude::ParquetWriter};

/// Write Parquet into bytes (WASM-friendly).
pub(crate) fn write_parquet_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    ParquetWriter::new(&mut out)
        .finish(&mut df.clone())
        .context("[io::parquet::write] Failed to write Parquet to bytes")?;
    Ok(out)
}
