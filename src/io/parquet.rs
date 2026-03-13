//! Parquet reading/writing operations.

use std::io::Cursor;

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::{ParquetReader, ParquetWriter}};

/// Read Parquet from bytes (WASM-friendly).
pub(crate) fn read_parquet_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    ParquetReader::new(cursor)
        .finish()
        .context("[io::parquet::read] Failed to read Parquet from bytes")
}

/// Write Parquet into bytes (WASM-friendly).
pub(crate) fn write_parquet_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    ParquetWriter::new(&mut out)
        .finish(&mut df.clone())
        .context("[io::parquet::write] Failed to write Parquet to bytes")?;
    Ok(out)
}
