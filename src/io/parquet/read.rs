//! Parquet reading operations.

use std::io::Cursor;

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::ParquetReader};

/// Read Parquet from bytes (WASM-friendly).
pub(crate) fn read_parquet_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    ParquetReader::new(cursor)
        .finish()
        .context("[io::parquet::read] Failed to read Parquet from bytes")
}
