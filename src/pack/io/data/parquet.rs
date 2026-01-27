use std::io::Cursor;

use anyhow::Result;
use polars::{frame::DataFrame, io::SerReader, prelude::{ParquetReader, ParquetWriter}};

/// Write Parquet into bytes (WASM-friendly).
pub(crate) fn write_to_parquet_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    ParquetWriter::new(&mut out).finish(&mut df.clone())?;
    Ok(out)
}

/// Read Parquet from bytes (WASM-friendly).
pub(crate) fn read_from_parquet_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    Ok(ParquetReader::new(cursor).finish()?)
}
