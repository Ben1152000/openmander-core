use std::{fs::File, io::{BufWriter, Cursor}, path::Path};

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::{ParquetReader, ParquetWriter}};

/// Writes a Polars DataFrame to a Parquet file at `path`.
pub(crate) fn write_to_parquet(path: &Path, df: &DataFrame) -> Result<()> {
    let file = File::create(path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut df.clone())?;
    Ok(())
}

/// Reads a Polars DataFrame from a Parquet file at `path`.
pub(crate) fn read_from_parquet(path: &Path) -> Result<DataFrame> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to read parquet file: {}", path.display()))?;
    Ok(ParquetReader::new(&mut file).finish()?)
}

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
