use std::{fs::File, io::BufWriter, path::Path};

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::{ParquetReader, ParquetWriter}};

/// Writes a Polars DataFrame to a Parquet file at `path`.
pub fn write_to_parquet(path: &Path, df: &DataFrame) -> Result<()> {
    let file = File::create(&path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut df.clone())?;
    Ok(())
}

pub fn read_from_parquet(path: &Path) -> Result<DataFrame> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to read parquet file: {}", path.display()))?;
    Ok(ParquetReader::new(&mut file).finish()?)
}
