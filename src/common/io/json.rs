use std::{fs::File, io::{BufReader, BufWriter, Cursor}, path::Path};

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::JsonWriter};

/// Writes a Polars DataFrame to a JSON file at `path`.
pub(crate) fn write_to_json_file(path: &Path, df: &DataFrame) -> Result<()> {
    let file = File::create(path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    JsonWriter::new(writer).finish(&mut df.clone())?;
    Ok(())
}

/// Reads a Polars DataFrame from a JSON file at `path`.
pub(crate) fn read_from_json_file(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read JSON file: {}", path.display()))?;
    let reader = BufReader::new(file);
    Ok(polars::io::json::JsonReader::new(reader).finish()?)
}

/// Write DataFrame to JSON bytes (WASM-friendly).
/// Uses JSON format that JsonReader can parse.
pub(crate) fn write_to_json_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    // Use JsonLines format which is more reliable for roundtrip
    JsonWriter::new(&mut out)
        .with_json_format(polars::io::json::JsonFormat::JsonLines)
        .finish(&mut df.clone())?;
    Ok(out)
}

/// Read DataFrame from JSON bytes (WASM-friendly).
/// Handles null columns by casting them to String if they match parent_* pattern.
pub(crate) fn read_from_json_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    // Use JsonLines format to match what we write
    let mut df = polars::io::json::JsonReader::new(cursor)
        .with_json_format(polars::io::json::JsonFormat::JsonLines)
        .finish()?;
    
    // Fix null columns that should be String type (e.g., parent_state, parent_county, etc.)
    // When JSON has all null values, Polars infers null type, but we need String
    let parent_columns = ["parent_state", "parent_county", "parent_tract", "parent_group", "parent_vtd"];
    for col_name in parent_columns {
        if let Ok(col) = df.column(col_name) {
            if col.dtype() == &polars::prelude::DataType::Null {
                // Cast null column to String with null values
                let len = col.len();
                let null_series = polars::prelude::Series::new_null(col_name.into(), len);
                let string_col = null_series.cast(&polars::prelude::DataType::String)?;
                df.replace_or_add(col_name.into(), string_col).map_err(|e| anyhow::anyhow!("Failed to cast {} to String: {}", col_name, e))?;
            }
        }
    }
    
    Ok(df)
}

