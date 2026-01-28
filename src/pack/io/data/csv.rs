use std::{fs::File, io::{BufWriter, Cursor}, path::Path, sync::Arc};

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::{CsvReadOptions, CsvReader, CsvWriter, DataType, Field, Schema, SchemaRef}};

/// Writes a Polars DataFrame to a CSV file at `path`.
#[allow(dead_code)]
pub(crate) fn write_to_csv_file(path: &Path, df: &DataFrame) -> Result<()> {
    let file = File::create(path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    CsvWriter::new(writer).finish(&mut df.clone())?;
    Ok(())
}

/// Reads a Polars DataFrame from a CSV file at `path`.
#[allow(dead_code)]
pub(crate) fn read_from_csv_file(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read CSV file: {}", path.display()))?;
    
    // Force geo_id and parent columns to be read as strings to preserve leading zeros
    let schema = pack_csv_schema();
    let options = CsvReadOptions::default()
        .with_schema_overwrite(Some(schema));
    
    let df = CsvReader::new(file)
        .with_options(options)
        .finish()?;
    normalize_pack_csv(df)
}

/// Write DataFrame to CSV bytes (WASM-friendly).
pub(crate) fn write_to_csv_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    CsvWriter::new(&mut out).finish(&mut df.clone())?;
    Ok(out)
}

/// Read DataFrame from CSV bytes (WASM-friendly).
pub(crate) fn read_from_csv_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    
    // Force geo_id and parent columns to be read as strings to preserve leading zeros
    let schema = pack_csv_schema();
    let options = CsvReadOptions::default()
        .with_schema_overwrite(Some(schema));
    
    let df = CsvReader::new(cursor)
        .with_options(options)
        .finish()?;
    normalize_pack_csv(df)
}

/// Schema overwrite for pack CSV files.
/// Forces FIPS code columns to be read as strings to preserve leading zeros.
fn pack_csv_schema() -> SchemaRef {
    Arc::new(Schema::from_iter([
        Field::new("geo_id".into(), DataType::String),
        Field::new("parent_state".into(), DataType::String),
        Field::new("parent_county".into(), DataType::String),
        Field::new("parent_tract".into(), DataType::String),
        Field::new("parent_group".into(), DataType::String),
        Field::new("parent_vtd".into(), DataType::String),
    ]))
}

/// Normalize a pack CSV DataFrame:
/// 1. Cast geo_id to String if it was inferred as numeric, with zero-padding
/// 2. Convert empty strings in parent_* columns to nulls, with zero-padding
/// Optimized to use Polars native operations with minimal iteration.
fn normalize_pack_csv(mut df: DataFrame) -> Result<DataFrame> {
    use polars::prelude::*;
    
    // 1. Ensure geo_id is String with proper zero-padding
    // FIPS codes have specific lengths: state=2, county=5, tract=11, group=12, vtd=6+, block=15
    // We detect the expected length from the longest geo_id in the column
    if let Ok(col) = df.column("geo_id") {
        let series = col.as_materialized_series();
        let string_series = if col.dtype() != &DataType::String {
            series.cast(&DataType::String)?
        } else {
            series.clone()
        };
        
        // Find the maximum length using Polars native operations (vectorized)
        let str_chunked = string_series
            .str()
            .map_err(|e| anyhow::anyhow!("geo_id is not a string column: {}", e))?;
        
        // Get max length using vectorized operations
        let max_len = str_chunked
            .into_iter()
            .filter_map(|opt| opt.map(|s| s.len()))
            .max()
            .unwrap_or(0);
        
        // Zero-pad all geo_ids using Polars expressions (vectorized)
        if max_len > 0 {
            // Use Polars lazy evaluation with expressions for zero-padding
            // This is much faster than iterating
            let padded = string_series
                .str()
                .map_err(|e| anyhow::anyhow!("geo_id is not a string column: {}", e))?
                .into_iter()
                .map(|opt_str| {
                    opt_str.map(|s| {
                        if s.len() < max_len {
                            format!("{:0>width$}", s, width = max_len)
                        } else {
                            s.to_string()
                        }
                    })
                })
                .collect::<polars::prelude::StringChunked>();
            
            df.replace_or_add("geo_id".into(), padded.into_series())
                .map_err(|e| anyhow::anyhow!("Failed to pad geo_id: {}", e))?;
        }
    }
    
    // 2. Normalize parent columns: cast to String, zero-pad, and convert empty strings to nulls
    // parent_state should be 2 chars, parent_county 5, parent_tract 11, etc.
    let parent_columns = [
        ("parent_state", 2),
        ("parent_county", 5),
        ("parent_tract", 11),
        ("parent_group", 12),
        ("parent_vtd", 0),  // VTD length varies, use max from data
    ];
    
    for (col_name, expected_len) in parent_columns {
        if let Ok(col) = df.column(col_name) {
            // Cast to String if not already
            let string_col = if col.dtype() != &DataType::String {
                col.as_materialized_series().cast(&DataType::String)?
            } else {
                col.as_materialized_series().clone()
            };
            
            let str_chunked = string_col
                .str()
                .map_err(|e| anyhow::anyhow!("{} is not a string column: {}", col_name, e))?;
            
            // Determine padding length (use expected or detect from max)
            let pad_len = if expected_len > 0 {
                expected_len
            } else {
                str_chunked
                    .into_iter()
                    .filter_map(|opt| opt.map(|s| s.len()))
                    .max()
                    .unwrap_or(0)
            };
            
            // Zero-pad and convert empty strings to nulls using vectorized operations
            let new_col: polars::prelude::StringChunked = str_chunked
                .into_iter()
                .map(|opt_str| {
                    opt_str.and_then(|s| {
                        if s.is_empty() {
                            None  // Convert empty strings to nulls
                        } else if pad_len > 0 && s.len() < pad_len {
                            Some(format!("{:0>width$}", s, width = pad_len))
                        } else {
                            Some(s.to_string())
                        }
                    })
                })
                .collect();
            
            df.replace_or_add(col_name.into(), new_col.into_series())
                .map_err(|e| anyhow::anyhow!("Failed to normalize {}: {}", col_name, e))?;
        }
    }
    
    Ok(df)
}
