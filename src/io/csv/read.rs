//! CSV reading operations.

use std::{fs::File, io::Cursor, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use polars::{frame::DataFrame, io::SerReader, prelude::{CsvReadOptions, CsvReader, DataType, Field, Schema, SchemaRef}};

use crate::map::{GeoId, GeoType, MapLayer};

/// Reads a CSV file from `path` into a Polars DataFrame.
pub(crate) fn read_csv(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("[io::csv::read] Failed to open CSV file: {}", path.display()))?;
    CsvReader::new(file)
        .finish()
        .with_context(|| format!("[io::csv::read] Failed to read CSV from {:?}", path))
}

/// Reads a CSV from a string (for WASM/browser use).
pub(crate) fn read_csv_string(csv: &str) -> Result<DataFrame> {
    CsvReader::new(Cursor::new(csv.as_bytes()))
        .finish()
        .with_context(|| "[io::csv::read] Failed to read CSV from string")
}

/// Read DataFrame from CSV bytes (for pack reading).
pub(crate) fn read_csv_bytes(bytes: &[u8]) -> Result<DataFrame> {
    let cursor = Cursor::new(bytes);
    
    // Force geo_id and parent columns to be read as strings to preserve leading zeros
    let schema = pack_csv_schema();
    let options = CsvReadOptions::default()
        .with_schema_overwrite(Some(schema));
    
    let df = CsvReader::new(cursor)
        .with_options(options)
        .finish()
        .context("[io::csv::read] Failed to read CSV from bytes")?;
    normalize_pack_csv(df)
}

/// Reads a pipe-delimited `.txt` file with a header row into a Polars DataFrame.
pub(crate) fn read_pipe_delimited_txt(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("[io::csv::read] Failed to open pipe-delimited file: {}", path.display()))?;
    CsvReadOptions::default()
        .with_has_header(true)
        .map_parse_options(|po| po.with_separator(b'|'))
        .with_infer_schema_length(Some(0))
        .into_reader_with_file_handle(file)
        .finish()
        .with_context(|| format!("[io::csv::read] Failed to read pipe-delimited file from {:?}", path))
}

/// Read plan assignments from a CSV DataFrame.
/// 
/// The DataFrame should have two columns: geo_id (string) and district (u32).
/// Returns a vector of (GeoId, district) tuples.
pub(crate) fn read_plan_assignments(df: DataFrame, block_layer: &MapLayer) -> Result<Vec<(GeoId, u32)>> {
    // Ensure CSV has the correct number of rows and columns
    ensure!(df.width() >= 2, "[io::csv::read] CSV must have two columns: geo_id,district");
    ensure!(df.height() == block_layer.len(), "[io::csv::read] CSV has {} rows, expected {}", df.height(), block_layer.len());

    // Parse assignments from CSV
    let blocks = df.column(df.get_column_names()[0])?.cast(&DataType::String)?;
    let districts = df.column(df.get_column_names()[1])?.cast(&DataType::UInt32)?;

    blocks.str()?.into_no_null_iter()
        .zip(districts.u32()?.into_no_null_iter())
        .map(|(block, district)| {
            let geo_id = GeoId::new(GeoType::Block, block);
            ensure!(block_layer.geo_ids().contains(&geo_id), "[io::csv::read] GeoId {} in CSV not found in map", geo_id.id());
            Ok((geo_id, district))
        })
        .collect()
}

/// Schema overwrite for pack CSV files.
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

/// Normalize a pack CSV DataFrame.
fn normalize_pack_csv(mut df: DataFrame) -> Result<DataFrame> {
    use polars::prelude::*;
    
    // Ensure geo_id is String with proper zero-padding
    if let Ok(col) = df.column("geo_id") {
        let series = col.as_materialized_series();
        let string_series = if col.dtype() != &DataType::String {
            series.cast(&DataType::String)?
        } else {
            series.clone()
        };
        
        let str_chunked = string_series.str()
            .map_err(|e| anyhow::anyhow!("geo_id is not a string column: {}", e))?;
        
        let max_len = str_chunked.into_iter()
            .filter_map(|opt| opt.map(|s| s.len()))
            .max()
            .unwrap_or(0);
        
        if max_len > 0 {
            let padded = string_series.str()
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
                .collect::<StringChunked>();
            
            df.replace_or_add("geo_id".into(), padded.into_series())
                .map_err(|e| anyhow::anyhow!("Failed to pad geo_id: {}", e))?;
        }
    }
    
    // Normalize parent columns
    let parent_columns = [
        ("parent_state", 2),
        ("parent_county", 5),
        ("parent_tract", 11),
        ("parent_group", 12),
        ("parent_vtd", 0),
    ];
    
    for (col_name, expected_len) in parent_columns {
        if let Ok(col) = df.column(col_name) {
            let string_col = if col.dtype() != &DataType::String {
                col.as_materialized_series().cast(&DataType::String)?
            } else {
                col.as_materialized_series().clone()
            };
            
            let str_chunked = string_col.str()
                .map_err(|e| anyhow::anyhow!("{} is not a string column: {}", col_name, e))?;
            
            let pad_len = if expected_len > 0 {
                expected_len
            } else {
                str_chunked.into_iter()
                    .filter_map(|opt| opt.map(|s| s.len()))
                    .max()
                    .unwrap_or(0)
            };
            
            let new_col: StringChunked = str_chunked.into_iter()
                .map(|opt_str| {
                    opt_str.and_then(|s| {
                        if s.is_empty() {
                            None
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
