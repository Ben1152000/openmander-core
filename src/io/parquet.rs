//! Parquet reading/writing using Polars (gated behind `parquet` feature).
//!
//! The public interface mirrors the pack-CSV reader/writer: both return/accept
//! `ParsedPackCsv` so `map/io/read.rs` and `map/io/write.rs` can use a uniform
//! dispatch.

use std::io::Cursor;

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::{Column, ParquetReader, ParquetWriter}};

use crate::{
    graph::WeightMatrix,
    io::csv::pack::{ParsedPackCsv, PARENT_COL_TYPES},
    map::{GeoId, ParentRefs},
};

/// Read Parquet bytes into a `ParsedPackCsv`.
pub(crate) fn read_parquet_bytes(bytes: &[u8]) -> Result<ParsedPackCsv> {
    let df = ParquetReader::new(Cursor::new(bytes))
        .finish()
        .context("[io::parquet::read] Failed to read Parquet from bytes")?;

    let n = df.height();

    // Extract geo_ids.
    let geo_ids: Vec<String> = df.column("geo_id")
        .context("parquet missing 'geo_id' column")?
        .str()
        .context("'geo_id' column is not a string")?
        .into_no_null_iter()
        .map(|s| s.to_string())
        .collect();

    // Extract unit_names.
    let unit_names: Vec<String> = if let Ok(col) = df.column("name") {
        col.str()
            .map(|s| s.into_no_null_iter().map(|s| s.to_string()).collect())
            .unwrap_or_else(|_| vec![String::new(); n])
    } else {
        vec![String::new(); n]
    };

    // Extract parent refs.
    let mut parents = vec![ParentRefs::default(); n];
    for &(col_name, ty) in PARENT_COL_TYPES {
        if let Ok(col) = df.column(col_name) {
            if let Ok(str_col) = col.str() {
                for (i, val) in str_col.into_iter().enumerate() {
                    if let Some(s) = val {
                        if !s.is_empty() {
                            parents[i].set(ty, Some(GeoId::new(ty, s)));
                        }
                    }
                }
            }
        }
    }

    // Build WeightMatrix from all non-string columns (skips idx, geo_id, name, parent_*).
    let weights = WeightMatrix::from_dataframe(&df);

    Ok(ParsedPackCsv { geo_ids, unit_names, parents, weights })
}

/// Write a layer to Parquet bytes.
pub(crate) fn write_parquet_bytes(
    geo_ids:    &[GeoId],
    unit_names: &[String],
    parents:    &[ParentRefs],
    weights:    &WeightMatrix,
) -> Result<Vec<u8>> {
    let n = geo_ids.len();
    let i64_names = weights.int_series_names();
    let f64_names = weights.f64_series_names();

    let mut cols: Vec<Column> = vec![
        Column::new(
            "geo_id".into(),
            geo_ids.iter().map(|g| g.id()).collect::<Vec<_>>(),
        ),
        Column::new(
            "name".into(),
            unit_names.to_vec(),
        ),
    ];

    // i64 columns.
    for (col_idx, &name) in i64_names.iter().enumerate() {
        let values: Vec<i64> = (0..n).map(|i| weights.int_row(i)[col_idx] as i64).collect();
        cols.push(Column::new(name.into(), values));
    }

    // f64 columns.
    for (col_idx, &name) in f64_names.iter().enumerate() {
        let values: Vec<f64> = (0..n).map(|i| weights.f64_row(i)[col_idx]).collect();
        cols.push(Column::new(name.into(), values));
    }

    // Parent columns (Optional<String>).
    for &(col_name, ty) in PARENT_COL_TYPES {
        let values: Vec<Option<String>> = (0..n)
            .map(|i| parents.get(i).and_then(|p| p.get(ty)).map(|g| g.id().to_string()))
            .collect();
        cols.push(Column::new(col_name.into(), values));
    }

    let mut df = DataFrame::new(cols)
        .context("[io::parquet::write] Failed to build DataFrame")?;

    let mut out = Vec::new();
    ParquetWriter::new(&mut out)
        .finish(&mut df)
        .context("[io::parquet::write] Failed to write Parquet to bytes")?;

    Ok(out)
}
