//! CSV writing operations.

use std::{fs::File, path::Path};

use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerWriter, prelude::{CsvWriter, NamedFrom}, series::Series};

use crate::map::GeoId;

/// Write a DataFrame to a CSV file.
pub(crate) fn write_csv(df: &mut DataFrame, path: &Path) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("[io::csv::write] Failed to create CSV file: {}", path.display()))?;
    CsvWriter::new(file)
        .finish(df)
        .with_context(|| format!("[io::csv::write] Failed to write CSV to {:?}", path))
}

/// Write a DataFrame to CSV bytes (for pack writing).
pub(crate) fn write_csv_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    CsvWriter::new(&mut out)
        .finish(&mut df.clone())
        .context("[io::csv::write] Failed to write CSV to bytes")?;
    Ok(out)
}

/// Write a DataFrame to a CSV string (for WASM/browser use).
pub(crate) fn write_csv_string(df: &mut DataFrame) -> Result<String> {
    let mut buffer = Vec::new();
    CsvWriter::new(&mut buffer)
        .finish(df)
        .with_context(|| "[io::csv::write] Failed to write CSV to string")?;
    String::from_utf8(buffer)
        .with_context(|| "[io::csv::write] CSV output is not valid UTF-8")
}

/// Write plan assignments to a CSV file.
/// 
/// The assignments are a vector of (GeoId, district) tuples.
/// Only non-zero districts are written.
pub(crate) fn write_plan_assignments(assignments: &[(GeoId, u32)], path: &Path) -> Result<()> {
    let (geo_ids, districts) = assignments.iter()
        .filter_map(|(geo_id, district)| {
            (*district != 0).then_some((geo_id.id().to_string(), *district))
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let mut df = DataFrame::new(vec![
        Series::new("geo_id".into(), geo_ids).into(),
        Series::new("district".into(), districts).into(),
    ])?;

    write_csv(&mut df, path)
}

/// Write plan assignments to a CSV string (for WASM/browser use).
pub(crate) fn write_plan_assignments_string(assignments: &[(GeoId, u32)]) -> Result<String> {
    let (geo_ids, districts) = assignments.iter()
        .filter_map(|(geo_id, district)| {
            (*district != 0).then_some((geo_id.id().to_string(), *district))
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let mut df = DataFrame::new(vec![
        Series::new("geo_id".into(), geo_ids).into(),
        Series::new("district".into(), districts).into(),
    ])?;

    write_csv_string(&mut df)
}
