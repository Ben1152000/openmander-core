//! CSV reading operations using the `csv` crate.

use std::{fs::File, path::Path};

use anyhow::{Context, Result, ensure};

use crate::map::{GeoId, GeoType, MapLayer};

/// Read plan assignments from a CSV file.
pub(crate) fn read_csv(path: &Path) -> Result<CsvTable> {
    let file = File::open(path)
        .with_context(|| format!("[io::csv::read] Failed to open CSV file: {}", path.display()))?;
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(file);
    parse_csv_table(&mut rdr)
        .with_context(|| format!("[io::csv::read] Failed to read CSV from {:?}", path))
}

/// Read plan assignments from a CSV string (for WASM/browser use).
pub(crate) fn read_csv_string(csv: &str) -> Result<CsvTable> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());
    parse_csv_table(&mut rdr)
        .context("[io::csv::read] Failed to read CSV from string")
}

/// Minimal in-memory CSV table used for plan assignment CSVs (2-column).
pub(crate) struct CsvTable {
    pub headers: Vec<String>,
    pub rows:    Vec<Vec<String>>,
}

impl CsvTable {
    pub fn height(&self) -> usize { self.rows.len() }

    pub fn column_str(&self, name: &str) -> Option<Vec<&str>> {
        let idx = self.headers.iter().position(|h| h == name)?;
        Some(self.rows.iter().map(|row| row[idx].as_str()).collect())
    }
}

fn parse_csv_table<R: std::io::Read>(rdr: &mut csv::Reader<R>) -> Result<CsvTable> {
    let headers: Vec<String> = rdr
        .headers()
        .context("failed to read headers")?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut rows = Vec::new();
    let mut record = csv::StringRecord::new();
    while rdr.read_record(&mut record).context("failed to read record")? {
        rows.push(record.iter().map(|s| s.to_string()).collect());
    }

    Ok(CsvTable { headers, rows })
}

/// Read plan assignments from a `CsvTable`.
///
/// The table should have two columns: geo_id (string) and district (u32).
pub(crate) fn read_plan_assignments(
    table: CsvTable,
    block_layer: &MapLayer,
) -> Result<Vec<(GeoId, u32)>> {
    ensure!(
        table.headers.len() >= 2,
        "[io::csv::read] CSV must have two columns: geo_id,district"
    );
    ensure!(
        table.height() == block_layer.len(),
        "[io::csv::read] CSV has {} rows, expected {}",
        table.height(),
        block_layer.len()
    );

    let geo_id_col_idx = 0usize;
    let district_col_idx = 1usize;

    table.rows.iter()
        .map(|row| {
            let block_str = &row[geo_id_col_idx];
            let district: u32 = row[district_col_idx]
                .parse()
                .with_context(|| format!("invalid district value: {:?}", row[district_col_idx]))?;
            let geo_id = GeoId::new(GeoType::Block, block_str);
            ensure!(
                block_layer.geo_ids().contains(&geo_id),
                "[io::csv::read] GeoId {} in CSV not found in map",
                geo_id.id()
            );
            Ok((geo_id, district))
        })
        .collect()
}
