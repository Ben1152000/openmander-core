//! Pack CSV reader and writer for the runtime path (pack loading and saving).
//!
//! Uses a two-pass approach:
//! 1. Count `\n` bytes to determine the row count for Array2 pre-allocation.
//! 2. Parse headers (to determine column layout and sniff numeric types from the
//!    first data row), then parse all rows directly into pre-allocated arrays.
//!
//! No intermediate `Vec` is materialised for numeric data — each parsed value is
//! written directly to its final location in the `Array2`.

use anyhow::{Context, Result, bail};
use ndarray::Array2;

use crate::{
    graph::WeightMatrix,
    map::{GeoId, GeoType, ParentRefs},
};

/// Output of the pack CSV reader.
pub(crate) struct ParsedPackCsv {
    pub geo_ids:    Vec<String>,    // raw strings; caller wraps with GeoId::new(ty, s)
    pub unit_names: Vec<String>,    // per-unit human-readable name ("name" column)
    pub parents:    Vec<ParentRefs>, // GeoType inferred from column name
    pub weights:    WeightMatrix,
}

/// (column name, GeoType) for the five parent-reference columns.
pub(crate) const PARENT_COL_TYPES: &[(&str, GeoType)] = &[
    ("parent_state",  GeoType::State),
    ("parent_county", GeoType::County),
    ("parent_tract",  GeoType::Tract),
    ("parent_group",  GeoType::Group),
    ("parent_vtd",    GeoType::VTD),
];

/// Columns that are always string-typed (never numeric).
const KNOWN_STRING_COLS: &[&str] = &[
    "geo_id", "name",
    "parent_state", "parent_county", "parent_tract", "parent_group", "parent_vtd",
];

/// Columns to skip entirely.
const SKIP_COLS: &[&str] = &["idx"];

/// Format an f64 for CSV output, ensuring the result always contains a decimal
/// point or exponent so type inference on read-back correctly identifies the
/// column as f64 (not i64).
#[inline]
fn format_f64(v: f64) -> String {
    if v.is_nan() {
        return String::new();
    }
    let s = format!("{}", v);
    if s.bytes().any(|b| b == b'.' || b == b'e' || b == b'E') {
        s
    } else {
        format!("{}.0", s)
    }
}

/// Parse a pack CSV from raw bytes.
///
/// Two-pass: first count newlines for exact row count (SIMD-fast in release),
/// then parse header to determine column layout and sniff numeric types from
/// the first data row, then parse all rows, filling pre-allocated `Array2`
/// matrices directly.
pub(crate) fn read_pack_csv(bytes: &[u8]) -> Result<ParsedPackCsv> {
    // Pass 1: count data rows via newline scan.
    // Pack CSVs always end with a trailing newline and never have quoted
    // multi-line fields, so newlines = header_line + data_lines.
    let newline_count = bytes.iter().filter(|&&b| b == b'\n').count();
    let n_rows = if newline_count > 0 { newline_count - 1 } else { 0 };

    if n_rows == 0 {
        return Ok(ParsedPackCsv {
            geo_ids:    Vec::new(),
            unit_names: Vec::new(),
            parents:    Vec::new(),
            weights:    WeightMatrix::from_arrays(
                vec![], Array2::zeros((0, 0)),
                vec![], Array2::zeros((0, 0)),
            ),
        });
    }

    // Parse the header row to get column names.
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let headers: Vec<String> = rdr
        .headers()
        .context("failed to read CSV headers")?
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Read the first data row to sniff numeric column types.
    let first_record = rdr
        .records()
        .next()
        .context("CSV has no data rows")?
        .context("failed to read first CSV record")?;

    // Categorise each column by how it should be handled.
    #[derive(Clone)]
    enum ColKind {
        GeoId,
        Name,
        Parent(GeoType),
        I64(usize), // index into i64 matrix column
        F64(usize), // index into f64 matrix column
        Skip,
    }

    let mut col_kinds: Vec<ColKind> = Vec::with_capacity(headers.len());
    let mut i64_names: Vec<String> = Vec::new();
    let mut f64_names: Vec<String> = Vec::new();

    for (ci, name) in headers.iter().enumerate() {
        let name_str = name.as_str();
        if SKIP_COLS.contains(&name_str) {
            col_kinds.push(ColKind::Skip);
        } else if name_str == "geo_id" {
            col_kinds.push(ColKind::GeoId);
        } else if name_str == "name" {
            col_kinds.push(ColKind::Name);
        } else if let Some(&(_, ty)) = PARENT_COL_TYPES.iter().find(|&&(n, _)| n == name_str) {
            col_kinds.push(ColKind::Parent(ty));
        } else if KNOWN_STRING_COLS.contains(&name_str) {
            // Other known string columns we don't use → skip.
            col_kinds.push(ColKind::Skip);
        } else {
            // Numeric column: sniff type from the first data row.
            let val = first_record.get(ci).unwrap_or("");
            let is_f64 = !val.is_empty()
                && (val.contains('.') || val.contains('e') || val.contains('E'));
            if is_f64 {
                let idx = f64_names.len();
                f64_names.push(name.clone());
                col_kinds.push(ColKind::F64(idx));
            } else {
                let idx = i64_names.len();
                i64_names.push(name.clone());
                col_kinds.push(ColKind::I64(idx));
            }
        }
    }

    let n_i64 = i64_names.len();
    let n_f64 = f64_names.len();

    // Pre-allocate output arrays.
    let mut geo_ids    = Vec::with_capacity(n_rows);
    let mut unit_names = Vec::with_capacity(n_rows);
    let mut parents    = vec![ParentRefs::default(); n_rows];
    let mut i64_data   = Array2::<i32>::zeros((n_rows, n_i64));
    let mut f64_data   = Array2::<f64>::zeros((n_rows, n_f64));

    // Pass 2: restart the reader and parse every row.
    let mut rdr2 = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let mut record = csv::StringRecord::new();
    let mut row = 0usize;

    while rdr2
        .read_record(&mut record)
        .context("failed to read CSV record")?
    {
        if row >= n_rows {
            // More rows than the newline count predicted — extend arrays.
            // (Shouldn't happen for well-formed pack CSVs.)
            bail!(
                "pack CSV has more data rows than the newline count predicted \
                 (got row {} but n_rows={})",
                row,
                n_rows
            );
        }

        for (ci, kind) in col_kinds.iter().enumerate() {
            let val = record.get(ci).unwrap_or("");
            match kind {
                ColKind::GeoId => geo_ids.push(val.to_string()),
                ColKind::Name  => unit_names.push(val.to_string()),
                ColKind::Parent(ty) => {
                    if !val.is_empty() {
                        parents[row].set(*ty, Some(GeoId::new(*ty, val)));
                    }
                }
                ColKind::I64(col_idx) => {
                    i64_data[(row, *col_idx)] = if val.is_empty() {
                        0
                    } else {
                        val.parse::<i32>().with_context(|| {
                            format!(
                                "column {:?} row {}: cannot parse {:?} as i32",
                                headers[ci], row, val
                            )
                        })?
                    };
                }
                ColKind::F64(col_idx) => {
                    f64_data[(row, *col_idx)] = if val.is_empty() {
                        0.0
                    } else {
                        val.parse::<f64>().with_context(|| {
                            format!(
                                "column {:?} row {}: cannot parse {:?} as f64",
                                headers[ci], row, val
                            )
                        })?
                    };
                }
                ColKind::Skip => {}
            }
        }

        row += 1;
    }

    // Fill unit_names if the "name" column was absent.
    if unit_names.is_empty() {
        unit_names = vec![String::new(); row];
    }

    let weights = WeightMatrix::from_arrays(i64_names, i64_data, f64_names, f64_data);

    Ok(ParsedPackCsv {
        geo_ids,
        unit_names,
        parents,
        weights,
    })
}

/// Serialise a layer back to pack CSV bytes.
pub(crate) fn write_pack_csv(
    geo_ids:    &[GeoId],
    unit_names: &[String],
    parents:    &[ParentRefs],
    weights:    &WeightMatrix,
) -> Result<Vec<u8>> {
    let n = geo_ids.len();
    let i64_names = weights.int_series_names();
    let f64_names = weights.f64_series_names();

    let mut out = Vec::new();
    {
        let mut wtr = csv::Writer::from_writer(&mut out);

        // Header: geo_id, name, <i64 cols>, <f64 cols>, <parent cols>
        let mut header: Vec<&str> = vec!["geo_id", "name"];
        header.extend(i64_names.iter().copied());
        header.extend(f64_names.iter().copied());
        for &(col_name, _) in PARENT_COL_TYPES {
            header.push(col_name);
        }
        wtr.write_record(&header)
            .context("failed to write CSV header")?;

        // One row per unit.
        let mut record = csv::StringRecord::new();

        for i in 0..n {
            record.clear();
            record.push_field(geo_ids[i].id());
            record.push_field(unit_names.get(i).map(|s| s.as_str()).unwrap_or(""));

            let i64_row = weights.int_row(i);
            for &v in i64_row.iter() {
                record.push_field(&v.to_string());
            }

            let f64_row = weights.f64_row(i);
            for &v in f64_row.iter() {
                record.push_field(&format_f64(v));
            }

            for &(_, ty) in PARENT_COL_TYPES {
                let val = parents
                    .get(i)
                    .and_then(|p| p.get(ty))
                    .map(|g| g.id())
                    .unwrap_or("");
                record.push_field(val);
            }

            wtr.write_record(&record)
                .context("failed to write CSV record")?;
        }

        wtr.flush().context("failed to flush CSV writer")?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_csv() -> Vec<u8> {
        // A small synthetic pack CSV with one i64 col and one f64 col.
        // Parent IDs must be exactly the right length for GeoId::new to accept them.
        b"geo_id,name,population,area_m2,parent_state,parent_county,parent_tract,parent_group,parent_vtd\n\
          060750101001000,Block 1,1234,567.89,06,06075,06075010100,060750101001,\n\
          060750101001001,Block 2,5678,1234.5,06,06075,06075010100,060750101001,\n"
            .to_vec()
    }

    #[test]
    fn test_round_trip() {
        let original_csv = make_test_csv();
        let parsed = read_pack_csv(&original_csv).expect("read failed");

        assert_eq!(parsed.geo_ids.len(), 2);
        assert_eq!(parsed.geo_ids[0], "060750101001000");
        assert_eq!(parsed.unit_names[0], "Block 1");
        assert_eq!(parsed.unit_names[1], "Block 2");

        // Check weights
        assert_eq!(parsed.weights.get_as_f64("population", 0), Some(1234.0));
        assert_eq!(parsed.weights.get_as_f64("population", 1), Some(5678.0));
        assert_eq!(parsed.weights.get_as_f64("area_m2", 0), Some(567.89));
        assert_eq!(parsed.weights.get_as_f64("area_m2", 1), Some(1234.5));

        // Check parents
        let geo_ids: Vec<GeoId> = parsed.geo_ids.iter()
            .map(|s| GeoId::new(GeoType::Block, s))
            .collect();
        let written = write_pack_csv(
            &geo_ids,
            &parsed.unit_names,
            &parsed.parents,
            &parsed.weights,
        ).expect("write failed");

        // Re-read and verify round-trip
        let reparsed = read_pack_csv(&written).expect("re-read failed");
        assert_eq!(reparsed.geo_ids, parsed.geo_ids);
        assert_eq!(reparsed.unit_names, parsed.unit_names);
        assert_eq!(
            reparsed.weights.get_as_f64("population", 0),
            parsed.weights.get_as_f64("population", 0)
        );
        assert_eq!(
            reparsed.weights.get_as_f64("area_m2", 0),
            parsed.weights.get_as_f64("area_m2", 0)
        );
    }
}
