//! CSV writing operations using the `csv` crate.

use std::{fs::File, path::Path};

use anyhow::{Context, Result};

use crate::map::GeoId;

/// Write plan assignments to a CSV file.
///
/// Only non-zero districts are written.
pub(crate) fn write_plan_assignments(assignments: &[(GeoId, u32)], path: &Path) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("[io::csv::write] Failed to create CSV file: {}", path.display()))?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(["geo_id", "district"])?;
    for (geo_id, district) in assignments.iter().filter(|(_, d)| *d != 0) {
        wtr.write_record(&[geo_id.id(), &district.to_string()])?;
    }
    wtr.flush()?;
    Ok(())
}

/// Write plan assignments to a CSV string (for WASM/browser use).
pub(crate) fn write_plan_assignments_string(assignments: &[(GeoId, u32)]) -> Result<String> {
    let mut out = Vec::new();
    {
        let mut wtr = csv::Writer::from_writer(&mut out);
        wtr.write_record(["geo_id", "district"])?;
        for (geo_id, district) in assignments.iter().filter(|(_, d)| *d != 0) {
            wtr.write_record(&[geo_id.id(), &district.to_string()])?;
        }
        wtr.flush()?;
    }
    String::from_utf8(out).context("[io::csv::write] CSV output is not valid UTF-8")
}
