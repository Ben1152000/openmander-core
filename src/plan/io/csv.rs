use std::{fs::File, io::Cursor, path::Path};

use anyhow::{Context, Ok, Result, ensure};
use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::{CsvReader, CsvWriter, DataType, NamedFrom}, series::Series};

use crate::{map::{GeoId, GeoType}, plan::Plan};

impl Plan {
    /// Load a plan from a CSV block assignment file.
    pub fn read_from_csv(&mut self, csv_path: &Path) -> Result<()> {
        // Read the CSV file into a Polars DataFrame, throwing an error if the file isn't found
        let df = CsvReader::new(File::open(csv_path)
            .with_context(|| format!("[Plan.from_csv] Failed to open CSV file: {}", csv_path.display()))?)
            .finish()
            .with_context(|| format!("[Plan.from_csv] Failed to read CSV file: {}", csv_path.display()))?;

        self.load_assignment_df(df)
    }

    /// Load a plan from CSV text (for browser/WASM use).
    pub fn load_csv(&mut self, csv: &str) -> Result<()> {
        let df = CsvReader::new(Cursor::new(csv.as_bytes()))
            .finish()
            .with_context(|| "[Plan.load_csv_text] Failed to read CSV text")?;

        self.load_assignment_df(df)
    }

    fn load_assignment_df(&mut self, df: DataFrame) -> Result<()> {
        let block_layer = self.map().base()?;

        // Ensure CSV has at the correct number of rows and columns.
        ensure!(df.width() >= 2, "[Plan.load_csv_text] CSV must have two columns: geo_id,district");
        ensure!(df.height() == block_layer.len(), "[Plan.load_csv_text] CSV has {} rows, expected {}", df.height(), block_layer.len());

        // Populate plan.assignments from CSV
        let blocks = df.column(df.get_column_names()[0])?.cast(&DataType::String)?;
        let districts = df.column(df.get_column_names()[1])?.cast(&DataType::UInt32)?;

        let assignments = blocks.str()?.into_no_null_iter()
            .zip(districts.u32()?.into_no_null_iter())
            .map(|(block, district)| {
                let geo_id = GeoId::new(GeoType::Block, block);
                ensure!(block_layer.geo_ids().contains(&geo_id), "[Plan.load_csv_text] GeoId {} in CSV not found in map", geo_id.id());

                Ok((geo_id, district))
            })
            .collect::<Result<_>>()?;

        self.set_assignments(assignments)
    }

    /// Generate a CSV block assignment
    pub fn write_to_csv(&self, path: &Path) -> Result<()> {
        let (geo_ids, districts) = self.get_assignments()?.iter()
            .filter_map(|(geo_id, &district)| (district != 0)
                .then_some((geo_id.id().to_string(), district)))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        CsvWriter::new(File::create(path)?).finish(
            &mut DataFrame::new(vec![
                Series::new("geo_id".into(), geo_ids).into(),
                Series::new("district".into(), districts).into(),
            ])?
        )?;

        Ok(())
    }

    /// Generate a CSV block assignment as a string (for browser/WASM use).
    pub fn to_csv(&self) -> Result<String> {
        let (geo_ids, districts) = self.get_assignments()?.iter()
            .filter_map(|(geo_id, &district)| (district != 0)
                .then_some((geo_id.id().to_string(), district)))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let mut buffer = Vec::new();
        CsvWriter::new(&mut buffer).finish(
            &mut DataFrame::new(vec![
                Series::new("geo_id".into(), geo_ids).into(),
                Series::new("district".into(), districts).into(),
            ])?
        )?;

        Ok(String::from_utf8(buffer)
            .context("[Plan.to_csv_text] CSV output is not valid UTF-8")?)
    }
}
