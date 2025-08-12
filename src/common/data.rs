use std::path::Path;
use std::fs::File;
use std::io::BufWriter;

use anyhow::{Context, Result};
use polars::{
    frame::DataFrame, 
    io::SerReader, 
    prelude::{CsvReader, ParquetWriter} 
};
use shapefile::{dbase::Record, Reader, Shape};

/// Reads a CSV file from `path` into a Polars DataFrame.
pub fn read_from_csv(path: &Path) -> Result<DataFrame> {
    let file = File::open(&path)?;
    let df = CsvReader::new(file)
        .finish()?;
    Ok(df)
}

/// Writes a Polars DataFrame to a Parquet file at `path`.
pub fn write_to_parquet(mut df: DataFrame, path: &Path) -> Result<()> {
    let file = File::create(&path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    ParquetWriter::new(writer)
        .finish(&mut df)?;
    Ok(())
}

/// Reads all shapes + attribute records from a given `.shp` file path.
pub fn read_shapefile(path: &Path) -> Result<Vec<(Shape, Record)>> {
    let mut reader = Reader::from_path(path)
        .with_context(|| format!("Failed to open shapefile: {}", path.display()))?;

    let mut items = Vec::with_capacity(reader.shape_count()?);
    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Error reading shape+record")?;
        items.push((shape, record));
    }
    Ok(items)
}

/// Debug info from just the items vector (no schema available).
pub fn debug_print_shapefile(items: &[(Shape, Record)]) {
    use std::collections::BTreeMap;

    println!("Number of records: {}", items.len());

    // Geometry-type breakdown
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (shape, _) in items {
        let k = match shape {
            Shape::Point(_) | Shape::PointM(_) | Shape::PointZ(_) => "Point",
            Shape::Polygon(_) | Shape::PolygonM(_) | Shape::PolygonZ(_) => "Polygon",
            _ => "Other",
        };
        *counts.entry(k).or_default() += 1;
    }
    println!("Geometry mix:");
    for (k, v) in counts {
        println!("  - {}: {}", k, v);
    }

    if let Some((_, record)) = items.first() {
        println!("Attribute columns:");
        for (field, value) in record.clone() {
            println!("  - {} ({:?})", field, value);
        }
    }
}
