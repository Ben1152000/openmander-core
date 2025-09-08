use std::{fs::File, path::Path};

use anyhow::{Result};
use polars::{
    frame::DataFrame,
    io::SerReader,
    prelude::{CsvReadOptions, CsvReader}
};

/// Reads a CSV file from `path` into a Polars DataFrame.
pub fn read_from_csv(path: &Path) -> Result<DataFrame> {
    let file = File::open(&path)?;
    let df = CsvReader::new(file)
        .finish()?;
    Ok(df)
}

/// Reads a pipe-delimited `.txt` file with a header row into a Polars DataFrame.
pub fn read_from_pipe_delimited_txt(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .map_parse_options(|po| po
            .with_separator(b'|'))
            .with_infer_schema_length(Some(0))
        .into_reader_with_file_handle(file).finish()?;
    Ok(df)
}
