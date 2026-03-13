//! IO module for format-specific reading and writing operations.
//!
//! Each format module handles reading and writing for a specific file format:
//!
//! - `csv` - CSV format for tabular data
//! - `parquet` - Parquet format for tabular data (requires `parquet` feature)
//! - `pmtiles` - PMTiles format for tile-based geometry storage (requires `pmtiles` feature)
//! - `shp` - Shapefile format for geographic data
//! - `svg` - SVG format for visualization export
//! - `wkb` - Well-Known Binary format for hull geometry

pub(crate) mod csv {
    mod read;
    mod write;
    pub(crate) use read::*;
    pub(crate) use write::*;
}

pub(crate) mod svg {
    mod color;
    mod geometry;
    mod proj;
    mod writer;
    pub(crate) use color::*;
    pub(crate) use geometry::*;
    pub(crate) use proj::*;
    pub(crate) use writer::*;
}

pub(crate) mod wkb;

#[cfg(feature = "download")]
pub(crate) mod shp;

#[cfg(feature = "parquet")]
pub(crate) mod parquet;

#[cfg(feature = "pmtiles")]
pub(crate) mod pmtiles;
