//! IO module for format-specific reading and writing operations.
//!
//! Each format module handles reading and writing for a specific file format:
//!
//! - `csv` - CSV format for tabular data
//! - `parquet` - Parquet format for tabular data (requires `parquet` feature)
//! - `geoparquet` - GeoParquet format for geometry storage (requires `parquet` feature)
//! - `pmtiles` - PMTiles format for tile-based geometry storage (requires `pmtiles` feature)
//! - `wkb` - Well-Known Binary format for hull geometry
//! - `svg` - SVG format for visualization export
//! - `shp` - Shapefile format for geographic data
//! - `csr` - Compressed Sparse Row format for adjacency graphs

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

pub(crate) mod wkb {
    mod read;
    mod write;
    pub(crate) use read::*;
    pub(crate) use write::*;
}

#[cfg(feature = "download")]
pub(crate) mod shp;
pub(crate) mod csr;

#[cfg(feature = "parquet")]
pub(crate) mod parquet {
    mod read;
    mod write;
    pub(crate) use read::*;
    pub(crate) use write::*;
}

#[cfg(feature = "parquet")]
pub(crate) mod geoparquet {
    mod read;
    mod write;
    pub(crate) use read::*;
    pub(crate) use write::*;
}

#[cfg(feature = "pmtiles")]
pub(crate) mod pmtiles {
    mod proj;
    mod read;
    mod write;
    pub(crate) use read::*;
    pub(crate) use write::*;
}
