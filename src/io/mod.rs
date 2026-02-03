//! IO module for format-specific reading and writing operations.
//!
//! This module consolidates all IO operations organized by format type rather than domain.
//! Each format module handles reading and writing for a specific file format.
//!
//! # Format Modules
//!
//! - `csv` - CSV format for tabular data and plan assignments
//! - `parquet` - Parquet format for tabular data (requires `parquet` feature)
//! - `geoparquet` - GeoParquet format for geometry storage (requires `parquet` feature)
//! - `pmtiles` - PMTiles format for tile-based geometry storage (requires `pmtiles` feature)
//! - `wkb` - Well-Known Binary format for hull geometry
//! - `svg` - SVG format for visualization export
//! - `shp` - Shapefile format for geographic data
//! - `csr` - Compressed Sparse Row format for adjacency graphs
//! - `pack` - Pack-level operations for reading/writing complete data packs
//!
//! Note: GeoJSON export is implemented as methods on MapLayer in map/io/geojson.rs
//! since it requires access to private fields.

pub(crate) mod csv;
pub(crate) mod svg;
pub(crate) mod shp;
pub(crate) mod csr;
pub(crate) mod wkb;
pub(crate) mod pack;

#[cfg(feature = "parquet")]
pub(crate) mod parquet;

#[cfg(feature = "parquet")]
pub(crate) mod geoparquet;

#[cfg(feature = "pmtiles")]
pub(crate) mod pmtiles;
