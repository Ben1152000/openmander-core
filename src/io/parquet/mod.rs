//! Parquet format reading and writing operations.
//!
//! This module is only available when the `parquet` feature is enabled.

mod read;
mod write;

pub(crate) use read::*;
pub(crate) use write::*;
