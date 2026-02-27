use std::io::{Read, Write};

use crate::region::Region;

/// Magic bytes at the start of every geograph binary file.
pub const MAGIC: &[u8; 4] = b"GPHR";

/// Current file format version.
pub const VERSION: u8 = 1;

/// Errors that can occur during serialisation or deserialisation.
#[derive(Debug)]
pub enum IoError {
    Io(std::io::Error),
    /// File does not start with the expected magic bytes.
    InvalidMagic,
    /// File was written by a newer or incompatible version.
    UnsupportedVersion(u8),
    /// File contents are structurally invalid.
    InvalidData(String),
}

impl From<std::io::Error> for IoError {
    fn from(e: std::io::Error) -> Self {
        IoError::Io(e)
    }
}

/// Serialise `region` to `writer` using the geograph binary format.
///
/// See §8 (Serialisation) of DESIGN.md for the full file layout.
pub fn write(region: &Region, writer: &mut impl Write) -> Result<(), IoError> {
    todo!()
}

/// Deserialise a `Region` from `reader`.
///
/// See §8 (Serialisation) of DESIGN.md for the full file layout.
pub fn read(reader: &mut impl Read) -> Result<Region, IoError> {
    todo!()
}
