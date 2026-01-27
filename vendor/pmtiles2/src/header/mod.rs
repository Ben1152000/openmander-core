pub use compression::*;
#[cfg(feature = "async")]
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub use lat_lng::*;
pub use tile_type::*;

mod compression;
mod lat_lng;
mod tile_type;

use deku::bitvec::{BitVec, BitView};
use deku::prelude::*;
use std::io::{Read, Write};

pub const HEADER_BYTES: u8 = 127;

/// A structure representing a `PMTiles` header.
#[derive(DekuRead, DekuWrite, Debug)]
#[deku(magic = b"PMTiles")]
#[deku(endian = "little")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header {
    /// Version of Specification (always 3)
    #[deku(assert_eq = "3")]
    pub spec_version: u8,

    /// Offset (in bytes) of root directory section from start of file
    pub root_directory_offset: u64,

    /// Length (in bytes) of root directory section
    pub root_directory_length: u64,

    /// Offset (in bytes) of metadata section from start of file
    pub json_metadata_offset: u64,

    /// Length (in bytes) of metadata section
    pub json_metadata_length: u64,

    /// Offset (in bytes) of leaf directories section from start of file
    pub leaf_directories_offset: u64,

    /// Length (in bytes) of leaf directories section
    pub leaf_directories_length: u64,

    /// Offset (in bytes) of tile data section from start of file
    pub tile_data_offset: u64,

    /// Length (in bytes) of tile data section
    pub tile_data_length: u64,

    /// Number of tiles, which are addressable in this `PMTiles` archive
    pub num_addressed_tiles: u64,

    /// Number of directory entries, that point to a tile
    pub num_tile_entries: u64,

    /// Number of distinct tile contents in the tile data section
    pub num_tile_content: u64,

    /// Indicates whether this archive is clustered, which means that
    /// all directory entries are ordered in ascending order by `tile_ids`
    #[deku(bits = 8)]
    pub clustered: bool,

    /// Compression of directories and meta data section
    pub internal_compression: Compression,

    /// Compression of tiles in this archive
    pub tile_compression: Compression,

    /// Type of tiles in this archive
    pub tile_type: TileType,

    /// Minimum zoom of all tiles this archive
    pub min_zoom: u8,

    /// Maximum zoom of all tiles this archive
    pub max_zoom: u8,

    /// Minimum latitude and longitude of bounds of available tiles in this archive
    pub min_pos: LatLng,

    /// Maximum latitude and longitude of bounds of available tiles in this archive
    pub max_pos: LatLng,

    /// Center zoom
    ///
    /// Implementations may use this to set the default zoom
    pub center_zoom: u8,

    /// Center latitude and longitude
    ///
    /// Implementations may use these values to set the default location
    pub center_pos: LatLng,
}

impl Header {
    /// Returns a option containing the value to which the `Content-Encoding`
    /// HTTP header should be set, when serving tiles from this archive.
    ///
    /// Returns [`None`] if a concrete `Content-Encoding` could not be determined.
    pub const fn http_content_type(&self) -> Option<&'static str> {
        self.tile_type.http_content_type()
    }

    /// Returns a option containing the value to which the `Content-Type` HTTP
    /// header should be set, when serving tiles from this archive.
    ///
    /// Returns [`None`] if a concrete `Content-Type` could not be determined.
    pub const fn http_content_encoding(&self) -> Option<&'static str> {
        self.tile_compression.http_content_encoding()
    }

    /// Reads a header from a [`std::io::Read`] and returns it.
    ///
    /// # Arguments
    /// * `input` - Reader
    ///
    /// # Errors
    /// Will return [`Err`] an I/O error occurred while reading from `input`.
    ///
    pub fn from_reader(input: &mut impl Read) -> std::io::Result<Self> {
        let mut buf = [0; HEADER_BYTES as usize];
        input.read_exact(&mut buf)?;

        let (_, header) = Self::read(buf.to_vec().view_bits(), ())?;

        Ok(header)
    }

    /// Reads a header from a anything that can be turned into a byte slice (e.g. [`Vec<u8>`]).
    ///
    /// # Arguments
    /// * `bytes` - Input bytes
    ///
    /// # Errors
    /// Will return [`Err`] an I/O error occurred while reading from `input`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Header};
    /// let bytes = include_bytes!("../../example.pmtiles");
    /// let header = Header::from_bytes(bytes).unwrap();
    /// ```
    ///
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> std::io::Result<Self> {
        let mut reader = std::io::Cursor::new(bytes);

        Self::from_reader(&mut reader)
    }

    /// Async version of [`from_reader`](Self::from_reader).
    ///
    /// Reads a header from a [`futures::io::AsyncRead`](https://docs.rs/futures/latest/futures/io/trait.AsyncRead.html) and returns it.
    ///
    /// # Arguments
    /// * `input` - Reader
    ///
    /// # Errors
    /// Will return [`Err`] an I/O error occurred while reading from `input`.
    ///
    #[cfg(feature = "async")]
    pub async fn from_async_reader(
        input: &mut (impl AsyncRead + Unpin + Send),
    ) -> std::io::Result<Self> {
        let mut buf = [0; HEADER_BYTES as usize];

        input.read_exact(&mut buf).await?;

        let (_, header) = Self::read(buf.to_vec().view_bits(), ())?;

        Ok(header)
    }

    /// Writes the header to a [`std::io::Write`].
    ///
    /// # Arguments
    /// * `output` - Writer to write header to
    ///
    /// # Errors
    /// Will return [`Err`] if an I/O error occurred while writing to `output`.
    ///
    pub fn to_writer(&self, output: &mut impl Write) -> std::io::Result<()> {
        let mut bit_vec = BitVec::with_capacity(8 * HEADER_BYTES as usize);
        self.write(&mut bit_vec, ())?;
        output.write_all(bit_vec.as_raw_slice())?;

        Ok(())
    }

    /// Async version of [`to_writer`](Self::to_writer).
    ///
    /// Writes the header to a [`futures::io::AsyncWrite`](https://docs.rs/futures/latest/futures/io/trait.AsyncWrite.html).
    ///
    /// # Arguments
    /// * `output` - Writer to write header to
    ///
    /// # Errors
    /// Will return [`Err`] if an I/O error occurred while writing to `output`.
    ///
    #[cfg(feature = "async")]
    pub async fn to_async_writer(
        &self,
        output: &mut (impl AsyncWrite + Unpin + Send),
    ) -> std::io::Result<()> {
        let vec = self.to_bytes()?;
        output.write_all(&vec).await?;
        output.flush().await?;

        Ok(())
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            spec_version: 3,
            root_directory_offset: 0,
            root_directory_length: 0,
            json_metadata_offset: 0,
            json_metadata_length: 0,
            leaf_directories_offset: 0,
            leaf_directories_length: 0,
            tile_data_offset: 0,
            tile_data_length: 0,
            num_addressed_tiles: 0,
            num_tile_entries: 0,
            num_tile_content: 0,
            clustered: false,
            internal_compression: Compression::GZip,
            tile_compression: Compression::None,
            tile_type: TileType::Unknown,
            min_zoom: 0,
            max_zoom: 0,
            min_pos: LatLng {
                longitude: -180.0,
                latitude: -85.0,
            },
            max_pos: LatLng {
                longitude: 180.0,
                latitude: 85.0,
            },
            center_zoom: 0,
            center_pos: LatLng {
                longitude: 0.0,
                latitude: 0.0,
            },
        }
    }
}

// Test module removed - test files are not included in this fork
