use deku::prelude::*;

/// A tile type, which is supported in `PMTiles` archives.
#[derive(DekuRead, DekuWrite, Debug, Clone, Copy, PartialEq, Eq)]
#[deku(type = "u8")]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum TileType {
    #[allow(missing_docs)]
    Unknown = 0x00,

    /// Mapbox Vector Tiles as defined [here](https://github.com/mapbox/vector-tile-spec)
    Mvt,

    #[allow(missing_docs)]
    Png,

    #[allow(missing_docs)]
    Jpeg,

    #[allow(missing_docs)]
    WebP,

    #[allow(missing_docs)]
    AVIF,
}

impl TileType {
    /// Returns a option containing the value to which the
    /// `Content-Type` HTTP header should be set, when serving
    /// tiles from this type.
    ///
    /// Returns [`None`] if a concrete `Content-Type` could not be determined.
    pub const fn http_content_type(&self) -> Option<&'static str> {
        match self {
            Self::Mvt => Some("application/vnd.mapbox-vector-tile"),
            Self::Png => Some("image/png"),
            Self::Jpeg => Some("image/jpeg"),
            Self::WebP => Some("image/webp"),
            Self::AVIF => Some("image/avif"),
            Self::Unknown => None,
        }
    }
}

// Test module removed - test files are not included in this fork
