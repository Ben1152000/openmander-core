use deku::prelude::*;

/// A compression, which is supported in `PMTiles` archives.
#[derive(DekuRead, DekuWrite, Debug, Clone, Copy, PartialEq, Eq)]
#[deku(type = "u8")]
#[deku(endian = "endian", ctx = "endian: deku::ctx::Endian")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum Compression {
    /// Unknown compression
    ///
    /// _This should almost never be used, because some reader
    /// implementations may not know how to handle this._
    Unknown = 0x00,

    /// No compression
    None,

    /// GZIP compression as defined in [RFC 1952](https://www.rfc-editor.org/rfc/rfc1952)
    GZip,

    /// Brotli compression as defined in [RFC 7932](https://www.rfc-editor.org/rfc/rfc7932)
    Brotli,

    /// Zstandard Compression as defined in [RFC 8478](https://www.rfc-editor.org/rfc/rfc8478)
    ZStd,
}

impl Compression {
    /// Returns a option containing the value to which the
    /// `Content-Encoding` HTTP header should be set, when serving
    /// tiles with this compression.
    ///
    /// Returns [`None`] if a concrete `Content-Encoding` could not be determined.
    pub const fn http_content_encoding(&self) -> Option<&'static str> {
        match self {
            Self::GZip => Some("gzip"),
            Self::Brotli => Some("br"),
            Self::ZStd => Some("zstd"),
            _ => None,
        }
    }
}

// Test module removed - test files are not included in this fork
