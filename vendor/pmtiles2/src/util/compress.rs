use crate::Compression;

#[cfg(feature = "async")]
use async_compression::futures::{
    bufread::{
        BrotliDecoder as AsyncBrotliDecoder, GzipDecoder as AsyncGzipDecoder,
    },
    write::{
        BrotliEncoder as AsyncBrotliEncoder, GzipEncoder as AsyncGzipEncoder,
    },
};
#[cfg(all(feature = "async", not(target_arch = "wasm32")))]
use async_compression::futures::{
    bufread::ZstdDecoder as AsyncZstdDecoder,
    write::ZstdEncoder as AsyncZstdEncoder,
};
use brotli::{CompressorWriter as BrotliEncoder, Decompressor as BrotliDecoder};
use flate2::{read::GzDecoder, write::GzEncoder};
#[cfg(feature = "async")]
use futures::{io::BufReader, AsyncRead, AsyncWrite};
#[cfg(not(target_arch = "wasm32"))]
use zstd::{Decoder as ZSTDDecoder, Encoder as ZSTDEncoder};

use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};

/// Returns a new instance of [`std::io::Write`] that will emit compressed data to the underlying writer.
///
/// # Arguments
/// * `compression` - Compression to use
/// * `writer` - Underlying writer to write compressed data to
///
/// # Errors
/// Will return [`Err`] if `compression` is set to [`Compression::Unknown`] or an error occurred
/// while creating the zstd encoder.
///
/// # Example
/// ```rust
/// # use pmtiles2::{util::compress, Compression};
/// let mut output = Vec::<u8>::new();
///
/// let mut writer = compress(Compression::GZip, &mut output).unwrap();
///
/// let data_to_compress: Vec<u8> = vec![1, 3, 3, 7, 0, 4, 2, 0, 6, 9];
/// writer.write_all(&data_to_compress).unwrap();
///
/// writer.flush().unwrap(); // do not forget to flush writer to make sure it is done writing
/// ```
pub fn compress<'a>(
    compression: Compression,
    writer: &'a mut impl Write,
) -> Result<Box<dyn Write + 'a>> {
    match compression {
        Compression::Unknown => Err(Error::new(
            ErrorKind::Other,
            "Cannot compress for Compression Unknown",
        )),
        Compression::None => Ok(Box::new(writer)),
        Compression::GZip => Ok(Box::new(GzEncoder::new(
            writer,
            flate2::Compression::default(),
        ))),
        Compression::Brotli => Ok(Box::new(BrotliEncoder::new(writer, 4096, 11, 24))),
        Compression::ZStd => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                Ok(Box::new(ZSTDEncoder::new(writer, 0)?.auto_finish()))
            }
            #[cfg(target_arch = "wasm32")]
            {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "ZStd compression is not supported on WASM targets",
                ))
            }
        },
    }
}

/// Async version of [`compress`].
///
/// Returns a new instance of [`futures::io::AsyncWrite`](https://docs.rs/futures/latest/futures/io/trait.AsyncWrite.html) that will emit compressed data to the underlying writer.
///
/// # Arguments
/// * `compression` - Compression to use
/// * `writer` - Underlying writer to write compressed data to
///
/// # Errors
/// Will return [`Err`] if `compression` is set to [`Compression::Unknown`] or an error occurred
/// while creating the zstd encoder.
///
/// # Example
/// ```rust
/// # use futures::io::{AsyncWriteExt};
/// # use pmtiles2::{util::compress_async, Compression};
/// # tokio_test::block_on(async {
/// let mut output = Vec::<u8>::new();
///
/// let mut writer = compress_async(Compression::GZip, &mut output).unwrap();
///
/// let data_to_compress: Vec<u8> = vec![1, 3, 3, 7, 0, 4, 2, 0, 6, 9];
/// writer.write_all(&data_to_compress).await.unwrap();
///
/// writer.close().await.unwrap(); // do not forget to close writer to make sure it is done writing
/// # })
/// ```
#[allow(clippy::module_name_repetitions)]
#[cfg(feature = "async")]
pub fn compress_async<'a>(
    compression: Compression,
    writer: &'a mut (impl AsyncWrite + Unpin + Send),
) -> Result<Box<dyn AsyncWrite + Unpin + Send + 'a>> {
    match compression {
        Compression::Unknown => Err(Error::new(
            ErrorKind::Other,
            "Cannot compress for Compression Unknown",
        )),
        Compression::None => Ok(Box::new(writer)),
        Compression::GZip => Ok(Box::new(AsyncGzipEncoder::new(writer))),
        Compression::Brotli => Ok(Box::new(AsyncBrotliEncoder::new(writer))),
        Compression::ZStd => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                Ok(Box::new(AsyncZstdEncoder::new(writer)))
            }
            #[cfg(target_arch = "wasm32")]
            {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "ZStd compression is not supported on WASM targets",
                ))
            }
        },
    }
}

/// Compresses a byte slice and returns the result as a new [`Vec<u8>`].
///
/// # Arguments
/// * `compression` - Compression to use
/// * `data` - Data to compress
///
/// # Errors
/// Will return [`Err`] if `compression` is set to [`Compression::Unknown`], there was an error
/// while creating the zstd encoder or an error occurred while writing to `data`.
#[allow(clippy::module_name_repetitions)]
pub fn compress_all(compression: Compression, data: &[u8]) -> Result<Vec<u8>> {
    let mut destination = Vec::<u8>::new();

    {
        let mut writer = compress(compression, &mut destination)?;

        writer.write_all(data)?;

        writer.flush()?;
    }

    Ok(destination)
}

/// Returns a new instance of [`std::io::Read`] that will emit uncompressed data from an the underlying reader.
///
/// # Arguments
/// * `compression` - Compression to use
/// * `compressed_data` - Underlying reader with compressed data
///
/// # Errors
/// Will return [`Err`] if `compression` is set to [`Compression::Unknown`],there was an
/// error while creating the zstd decoder.
///
/// # Example
/// ```rust
/// # use pmtiles2::{util::decompress, Compression};
/// # let data = include_bytes!("../../example.json.gz");
/// let mut data_reader = std::io::Cursor::new(data);
///
/// let mut reader = decompress(Compression::GZip, &mut data_reader).unwrap();
///
/// let mut destination = Vec::<u8>::new();
///
/// reader.read_to_end(&mut destination).unwrap();
/// ```
pub fn decompress<'a>(
    compression: Compression,
    compressed_data: &'a mut impl Read,
) -> Result<Box<dyn Read + 'a>> {
    match compression {
        Compression::Unknown => Err(Error::new(
            ErrorKind::Other,
            "Cannot decompress for Compression Unknown",
        )),
        Compression::None => Ok(Box::new(compressed_data)),
        Compression::GZip => Ok(Box::new(GzDecoder::new(compressed_data))),
        Compression::Brotli => Ok(Box::new(BrotliDecoder::new(compressed_data, 4096))),
        Compression::ZStd => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                Ok(Box::new(ZSTDDecoder::new(compressed_data)?))
            }
            #[cfg(target_arch = "wasm32")]
            {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "ZStd decompression is not supported on WASM targets",
                ))
            }
        },
    }
}

/// Async version of [`decompress`].
///
/// Returns a new instance of [`futures::io::AsyncRead`](https://docs.rs/futures/latest/futures/io/trait.AsyncRead.html) that will emit uncompressed data from an the underlying reader.
///
/// # Arguments
/// * `compression` - Compression to use
/// * `compressed_data` - Underlying reader with compressed data
///
/// # Errors
/// Will return [`Err`] if `compression` is set to [`Compression::Unknown`],there was an
/// error while creating the zstd decoder.
///
#[cfg(feature = "async")]
pub fn decompress_async<'a>(
    compression: Compression,
    compressed_data: &'a mut (impl AsyncRead + Unpin + Send),
) -> Result<Box<dyn AsyncRead + Unpin + Send + 'a>> {
    match compression {
        Compression::Unknown => Err(Error::new(
            ErrorKind::Other,
            "Cannot decompress for Compression Unknown",
        )),
        Compression::None => Ok(Box::new(compressed_data)),
        Compression::GZip => Ok(Box::new(AsyncGzipDecoder::new(BufReader::new(
            compressed_data,
        )))),
        Compression::Brotli => Ok(Box::new(AsyncBrotliDecoder::new(BufReader::new(
            compressed_data,
        )))),
        Compression::ZStd => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                Ok(Box::new(AsyncZstdDecoder::new(BufReader::new(
                    compressed_data,
                ))))
            }
            #[cfg(target_arch = "wasm32")]
            {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "ZStd decompression is not supported on WASM targets",
                ))
            }
        },
    }
}

/// Decompresses a byte slice and returns the result as a new [`Vec<u8>`].
///
/// # Arguments
/// * `compression` - Compression to use
/// * `data` - Data to decompress
///
/// # Errors
/// Will return [`Err`] if...
/// - `compression` is set to [`Compression::Unknown`]
/// - there was an error while creating the zstd decoder
/// - there was an error reading the `data`
/// - `data` is not compressed correctly
///
pub fn decompress_all(compression: Compression, data: &[u8]) -> Result<Vec<u8>> {
    let mut data_reader = Cursor::new(data);

    let mut reader = decompress(compression, &mut data_reader)?;

    let mut destination = Vec::<u8>::new();

    reader.read_to_end(&mut destination)?;

    Ok(destination)
}

// Test module removed - test files are not included in this fork
