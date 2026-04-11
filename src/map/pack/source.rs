use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};

/// Read-only access to pack files by pack-relative path, e.g.
/// "data/block.parquet", "adj/block.csr.bin", "manifest.json".
pub trait PackSource: Send + Sync {
    fn get(&self, rel: &str) -> Result<Arc<[u8]>>;
    fn has(&self, rel: &str) -> bool;

    /// Open a file for reading, **transparently decompressing gzip** if the file ends with
    /// `.gz` and the bytes start with the gzip magic `\x1f\x8b`.
    ///
    /// The returned stream always yields the **raw (uncompressed) content** regardless of
    /// whether the bytes on disk / in memory are already decompressed (e.g. a browser HTTP
    /// client that applied `Content-Encoding: gzip` before storing bytes in a `MemPack`) or
    /// still compressed (e.g. `DiskPack` reading a file directly from disk).
    ///
    /// The default implementation calls [`Self::get`], sniffs the first two bytes, and wraps
    /// in a `GzDecoder` only when the magic matches.  Disk-backed implementations should
    /// override this to stream from the file without a large intermediate buffer.
    fn open_read(&self, rel: &str) -> Result<Box<dyn std::io::Read + Send + 'static>> {
        let bytes = self.get(rel)?;
        if bytes.starts_with(b"\x1f\x8b") {
            Ok(Box::new(flate2::read::GzDecoder::new(std::io::Cursor::new(bytes))))
        } else {
            Ok(Box::new(std::io::Cursor::new(bytes)))
        }
    }
}

/// Write access to pack files by pack-relative path.
/// Used by disk writers and in-memory pack assembly.
pub trait PackSink: Send + Sync {
    fn put(&mut self, rel: &str, bytes: &[u8]) -> Result<()>;
}

/// Simple disk-based pack.
pub struct DiskPack {
    root: PathBuf,
}

impl DiskPack {
    pub fn new(root: impl Into<PathBuf>) -> Self { Self { root: root.into() } }

    fn full(&self, rel: &str) -> PathBuf { self.root.join(rel) }
}

impl PackSource for DiskPack {
    fn get(&self, rel: &str) -> Result<Arc<[u8]>> {
        Ok(Arc::from(std::fs::read(self.full(rel))?))
    }

    fn has(&self, rel: &str) -> bool { self.full(rel).exists() }

    fn open_read(&self, rel: &str) -> Result<Box<dyn std::io::Read + Send + 'static>> {
        let buf = std::io::BufReader::new(std::fs::File::open(self.full(rel))?);
        if rel.ends_with(".gz") {
            Ok(Box::new(flate2::read::GzDecoder::new(buf)))
        } else {
            Ok(Box::new(buf))
        }
    }
}

impl PackSink for DiskPack {
    fn put(&mut self, rel: &str, bytes: &[u8]) -> Result<()> {
        let path = self.full(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

/// Simple in-memory pack.
/// Keys are pack-relative paths, e.g. "data/block.parquet".
#[derive(Default, Clone)]
pub struct MemPack {
    pub(crate) files: HashMap<String, Arc<[u8]>>,
}

impl MemPack {
    pub fn new(files: HashMap<String, Arc<[u8]>>) -> Self { Self { files } }
}

impl PackSource for MemPack {
    fn get(&self, rel: &str) -> Result<Arc<[u8]>> {
        self.files.get(rel).cloned()
            .ok_or_else(|| anyhow!("missing pack file: {rel}"))
    }

    fn has(&self, rel: &str) -> bool { self.files.contains_key(rel) }
}

impl PackSink for MemPack {
    fn put(&mut self, rel: &str, bytes: &[u8]) -> Result<()> {
        self.files.insert(rel.to_string(), Arc::from(bytes.to_vec()));
        Ok(())
    }
}
