use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};

/// Read-only access to pack files by pack-relative path, e.g.
/// "data/block.parquet", "adj/block.csr.bin", "manifest.json".
pub trait PackSource: Send + Sync {
    fn get(&self, rel: &str) -> Result<Arc<[u8]>>;
    fn has(&self, rel: &str) -> bool;
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
