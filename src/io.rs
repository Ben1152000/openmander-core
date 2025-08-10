use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

pub fn assert_not_stdout(path: &Path) -> Result<()> {
    if path == Path::new("-") {
        bail!("stdout is not supported; provide a real file path.");
    }
    Ok(())
}

pub fn looks_like_dir(path: &Path) -> bool {
    path.extension().is_none() && !path.exists()
}

/// Write-then-rename wrapper for atomic big-file outputs
pub struct PendingWrite {
    target: PathBuf,
    tmp: Option<(NamedTempFile, bool)>, // (file, need_fsync_dir)
}

pub fn open_for_big_write(target: &Path, force: bool) -> Result<PendingWrite> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    if !force && target.exists() {
        bail!("Refusing to overwrite existing file: {} (use --force)", target.display());
    }
    let need_fsync_dir = target.parent().is_some();
    let tmp = NamedTempFile::new_in(target.parent().unwrap_or(Path::new(".")))
        .context("create temp file")?;

    Ok(PendingWrite { target: target.to_path_buf(), tmp: Some((tmp, need_fsync_dir)) })
}

impl Write for PendingWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tmp.as_mut().unwrap().0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.tmp.as_mut().unwrap().0.flush()
    }
}
impl Seek for PendingWrite {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.tmp.as_mut().unwrap().0.as_file_mut().seek(pos)
    }
}

pub fn finalize_big_write(mut pending: PendingWrite) -> Result<()> {
    let (tmp, need_fsync_dir) = pending.tmp.take().expect("not finalized");
    tmp.as_file().sync_all().ok(); // best-effort fsync file
    tmp.persist(&pending.target)
        .with_context(|| format!("rename to {}", pending.target.display()))?;
    if need_fsync_dir {
        if let Some(dir) = pending.target.parent() {
            let _ = File::open(dir).and_then(|f| f.sync_all());
        }
    }
    Ok(())
}