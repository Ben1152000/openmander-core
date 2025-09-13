use std::{fs::File, io::{Seek, Write}, path::{Path, PathBuf}, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::{blocking::Client, redirect::Policy, StatusCode};
use tempfile::NamedTempFile;

/// Write-then-rename wrapper for atomic big-file outputs
struct PendingWrite {
    target: PathBuf,
    tmp: Option<(NamedTempFile, bool)>, // (file, need_fsync_dir)
}

impl PendingWrite {
    /// Open a file for a big write.
    fn open(target: &Path, force: bool) -> Result<Self> {
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        if !force && target.exists() {
            bail!("Refusing to overwrite existing file: {} (use --force)", target.display());
        }
        let need_fsync_dir = target.parent().is_some();
        let tmp = NamedTempFile::new_in(target.parent().unwrap_or(Path::new(".")))
            .context("create temp file")?;

        Ok(Self { target: target.to_path_buf(), tmp: Some((tmp, need_fsync_dir)) })
    }

    /// Finalize the big write.
    fn finalize(&mut self) -> Result<()> {
        let (tmp, need_fsync_dir) = self.tmp.take().expect("not finalized");
        tmp.as_file().sync_all().ok(); // best-effort fsync file
        tmp.persist(&self.target)
            .with_context(|| format!("rename to {}", self.target.display()))?;
        if need_fsync_dir {
            if let Some(dir) = self.target.parent() {
                let _ = File::open(dir).and_then(|f| f.sync_all());
            }
        }
        Ok(())
    }
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

/// Download a large file from `file_url` to `out_path`.
pub fn download_big_file(file_url: String, out_path: &PathBuf, force: bool) -> Result<()> {
    // Safe big-file write (tempfile -> atomic rename), no accidental overwrite unless --force
    let mut sink = PendingWrite::open(&out_path, force)?;

    let mut resp = reqwest::blocking::get(&file_url)
        .with_context(|| format!("GET {file_url}"))?
        .error_for_status()
        .with_context(|| format!("GET {file_url} returned error status"))?;

    std::io::copy(&mut resp, &mut sink).with_context(|| format!("write {}", out_path.display()))?;

    sink.finalize()?;
    Ok(())
}

/// Lightweight existence check for a remote file.
/// Returns Ok(true) if it exists, Ok(false) if it's 404/410, Err(_) otherwise.
pub fn remote_file_exists(url: &str) -> Result<bool> {
    let client = Client::builder()
        .user_agent("openmander/0.1 (+https://github.com/Ben1152000/openmander-core)")
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(10))
        .build()?;

    // Try HEAD first
    match client.head(url).send() {
        Ok(resp) => match resp.status() {
            StatusCode::OK => return Ok(true),
            StatusCode::NOT_FOUND | StatusCode::GONE => return Ok(false),
            // Some servers don’t like HEAD; fall through to range GET.
            _ => {}
        },
        Err(_) => {} // fall through to range GET
    }

    // Fallback: GET first byte only
    let resp = client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()?;

    match resp.status() {
        StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(true),
        StatusCode::NOT_FOUND | StatusCode::GONE => Ok(false),
        s => Err(anyhow!("unexpected status {} probing {}", s, url)),
    }
}
