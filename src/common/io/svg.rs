use std::{fs::File, io::{BufWriter, Write}, path::Path};

use anyhow::{Context, Ok, Result};

pub(crate) struct SvgWriter {
    writer: BufWriter<File>
}

/// Implement std::io::Write so `write!` / `writeln!` work.
impl Write for SvgWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.writer.write(buf) }

    fn flush(&mut self) -> std::io::Result<()> { self.writer.flush() }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> { self.writer.write_all(buf) }
}

impl SvgWriter {
    /// Create a new SVG writer to a file path
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("[to_svg] Failed to create {}", path.display()))?;

        Ok(Self { writer: BufWriter::new(file) })
    }

    /// Write the SVG header, including the XML declaration and opening <svg> tag.
    pub(crate) fn write_header(&mut self, width: f64, height: f64) -> Result<()> {
        writeln!(self, r##"<?xml version="1.0" encoding="UTF-8" standalone="no"?>"##)?;
        writeln!(self, r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">"##)?;
        writeln!(self, r##"<rect width="100%" height="100%" fill="#ffffff"/>"##)?;
        Ok(())
    }
    
    /// Write SVG styles for map features.
    pub(crate) fn write_styles(&mut self) -> Result<()> {
        writeln!(self, r##"<defs>
<style>
    .blk {{ fill: #e5e7eb; stroke: #111827; stroke-width: 0.5; fill-opacity: 0.85; }}
    .edge {{ stroke: #2563eb; stroke-opacity: 0.35; stroke-width: 0.6; }}
    .dist {{ vector-effect: non-scaling-stroke; }}
</style>
</defs>"##)?;
        Ok(())
    }

    /// Write the closing </svg> tag.
    pub(crate) fn write_footer(&mut self) -> Result<()> {
        writeln!(self, "</svg>")?;
        Ok(())
    }
}
