//! SVG writing operations.

use std::{fs::File, io::{BufWriter, Write}, path::Path};

use anyhow::{Context, Result};

pub(crate) struct SvgWriter {
    writer: BufWriter<File>
}

/// String-based SVG writer for WASM/browser use
pub(crate) struct SvgStringWriter {
    buffer: Vec<u8>
}

/// Implement std::io::Write so `write!` / `writeln!` work.
impl Write for SvgWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.writer.write(buf) }

    fn flush(&mut self) -> std::io::Result<()> { self.writer.flush() }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> { self.writer.write_all(buf) }
}

impl Write for SvgStringWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        std::io::Result::Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { std::io::Result::Ok(()) }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.buffer.extend_from_slice(buf);
        std::io::Result::Ok(())
    }
}

impl SvgStringWriter {
    /// Create a new string-based SVG writer
    pub(crate) fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Get the SVG string
    pub(crate) fn into_string(self) -> Result<String> {
        String::from_utf8(self.buffer)
            .context("[io::svg] SVG output is not valid UTF-8")
    }
}

impl SvgWriter {
    /// Create a new SVG writer to a file path
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("[io::svg] Failed to create {}", path.display()))?;

        Ok(Self { writer: BufWriter::new(file) })
    }

    /// Write the SVG header, including the XML declaration and opening <svg> tag.
    pub(crate) fn write_header(&mut self, width: f64, height: f64, margin: f64, scale: f64, bounds: &geo::Rect) -> Result<()> {
        write_svg_header(self, width, height, margin, scale, bounds)
    }
    
    /// Write SVG styles for map features.
    pub(crate) fn write_styles(&mut self) -> Result<()> {
        write_svg_styles(self)
    }

    /// Write the closing </svg> tag.
    pub(crate) fn write_footer(&mut self) -> Result<()> {
        write_svg_footer(self)
    }
}

impl SvgStringWriter {
    /// Write the SVG header, including the XML declaration and opening <svg> tag.
    pub(crate) fn write_header(&mut self, width: f64, height: f64, margin: f64, scale: f64, bounds: &geo::Rect) -> Result<()> {
        write_svg_header(self, width, height, margin, scale, bounds)
    }
    
    /// Write SVG styles for map features.
    pub(crate) fn write_styles(&mut self) -> Result<()> {
        write_svg_styles(self)
    }

    /// Write the closing </svg> tag.
    pub(crate) fn write_footer(&mut self) -> Result<()> {
        write_svg_footer(self)
    }
}

/// Write SVG header to any writer (standalone function).
pub(crate) fn write_svg_header<W: Write>(writer: &mut W, width: f64, height: f64, margin: f64, scale: f64, bounds: &geo::Rect) -> Result<()> {
    writeln!(writer, r##"<?xml version="1.0" encoding="UTF-8" standalone="no"?>"##)?;
    writeln!(writer, r##"<svg xmlns="http://www.w3.org/2000/svg" 
        width="{width}" height="{height}"
        viewBox="0 0 {width} {height}"
        data-lon-min="{lon_min}" data-lon-max="{lon_max}"
        data-lat-min="{lat_min}" data-lat-max="{lat_max}"
        data-margin="{margin}" data-scale="{scale}">"##,
        lon_min = bounds.min().x,
        lon_max = bounds.max().x,
        lat_min = bounds.min().y,
        lat_max = bounds.max().y,
    )?;
    writeln!(writer, r##"<rect width="100%" height="100%" fill="#ffffff"/>"##)?;
    Ok(())
}

/// Write SVG styles to any writer (standalone function).
pub(crate) fn write_svg_styles<W: Write>(writer: &mut W) -> Result<()> {
    writeln!(writer, r##"<defs>
<style>
    .blk {{ fill: #e5e7eb; stroke: #111827; stroke-width: 0.5; fill-opacity: 0.85; }}
    .edge {{ stroke: #2563eb; stroke-opacity: 0.35; stroke-width: 0.6; }}
    .dist {{ vector-effect: non-scaling-stroke; }}
</style>
</defs>"##)?;
    Ok(())
}

/// Write SVG footer to any writer (standalone function).
pub(crate) fn write_svg_footer<W: Write>(writer: &mut W) -> Result<()> {
    writeln!(writer, "</svg>")?;
    Ok(())
}
