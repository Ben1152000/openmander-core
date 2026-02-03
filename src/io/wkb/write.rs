//! WKB writing operations.

use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression as Flate2Compression;
use geo::Polygon;
use std::io::Write;

/// WKB geometry type for Polygon
const WKB_POLYGON: u32 = 3;
/// WKB byte order: little endian
const WKB_LE: u8 = 1;

/// Magic bytes for WKB hull file format: "OMHK" (OpenMander Hull)
const MAGIC: &[u8] = b"OMHK";
/// Format version (currently 1)
const VERSION: u8 = 1;

/// Write a Polygon to WKB format (minimal implementation)
fn polygon_to_wkb(poly: &Polygon<f64>) -> Result<Vec<u8>> {
    let mut wkb = Vec::new();
    
    // Byte order (little endian)
    wkb.write_all(&[WKB_LE])?;
    
    // Geometry type (Polygon)
    wkb.write_all(&WKB_POLYGON.to_le_bytes())?;
    
    // Number of rings (1 exterior + interiors)
    let num_rings = (1 + poly.interiors().len()) as u32;
    wkb.write_all(&num_rings.to_le_bytes())?;
    
    // Exterior ring
    let exterior = poly.exterior();
    wkb.write_all(&(exterior.0.len() as u32).to_le_bytes())?;
    for coord in exterior.coords() {
        wkb.write_all(&coord.x.to_le_bytes())?;
        wkb.write_all(&coord.y.to_le_bytes())?;
    }
    
    // Interior rings
    for interior in poly.interiors() {
        wkb.write_all(&(interior.0.len() as u32).to_le_bytes())?;
        for coord in interior.coords() {
            wkb.write_all(&coord.x.to_le_bytes())?;
            wkb.write_all(&coord.y.to_le_bytes())?;
        }
    }
    
    Ok(wkb)
}

/// Write hulls to WKB format bytes.
pub(crate) fn write_hulls_to_wkb_bytes(hulls: &[Polygon<f64>], compress: bool) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    
    // Write header
    out.write_all(MAGIC)
        .context("[io::wkb::write] Failed to write magic bytes")?;
    out.write_all(&[VERSION])
        .context("[io::wkb::write] Failed to write version")?;
    out.write_all(&(hulls.len() as u32).to_le_bytes())
        .context("[io::wkb::write] Failed to write hull count")?;
    out.write_all(&[if compress { 1 } else { 0 }])
        .context("[io::wkb::write] Failed to write compression flag")?;
    
    // Write WKB data
    let mut wkb_data = Vec::new();
    for hull in hulls {
        let wkb_bytes = polygon_to_wkb(hull)
            .context("[io::wkb::write] Failed to convert polygon to WKB")?;
        let len = wkb_bytes.len() as u32;
        wkb_data.write_all(&len.to_le_bytes())?;
        wkb_data.write_all(&wkb_bytes)?;
    }
    
    // Compress if requested
    if compress {
        let mut encoder = GzEncoder::new(Vec::new(), Flate2Compression::default());
        encoder.write_all(&wkb_data)
            .context("[io::wkb::write] Failed to compress WKB data")?;
        let compressed = encoder.finish()
            .context("[io::wkb::write] Failed to finish compression")?;
        out.write_all(&compressed)?;
    } else {
        out.write_all(&wkb_data)?;
    }
    
    Ok(out)
}
