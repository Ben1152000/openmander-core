//! WKB reading operations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use geo::Polygon;
use std::io::Read;

/// WKB geometry type for Polygon
const WKB_POLYGON: u32 = 3;
/// WKB byte order: little endian
const WKB_LE: u8 = 1;

/// Magic bytes for WKB hull file format: "OMHK" (OpenMander Hull)
const MAGIC: &[u8] = b"OMHK";
/// Format version (currently 1)
const VERSION: u8 = 1;

/// Read a Polygon from WKB format (minimal implementation)
fn polygon_from_wkb(wkb_bytes: &[u8]) -> Result<Polygon<f64>> {
    let mut cursor = std::io::Cursor::new(wkb_bytes);
    
    // Read byte order
    let mut byte_order = [0u8; 1];
    cursor.read_exact(&mut byte_order)
        .context("[io::wkb::read] Failed to read byte order")?;
    let is_le = byte_order[0] == WKB_LE;
    
    // Read geometry type
    let mut geom_type_bytes = [0u8; 4];
    cursor.read_exact(&mut geom_type_bytes)
        .context("[io::wkb::read] Failed to read geometry type")?;
    let geom_type = if is_le {
        u32::from_le_bytes(geom_type_bytes)
    } else {
        u32::from_be_bytes(geom_type_bytes)
    };
    
    if geom_type != WKB_POLYGON {
        return Err(anyhow::anyhow!("[io::wkb::read] Expected Polygon geometry type, got {}", geom_type));
    }
    
    // Read number of rings
    let mut num_rings_bytes = [0u8; 4];
    cursor.read_exact(&mut num_rings_bytes)
        .context("[io::wkb::read] Failed to read number of rings")?;
    let num_rings = if is_le {
        u32::from_le_bytes(num_rings_bytes)
    } else {
        u32::from_be_bytes(num_rings_bytes)
    };
    
    if num_rings == 0 {
        return Err(anyhow::anyhow!("[io::wkb::read] Polygon must have at least one ring"));
    }
    
    // Read exterior ring
    let mut exterior_len_bytes = [0u8; 4];
    cursor.read_exact(&mut exterior_len_bytes)
        .context("[io::wkb::read] Failed to read exterior ring length")?;
    let exterior_len = if is_le {
        u32::from_le_bytes(exterior_len_bytes)
    } else {
        u32::from_be_bytes(exterior_len_bytes)
    };
    
    let mut exterior_coords = Vec::with_capacity(exterior_len as usize);
    for _ in 0..exterior_len {
        let mut x_bytes = [0u8; 8];
        let mut y_bytes = [0u8; 8];
        cursor.read_exact(&mut x_bytes)
            .context("[io::wkb::read] Failed to read exterior x coordinate")?;
        cursor.read_exact(&mut y_bytes)
            .context("[io::wkb::read] Failed to read exterior y coordinate")?;
        let x = if is_le { f64::from_le_bytes(x_bytes) } else { f64::from_be_bytes(x_bytes) };
        let y = if is_le { f64::from_le_bytes(y_bytes) } else { f64::from_be_bytes(y_bytes) };
        exterior_coords.push(geo::Coord { x, y });
    }
    let exterior = geo::LineString::from(exterior_coords);
    
    // Read interior rings
    let mut interiors = Vec::new();
    for _ in 1..num_rings {
        let mut ring_len_bytes = [0u8; 4];
        cursor.read_exact(&mut ring_len_bytes)
            .context("[io::wkb::read] Failed to read interior ring length")?;
        let ring_len = if is_le {
            u32::from_le_bytes(ring_len_bytes)
        } else {
            u32::from_be_bytes(ring_len_bytes)
        };
        
        let mut ring_coords = Vec::with_capacity(ring_len as usize);
        for _ in 0..ring_len {
            let mut x_bytes = [0u8; 8];
            let mut y_bytes = [0u8; 8];
            cursor.read_exact(&mut x_bytes)
                .context("[io::wkb::read] Failed to read interior x coordinate")?;
            cursor.read_exact(&mut y_bytes)
                .context("[io::wkb::read] Failed to read interior y coordinate")?;
            let x = if is_le { f64::from_le_bytes(x_bytes) } else { f64::from_be_bytes(x_bytes) };
            let y = if is_le { f64::from_le_bytes(y_bytes) } else { f64::from_be_bytes(y_bytes) };
            ring_coords.push(geo::Coord { x, y });
        }
        interiors.push(geo::LineString::from(ring_coords));
    }
    
    Ok(Polygon::new(exterior, interiors))
}

/// Read hulls from WKB format bytes.
pub(crate) fn read_hulls_from_wkb_bytes(bytes: &[u8]) -> Result<Vec<Polygon<f64>>> {
    let mut cursor = std::io::Cursor::new(bytes);
    
    // Read and verify magic
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)
        .context("[io::wkb::read] Failed to read magic bytes")?;
    if magic != MAGIC {
        return Err(anyhow::anyhow!("[io::wkb::read] Invalid WKB hull file: bad magic bytes"));
    }
    
    // Read version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)
        .context("[io::wkb::read] Failed to read version")?;
    if version[0] != VERSION {
        return Err(anyhow::anyhow!("[io::wkb::read] Unsupported WKB hull file version: {}", version[0]));
    }
    
    // Read count
    let mut count_bytes = [0u8; 4];
    cursor.read_exact(&mut count_bytes)
        .context("[io::wkb::read] Failed to read hull count")?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    
    // Read compression flag
    let mut compressed_flag = [0u8; 1];
    cursor.read_exact(&mut compressed_flag)
        .context("[io::wkb::read] Failed to read compression flag")?;
    let is_compressed = compressed_flag[0] != 0;
    
    // Read and decompress data if needed
    let mut data = Vec::new();
    cursor.read_to_end(&mut data)
        .context("[io::wkb::read] Failed to read WKB data")?;
    
    let wkb_data = if is_compressed {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .context("[io::wkb::read] Failed to decompress WKB data")?;
        decompressed
    } else {
        data
    };
    
    // Parse WKB geometries
    let mut cursor = std::io::Cursor::new(wkb_data);
    let mut hulls = Vec::with_capacity(count);
    
    for _ in 0..count {
        // Read length
        let mut len_bytes = [0u8; 4];
        cursor.read_exact(&mut len_bytes)
            .context("[io::wkb::read] Failed to read WKB length")?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read WKB bytes
        let mut wkb_bytes = vec![0u8; len];
        cursor.read_exact(&mut wkb_bytes)
            .context("[io::wkb::read] Failed to read WKB bytes")?;
        
        // Parse WKB to Polygon
        let polygon = polygon_from_wkb(&wkb_bytes)
            .context("[io::wkb::read] Failed to parse WKB polygon")?;
        hulls.push(polygon);
    }
    
    Ok(hulls)
}
