use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as Flate2Compression;
use geo::Polygon;
use std::io::{Read, Write};

/// Magic bytes for WKB hull file format: "OMHK" (OpenMander Hull)
const MAGIC: &[u8] = b"OMHK";
/// Format version (currently 1)
const VERSION: u8 = 1;

/// WKB geometry type for Polygon
const WKB_POLYGON: u32 = 3;
/// WKB byte order: little endian
const WKB_LE: u8 = 1;

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

/// Read a Polygon from WKB format (minimal implementation)
fn polygon_from_wkb(wkb_bytes: &[u8]) -> Result<Polygon<f64>> {
    let mut cursor = std::io::Cursor::new(wkb_bytes);
    
    // Read byte order
    let mut byte_order = [0u8; 1];
    cursor.read_exact(&mut byte_order)?;
    let is_le = byte_order[0] == WKB_LE;
    
    // Read geometry type
    let mut geom_type_bytes = [0u8; 4];
    cursor.read_exact(&mut geom_type_bytes)?;
    let geom_type = if is_le {
        u32::from_le_bytes(geom_type_bytes)
    } else {
        u32::from_be_bytes(geom_type_bytes)
    };
    
    if geom_type != WKB_POLYGON {
        return Err(anyhow::anyhow!("Expected Polygon geometry type, got {}", geom_type));
    }
    
    // Read number of rings
    let mut num_rings_bytes = [0u8; 4];
    cursor.read_exact(&mut num_rings_bytes)?;
    let num_rings = if is_le {
        u32::from_le_bytes(num_rings_bytes)
    } else {
        u32::from_be_bytes(num_rings_bytes)
    };
    
    if num_rings == 0 {
        return Err(anyhow::anyhow!("Polygon must have at least one ring"));
    }
    
    // Read exterior ring
    let mut exterior_len_bytes = [0u8; 4];
    cursor.read_exact(&mut exterior_len_bytes)?;
    let exterior_len = if is_le {
        u32::from_le_bytes(exterior_len_bytes)
    } else {
        u32::from_be_bytes(exterior_len_bytes)
    };
    
    let mut exterior_coords = Vec::with_capacity(exterior_len as usize);
    for _ in 0..exterior_len {
        let mut x_bytes = [0u8; 8];
        let mut y_bytes = [0u8; 8];
        cursor.read_exact(&mut x_bytes)?;
        cursor.read_exact(&mut y_bytes)?;
        let x = if is_le { f64::from_le_bytes(x_bytes) } else { f64::from_be_bytes(x_bytes) };
        let y = if is_le { f64::from_le_bytes(y_bytes) } else { f64::from_be_bytes(y_bytes) };
        exterior_coords.push(geo::Coord { x, y });
    }
    let exterior = geo::LineString::from(exterior_coords);
    
    // Read interior rings
    let mut interiors = Vec::new();
    for _ in 1..num_rings {
        let mut ring_len_bytes = [0u8; 4];
        cursor.read_exact(&mut ring_len_bytes)?;
        let ring_len = if is_le {
            u32::from_le_bytes(ring_len_bytes)
        } else {
            u32::from_be_bytes(ring_len_bytes)
        };
        
        let mut ring_coords = Vec::with_capacity(ring_len as usize);
        for _ in 0..ring_len {
            let mut x_bytes = [0u8; 8];
            let mut y_bytes = [0u8; 8];
            cursor.read_exact(&mut x_bytes)?;
            cursor.read_exact(&mut y_bytes)?;
            let x = if is_le { f64::from_le_bytes(x_bytes) } else { f64::from_be_bytes(x_bytes) };
            let y = if is_le { f64::from_le_bytes(y_bytes) } else { f64::from_be_bytes(y_bytes) };
            ring_coords.push(geo::Coord { x, y });
        }
        interiors.push(geo::LineString::from(ring_coords));
    }
    
    Ok(Polygon::new(exterior, interiors))
}

/// Write hulls to WKB format bytes.
/// Format: [magic: 4 bytes][version: 1 byte][count: u32][compressed: 1 byte][data...]
/// If compressed: data is gzipped, otherwise raw WKB
/// Data format: repeated [len: u32][wkb_bytes: len bytes]
pub(crate) fn write_hulls_to_wkb_bytes(hulls: &[Polygon<f64>], compress: bool) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    
    // Write header
    out.write_all(MAGIC)?;
    out.write_all(&[VERSION])?;
    out.write_all(&(hulls.len() as u32).to_le_bytes())?;
    out.write_all(&[if compress { 1 } else { 0 }])?;
    
    // Write WKB data
    let mut wkb_data = Vec::new();
    for hull in hulls {
        let wkb_bytes = polygon_to_wkb(hull)?;
        let len = wkb_bytes.len() as u32;
        wkb_data.write_all(&len.to_le_bytes())?;
        wkb_data.write_all(&wkb_bytes)?;
    }
    
    // Compress if requested
    if compress {
        let mut encoder = GzEncoder::new(Vec::new(), Flate2Compression::default());
        encoder.write_all(&wkb_data)?;
        let compressed = encoder.finish()?;
        out.write_all(&compressed)?;
    } else {
        out.write_all(&wkb_data)?;
    }
    
    Ok(out)
}

/// Read hulls from WKB format bytes.
pub(crate) fn read_hulls_from_wkb_bytes(bytes: &[u8]) -> Result<Vec<Polygon<f64>>> {
    let mut cursor = std::io::Cursor::new(bytes);
    
    // Read and verify magic
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(anyhow::anyhow!("Invalid WKB hull file: bad magic bytes"));
    }
    
    // Read version
    let mut version = [0u8; 1];
    cursor.read_exact(&mut version)?;
    if version[0] != VERSION {
        return Err(anyhow::anyhow!("Unsupported WKB hull file version: {}", version[0]));
    }
    
    // Read count
    let mut count_bytes = [0u8; 4];
    cursor.read_exact(&mut count_bytes)?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    
    // Read compression flag
    let mut compressed_flag = [0u8; 1];
    cursor.read_exact(&mut compressed_flag)?;
    let is_compressed = compressed_flag[0] != 0;
    
    // Read and decompress data if needed
    let mut data = Vec::new();
    cursor.read_to_end(&mut data)?;
    
    let wkb_data = if is_compressed {
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
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
        cursor.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read WKB bytes
        let mut wkb_bytes = vec![0u8; len];
        cursor.read_exact(&mut wkb_bytes)?;
        
        // Parse WKB to Polygon
        let polygon = polygon_from_wkb(&wkb_bytes)
            .context("Failed to parse WKB polygon")?;
        hulls.push(polygon);
    }
    
    Ok(hulls)
}
