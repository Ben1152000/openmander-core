//! WKB writing operations.

use anyhow::{Context, Result};
use geo::{MultiPolygon, Polygon};
use std::io::{Read, Write};

/// WKB geometry type for Polygon
const WKB_POLYGON: u32 = 3;
/// WKB geometry type for MultiPolygon
const WKB_MULTIPOLYGON: u32 = 6;
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
#[allow(unused)]
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

/// Write a MultiPolygon to WKB format.
pub(crate) fn multipolygon_to_wkb(mp: &MultiPolygon<f64>) -> Result<Vec<u8>> {
    let mut wkb = Vec::new();

    // Byte order (little endian)
    wkb.write_all(&[WKB_LE])?;

    // Geometry type (MultiPolygon)
    wkb.write_all(&WKB_MULTIPOLYGON.to_le_bytes())?;

    // Number of polygons
    let num_polygons = mp.0.len() as u32;
    wkb.write_all(&num_polygons.to_le_bytes())?;

    // Write each polygon
    for poly in &mp.0 {
        let poly_wkb = polygon_to_wkb(poly)?;
        wkb.write_all(&poly_wkb)?;
    }

    Ok(wkb)
}
