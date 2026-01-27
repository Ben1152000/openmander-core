use anyhow::{Context, Result};
use geo::{Coord, LineString, MultiPolygon, Polygon};
use serde_json::{json, Value};

/// Write geometries to GeoJSON bytes.
pub(crate) fn write_to_geojson_bytes(geoms: &[MultiPolygon<f64>]) -> Result<Vec<u8>> {
    let features: Vec<Value> = geoms.iter().enumerate().map(|(idx, mp)| {
        // Convert MultiPolygon to GeoJSON geometry
        let mut polygons_json = Vec::new();
        for polygon in mp.0.iter() {
            let exterior: Vec<Vec<f64>> = polygon.exterior().coords()
                .map(|c| vec![c.x, c.y])
                .collect();
            let interiors: Vec<Vec<Vec<f64>>> = polygon.interiors().iter()
                .map(|ls| ls.coords().map(|c| vec![c.x, c.y]).collect())
                .collect();
            polygons_json.push(json!([exterior, interiors]));
        }
        
        json!({
            "type": "Feature",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": polygons_json
            },
            "properties": {
                "index": idx
            }
        })
    }).collect();

    let feature_collection = json!({
        "type": "FeatureCollection",
        "features": features,
    });

    serde_json::to_vec(&feature_collection).context("Failed to serialize GeoJSON to bytes")
}

/// Read geometries from GeoJSON bytes.
pub(crate) fn read_from_geojson_bytes(bytes: &[u8]) -> Result<Vec<MultiPolygon<f64>>> {
    let value: Value = serde_json::from_slice(bytes).context("Failed to parse GeoJSON bytes")?;
    let mut geoms = Vec::new();

    if let Some(features) = value["features"].as_array() {
        for feature in features {
            if let Some(geometry) = feature["geometry"].as_object() {
                if geometry["type"].as_str() == Some("MultiPolygon") {
                    if let Some(coords) = geometry["coordinates"].as_array() {
                        let multipolygon = parse_multipolygon_coords(coords)?;
                        geoms.push(multipolygon);
                    }
                }
            }
        }
    }
    Ok(geoms)
}

/// Parse GeoJSON MultiPolygon coordinates into a geo::MultiPolygon.
/// Format matches write format: [[exterior, interiors], ...]
/// where exterior is [[x, y], [x, y], ...] and interiors is [[[x, y], ...], [[x, y], ...], ...]
fn parse_multipolygon_coords(coords: &[Value]) -> Result<MultiPolygon<f64>> {
    let mut polygons = Vec::new();
    
    for polygon_coords in coords {
        if let Some(poly_array) = polygon_coords.as_array() {
            if poly_array.len() < 1 {
                continue;
            }
            
            // First element is exterior ring: [[x, y], [x, y], ...]
            let exterior_coords = poly_array.get(0)
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Invalid MultiPolygon: missing exterior ring"))?;
            let exterior = parse_ring_coords(exterior_coords)?;
            
            // Second element (if present) is interiors array: [[[x, y], ...], [[x, y], ...], ...]
            let mut interiors = Vec::new();
            if let Some(interiors_value) = poly_array.get(1) {
                if let Some(interiors_array) = interiors_value.as_array() {
                    for interior_ring in interiors_array {
                        if let Some(ring_array) = interior_ring.as_array() {
                            interiors.push(parse_ring_coords(ring_array)?);
                        }
                    }
                }
            }
            
            polygons.push(Polygon::new(exterior, interiors));
        }
    }
    
    Ok(MultiPolygon(polygons))
}

/// Parse a ring (exterior or interior) from GeoJSON coordinates.
/// Format: [[x, y], [x, y], ...]
fn parse_ring_coords(coords: &[Value]) -> Result<LineString<f64>> {
    let mut points = Vec::new();
    
    for coord_pair in coords {
        if let Some(coord_array) = coord_pair.as_array() {
            if coord_array.len() >= 2 {
                let x = coord_array[0].as_f64()
                    .ok_or_else(|| anyhow::anyhow!("Invalid coordinate: x must be a number"))?;
                let y = coord_array[1].as_f64()
                    .ok_or_else(|| anyhow::anyhow!("Invalid coordinate: y must be a number"))?;
                points.push(Coord { x, y });
            }
        }
    }
    
    // Ensure ring is closed (first point == last point)
    if !points.is_empty() && points[0] != points[points.len() - 1] {
        points.push(points[0]);
    }
    
    Ok(LineString(points))
}

