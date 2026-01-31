use std::io::Cursor;
use std::f64::consts::PI;

use anyhow::{Context, Result};
use geo::{BooleanOps, MultiPolygon, Coord, LineString, Polygon, Simplify};
#[cfg(feature = "pmtiles")]
use geo_types::{GeometryCollection, LineString as GeoLineString, MultiLineString, MultiPoint, MultiPolygon as GeoMultiPolygon, Point, Polygon as GeoPolygon};

/// Convert longitude to Web Mercator X coordinate (in radians)
fn lon_to_mercator_x(lon: f64) -> f64 { lon.to_radians() }

/// Convert latitude to Web Mercator Y coordinate (in radians)
fn lat_to_mercator_y(lat: f64) -> f64 { (PI / 4.0 + lat.to_radians() / 2.0).tan().ln() }

/// Convert Web Mercator X to longitude
fn mercator_x_to_lon(x: f64) -> f64 { x.to_degrees() }

/// Convert Web Mercator Y to latitude
fn mercator_y_to_lat(y: f64) -> f64 { (2.0 * (y.exp().atan() - PI / 4.0)).to_degrees() }

/// Get tile bounds in Web Mercator coordinates
fn tile_bounds(z: u8, x: u64, y: u64) -> (f64, f64, f64, f64) {
    let n = 2.0_f64.powi(z as i32);
    let min_x = (x as f64 / n) * 2.0 * PI - PI;
    let max_x = ((x + 1) as f64 / n) * 2.0 * PI - PI;
    let min_y = PI - ((y + 1) as f64 / n) * 2.0 * PI;
    let max_y = PI - (y as f64 / n) * 2.0 * PI;
    (min_x, min_y, max_x, max_y)
}

/// Convert world coordinates to tile coordinates (0-4096)
/// Coordinates may be outside [0, extent] if the point is outside the tile.
/// Use clip_ring_to_tile() to properly clip geometry to tile boundaries.
fn world_to_tile_coords(lon: f64, lat: f64, z: u8, x: u64, y: u64, extent: f64) -> (f64, f64) {
    let (tile_min_x, tile_min_y, tile_max_x, tile_max_y) = tile_bounds(z, x, y);
    let merc_x = lon_to_mercator_x(lon);
    let merc_y = lat_to_mercator_y(lat);
    
    // X increases left to right (west to east)
    let tile_x = ((merc_x - tile_min_x) / (tile_max_x - tile_min_x)) * extent;
    
    // Y in MVT: 0 = top of tile (north), extent = bottom of tile (south)
    // Higher latitude (north) -> lower tile_y
    // Lower latitude (south) -> higher tile_y
    // merc_y increases with latitude, so invert the mapping
    let tile_y = extent - ((merc_y - tile_min_y) / (tile_max_y - tile_min_y)) * extent;
    
    (tile_x, tile_y)
}

/// Clip a polygon ring to tile boundaries using Sutherland-Hodgman algorithm.
/// This properly interpolates intersection points where lines cross tile edges,
/// avoiding the "stair-step" effect from simple coordinate clamping.
/// 
/// The buffer parameter extends the clipping bounds beyond the tile edges to ensure
/// geometries that cross tile boundaries are rendered seamlessly without visible seams.
/// A buffer of 256 pixels (out of 4096 extent) is recommended for production use.
fn clip_ring_to_tile(ring: &[(f64, f64)], extent: f64, buffer: f64) -> Vec<(f64, f64)> {
    if ring.is_empty() {
        return Vec::new();
    }
    
    // Extend clipping bounds by buffer amount
    let min_bound = -buffer;
    let max_bound = extent + buffer;
    
    // Clip against each of the four tile edges in sequence
    let mut output = ring.to_vec();
    
    // Clip against left edge (x = -buffer)
    output = clip_against_edge(&output, |p| p.0 >= min_bound, |p1, p2| {
        let t = (min_bound - p1.0) / (p2.0 - p1.0);
        (min_bound, p1.1 + t * (p2.1 - p1.1))
    });
    
    // Clip against right edge (x = extent + buffer)
    output = clip_against_edge(&output, |p| p.0 <= max_bound, |p1, p2| {
        let t = (max_bound - p1.0) / (p2.0 - p1.0);
        (max_bound, p1.1 + t * (p2.1 - p1.1))
    });
    
    // Clip against top edge (y = -buffer)
    output = clip_against_edge(&output, |p| p.1 >= min_bound, |p1, p2| {
        let t = (min_bound - p1.1) / (p2.1 - p1.1);
        (p1.0 + t * (p2.0 - p1.0), min_bound)
    });
    
    // Clip against bottom edge (y = extent + buffer)
    output = clip_against_edge(&output, |p| p.1 <= max_bound, |p1, p2| {
        let t = (max_bound - p1.1) / (p2.1 - p1.1);
        (p1.0 + t * (p2.0 - p1.0), max_bound)
    });
    
    output
}

/// Helper for Sutherland-Hodgman: clip polygon against a single edge
fn clip_against_edge<F, I>(polygon: &[(f64, f64)], inside: F, intersect: I) -> Vec<(f64, f64)>
where
    F: Fn(&(f64, f64)) -> bool,
    I: Fn(&(f64, f64), &(f64, f64)) -> (f64, f64),
{
    if polygon.is_empty() {
        return Vec::new();
    }
    
    let mut output = Vec::new();
    let n = polygon.len();
    
    for i in 0..n {
        let current = &polygon[i];
        let next = &polygon[(i + 1) % n];
        
        let current_inside = inside(current);
        let next_inside = inside(next);
        
        if current_inside {
            if next_inside {
                // Both inside: add next point
                output.push(*next);
            } else {
                // Going outside: add intersection
                output.push(intersect(current, next));
            }
        } else if next_inside {
            // Coming inside: add intersection, then next point
            output.push(intersect(current, next));
            output.push(*next);
        }
        // Both outside: add nothing
    }
    
    output
}

/// Calculate signed area of a ring (for winding order detection)
/// Positive = counter-clockwise, Negative = clockwise
fn _ring_signed_area(ring: &[(f64, f64)]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    let mut area = 0.0;
    for i in 0..ring.len() {
        let j = (i + 1) % ring.len();
        area += ring[i].0 * ring[j].1;
        area -= ring[j].0 * ring[i].1;
    }
    area / 2.0
}

/// Clean a ring: remove duplicates, remove closing duplicate, ensure minimum points
fn clean_ring(ring: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    if ring.is_empty() {
        return ring;
    }
    
    // Remove consecutive duplicates
    let mut cleaned = Vec::new();
    cleaned.push(ring[0]);
    for i in 1..ring.len() {
        let prev = cleaned.last().unwrap();
        let curr = ring[i];
        // Only add if different from previous point
        if (curr.0 - prev.0).abs() > f64::EPSILON || (curr.1 - prev.1).abs() > f64::EPSILON {
            cleaned.push(curr);
        }
    }
    
    // Remove closing duplicate (if last point equals first)
    if cleaned.len() > 1 {
        let first = cleaned[0];
        let last = cleaned[cleaned.len() - 1];
        if (last.0 - first.0).abs() < f64::EPSILON && (last.1 - first.1).abs() < f64::EPSILON {
            cleaned.pop();
        }
    }
    
    // Remove immediate backtracks (A-B-A patterns)
    if cleaned.len() >= 3 {
        let mut deduped = Vec::new();
        deduped.push(cleaned[0]);
        for i in 1..cleaned.len() {
            let prev_idx = if deduped.len() >= 2 { deduped.len() - 2 } else { 0 };
            let prev = deduped[prev_idx];
            let curr = cleaned[i];
            // Skip if this point equals the point before the previous (A-B-A pattern)
            if deduped.len() >= 2 && (curr.0 - prev.0).abs() < f64::EPSILON && (curr.1 - prev.1).abs() < f64::EPSILON {
                continue;
            }
            deduped.push(curr);
        }
        cleaned = deduped;
    }
    
    // Need at least 3 distinct points for a valid ring
    if cleaned.len() < 3 {
        return Vec::new();
    }
    
    cleaned
}

/// Ensure correct winding order for MVT: outer rings clockwise, inner rings counter-clockwise
/// Note: In tile coordinates, Y increases DOWNWARD, which inverts the signed area meaning
fn _ensure_winding_order(ring: Vec<(f64, f64)>, is_hole: bool) -> Vec<(f64, f64)> {
    if ring.len() < 3 {
        return ring;
    }
    
    let area = _ring_signed_area(&ring);
    // In tile coords (Y down): positive area = clockwise, negative = counter-clockwise
    // This is opposite of standard math coordinates (Y up)
    let is_clockwise = area > 0.0;
    
    // MVT spec: outer rings should be clockwise, holes should be counter-clockwise
    let should_be_clockwise = !is_hole;
    
    if is_clockwise != should_be_clockwise {
        // Reverse the ring
        ring.into_iter().rev().collect()
    } else {
        ring
    }
}

/// Convert tile coordinates to world coordinates
fn tile_to_world_coords(tile_x: f64, tile_y: f64, z: u8, x: u64, y: u64, extent: f64) -> (f64, f64) {
    let (tile_min_x, tile_min_y, tile_max_x, tile_max_y) = tile_bounds(z, x, y);
    
    let merc_x = tile_min_x + (tile_x / extent) * (tile_max_x - tile_min_x);
    let merc_y = tile_max_y - (tile_y / extent) * (tile_max_y - tile_min_y); // Y is flipped (down)
    
    let lon = mercator_x_to_lon(merc_x);
    let lat = mercator_y_to_lat(merc_y);
    
    (lon, lat)
}

/// Convert a single tile-local point (px, py) into lon/lat
fn tile_point_to_lonlat(
    z: u8,
    tx: u64,
    ty: u64,
    extent: f32,
    px: f32,
    py: f32,
) -> (f64, f64) {
    tile_to_world_coords(px as f64, py as f64, z, tx, ty, extent as f64)
}

/// Convert geo_types::Geometry<f32> from tile coordinates to world coordinates
#[cfg(feature = "pmtiles")]
fn geometry_tile_to_world(
    z: u8,
    tx: u64,
    ty: u64,
    extent: f32,
    geom: &geo_types::Geometry<f32>,
) -> geo_types::Geometry<f32> {
    match geom {
        geo_types::Geometry::Point(p) => {
            let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
            geo_types::Geometry::Point(Point::new(lon as f32, lat as f32))
        }
        geo_types::Geometry::MultiPoint(mp) => {
            let pts = mp
                .iter()
                .map(|p| {
                    let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                    Point::new(lon as f32, lat as f32)
                })
                .collect::<Vec<_>>();
            geo_types::Geometry::MultiPoint(MultiPoint(pts))
        }
        geo_types::Geometry::LineString(ls) => {
            let coords = ls
                .points()
                .map(|p| {
                    let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                    (lon as f32, lat as f32)
                })
                .collect::<Vec<_>>();
            geo_types::Geometry::LineString(GeoLineString::from(coords))
        }
        geo_types::Geometry::MultiLineString(mls) => {
            let out = mls
                .iter()
                .map(|ls| {
                    let coords = ls
                        .points()
                        .map(|p| {
                            let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                            (lon as f32, lat as f32)
                        })
                        .collect::<Vec<_>>();
                    GeoLineString::from(coords)
                })
                .collect::<Vec<_>>();
            geo_types::Geometry::MultiLineString(MultiLineString(out))
        }
        geo_types::Geometry::Polygon(p) => {
            // Convert exterior ring
            let exterior_coords = p
                .exterior()
                .points()
                .map(|p| {
                    let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                    (lon as f32, lat as f32)
                })
                .collect::<Vec<_>>();
            let exterior = GeoLineString::from(exterior_coords);
            
            // Convert interior rings
            let interiors = p
                .interiors()
                .iter()
                .map(|ring| {
                    let coords = ring
                        .points()
                        .map(|p| {
                            let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                            (lon as f32, lat as f32)
                        })
                        .collect::<Vec<_>>();
                    GeoLineString::from(coords)
                })
                .collect::<Vec<_>>();
            
            geo_types::Geometry::Polygon(GeoPolygon::new(exterior, interiors))
        }
        geo_types::Geometry::MultiPolygon(mp) => {
            let out = mp
                .iter()
                .map(|p| {
                    // Convert exterior ring
                    let exterior_coords = p
                        .exterior()
                        .points()
                        .map(|p| {
                            let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                            (lon as f32, lat as f32)
                        })
                        .collect::<Vec<_>>();
                    let exterior = GeoLineString::from(exterior_coords);
                    
                    // Convert interior rings
                    let interiors = p
                        .interiors()
                        .iter()
                        .map(|ring| {
                            let coords = ring
                                .points()
                                .map(|p| {
                                    let (lon, lat) = tile_point_to_lonlat(z, tx, ty, extent, p.x(), p.y());
                                    (lon as f32, lat as f32)
                                })
                                .collect::<Vec<_>>();
                            GeoLineString::from(coords)
                        })
                        .collect::<Vec<_>>();
                    
                    GeoPolygon::new(exterior, interiors)
                })
                .collect::<Vec<_>>();
            geo_types::Geometry::MultiPolygon(GeoMultiPolygon(out))
        }
        geo_types::Geometry::GeometryCollection(gc) => {
            let out = gc
                .iter()
                .map(|g| geometry_tile_to_world(z, tx, ty, extent, g))
                .collect::<Vec<_>>();
            geo_types::Geometry::GeometryCollection(GeometryCollection(out))
        }
        // Additional geo::Geometry variants (geo_types re-exports geo::Geometry)
        geo_types::Geometry::Line(_) | 
        geo_types::Geometry::Rect(_) | 
        geo_types::Geometry::Triangle(_) => {
            // These geometry types are not commonly used in MVT tiles
            // For now, skip them (could be extended if needed)
            geom.clone() // Return unchanged for now
        }
    }
}

/// Convert geo_types::Geometry<f32> to geo::MultiPolygon<f64>
/// Extracts polygons from the geometry and converts to our internal representation
#[cfg(feature = "pmtiles")]
fn geometry_to_multipolygon(geom: &geo_types::Geometry<f32>) -> Option<MultiPolygon<f64>> {
    match geom {
        geo_types::Geometry::Polygon(p) => {
            // Convert geo_types::Polygon<f32> to geo::Polygon<f64>
            let exterior_coords: Vec<Coord<f64>> = p
                .exterior()
                .points()
                .map(|p| Coord {
                    x: p.x() as f64,
                    y: p.y() as f64,
                })
                .collect();
            let exterior = LineString(exterior_coords);
            
            let interiors: Vec<LineString<f64>> = p
                .interiors()
                .iter()
                .map(|ring| {
                    LineString(
                        ring.points()
                            .map(|p| Coord {
                                x: p.x() as f64,
                                y: p.y() as f64,
                            })
                            .collect(),
                    )
                })
                .collect();
            
            Some(MultiPolygon(vec![Polygon::new(exterior, interiors)]))
        }
        geo_types::Geometry::MultiPolygon(mp) => {
            let polygons: Vec<Polygon<f64>> = mp
                .iter()
                .map(|p| {
                    let exterior_coords: Vec<Coord<f64>> = p
                        .exterior()
                        .points()
                        .map(|p| Coord {
                            x: p.x() as f64,
                            y: p.y() as f64,
                        })
                        .collect();
                    let exterior = LineString(exterior_coords);
                    
                    let interiors: Vec<LineString<f64>> = p
                        .interiors()
                        .iter()
                        .map(|ring| {
                            LineString(
                                ring.points()
                                    .map(|p| Coord {
                                        x: p.x() as f64,
                                        y: p.y() as f64,
                                    })
                                    .collect(),
                            )
                        })
                        .collect();
                    
                    Polygon::new(exterior, interiors)
                })
                .collect();
            
            if polygons.is_empty() {
                None
            } else {
                Some(MultiPolygon(polygons))
            }
        }
        _ => {
            // Skip non-polygon geometries
            None
        }
    }
}

/// Write geometries to PMTiles format bytes.
/// PMTiles stores vector tiles in a single file with an index.
/// Convert longitude to tile X coordinate at a given zoom level
fn lon_to_tile_x(lon: f64, zoom: u8) -> u64 {
    let n = 2.0_f64.powi(zoom as i32);
    ((lon + 180.0) / 360.0 * n).floor() as u64
}

/// Convert latitude to tile Y coordinate at a given zoom level
fn lat_to_tile_y(lat: f64, zoom: u8) -> u64 {
    let n = 2.0_f64.powi(zoom as i32);
    let lat_rad = lat.to_radians();
    ((1.0 - lat_rad.tan().asinh() / PI) / 2.0 * n).floor() as u64
}

/// Calculate the centroid of a polygon (simple average of exterior ring coordinates)
fn _polygon_centroid(poly: &Polygon<f64>) -> Option<(f64, f64)> {
    let coords: Vec<_> = poly.exterior().coords()
        .filter(|c| c.x.is_finite() && c.y.is_finite())
        .collect();
    
    if coords.is_empty() {
        return None;
    }
    
    let sum_lon: f64 = coords.iter().map(|c| c.x).sum();
    let sum_lat: f64 = coords.iter().map(|c| c.y).sum();
    let n = coords.len() as f64;
    
    Some((sum_lon / n, sum_lat / n))
}

/// Calculate the bounding box of a polygon in lon/lat
fn polygon_bounds(poly: &Polygon<f64>) -> (f64, f64, f64, f64) {
    let mut min_lon = f64::INFINITY;
    let mut min_lat = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    
    for coord in poly.exterior().coords() {
        if coord.x.is_finite() && coord.y.is_finite() {
            min_lon = min_lon.min(coord.x);
            min_lat = min_lat.min(coord.y);
            max_lon = max_lon.max(coord.x);
            max_lat = max_lat.max(coord.y);
        }
    }
    
    (min_lon, min_lat, max_lon, max_lat)
}

/// Write multiple layers to a single PMTiles file with each layer at its appropriate zoom range.
/// This creates a single PMTiles archive containing all geometry layers, where each layer
/// is visible at its designated zoom levels (e.g., states at z4-8, blocks at z12-14).
/// 
/// Parameters:
/// - layers: Vec of (layer_name, geometries, geo_ids, min_zoom, max_zoom)
/// 
/// Returns: PMTiles file as bytes
#[cfg(feature = "pmtiles")]
pub(crate) fn write_multilayer_pmtiles_bytes(
    layers: Vec<(&str, &[MultiPolygon<f64>], Option<&[String]>, u8, u8)>
) -> Result<Vec<u8>> {
    use pmtiles2::{PMTiles, TileType, Compression as PmtilesCompression};
    use pmtiles2::util::tile_id;
    use mvt::{Tile, GeomEncoder, GeomType};
    use flate2::write::GzEncoder;
    use flate2::Compression as Flate2Compression;
    use std::io::Write;
    use std::collections::HashMap;
    
    if layers.is_empty() {
        return Err(anyhow::anyhow!("Cannot write empty layer list to PMTiles"));
    }
    
    // Calculate overall bounds from all layers
    let mut global_min_lon = f64::INFINITY;
    let mut global_min_lat = f64::INFINITY;
    let mut global_max_lon = f64::NEG_INFINITY;
    let mut global_max_lat = f64::NEG_INFINITY;
    let mut global_min_zoom = u8::MAX;
    let mut global_max_zoom = u8::MIN;
    
    for (_, geoms, _, min_zoom, max_zoom) in &layers {
        global_min_zoom = global_min_zoom.min(*min_zoom);
        global_max_zoom = global_max_zoom.max(*max_zoom);
        
        for mp in *geoms {
            for poly in &mp.0 {
                let (pmin_lon, pmin_lat, pmax_lon, pmax_lat) = polygon_bounds(poly);
                if pmin_lon.is_finite() && pmin_lat.is_finite() && 
                   pmax_lon.is_finite() && pmax_lat.is_finite() {
                    global_min_lon = global_min_lon.min(pmin_lon);
                    global_min_lat = global_min_lat.min(pmin_lat);
                    global_max_lon = global_max_lon.max(pmax_lon);
                    global_max_lat = global_max_lat.max(pmax_lat);
                }
            }
        }
    }
    
    /// Calculate simplification tolerance for a given zoom level.
    /// Returns 0.0 (no simplification) at max zoom to preserve full detail.
    fn calculate_tolerance_for_zoom(zoom: u8, max_zoom: u8) -> f64 {
        // No simplification at max zoom - preserve full detail
        if zoom >= max_zoom {
            return 0.0;
        }
        
        let tile_size_degrees = 360.0 / (2.0_f64.powi(zoom as i32));
        // Very conservative simplification at lower zooms
        tile_size_degrees / 1000.0
    }
    
    /// Simplify a MultiPolygon using Douglas-Peucker algorithm.
    fn simplify_multipolygon(mp: &MultiPolygon<f64>, tolerance: f64) -> MultiPolygon<f64> {
        let simplified_polygons: Vec<Polygon<f64>> = mp.0.iter()
            .map(|poly| {
                let simplified_exterior = poly.exterior().simplify(&tolerance);
                let simplified_interiors: Vec<LineString<f64>> = poly.interiors()
                    .iter()
                    .map(|ring| ring.simplify(&tolerance))
                    .collect();
                Polygon::new(simplified_exterior, simplified_interiors)
            })
            .collect();
        MultiPolygon(simplified_polygons)
    }
    
    // Build a map of (zoom, tile_x, tile_y) -> Vec<(layer_name, geometry_index, polygon)>
    let mut tile_geometries: HashMap<(u8, u64, u64), Vec<(&str, usize, Polygon<f64>)>> = HashMap::new();
    
    // Process each layer
    for (layer_name, geoms, _geo_ids, min_zoom, max_zoom) in &layers {
        // Process geometries per zoom level for this layer
        for zoom in *min_zoom..=*max_zoom {
            let tolerance = calculate_tolerance_for_zoom(zoom, *max_zoom);
            
            // Simplify all geometries for this zoom level
            let simplified_geoms: Vec<MultiPolygon<f64>> = geoms.iter()
                .map(|mp| simplify_multipolygon(mp, tolerance))
                .collect();
            
            // Assign simplified geometries to tiles at this zoom level
            for (idx, mp) in simplified_geoms.iter().enumerate() {
                for poly in &mp.0 {
                    let (poly_min_lon, poly_min_lat, poly_max_lon, poly_max_lat) = polygon_bounds(poly);
                    
                    if !poly_min_lon.is_finite() || !poly_min_lat.is_finite() || 
                       !poly_max_lon.is_finite() || !poly_max_lat.is_finite() {
                        continue;
                    }
                    
                    let tile_min_x = lon_to_tile_x(poly_min_lon, zoom);
                    let tile_max_x = lon_to_tile_x(poly_max_lon, zoom);
                    let tile_min_y = lat_to_tile_y(poly_max_lat, zoom);
                    let tile_max_y = lat_to_tile_y(poly_min_lat, zoom);
                    
                    for tile_x in tile_min_x..=tile_max_x {
                        for tile_y in tile_min_y..=tile_max_y {
                            tile_geometries
                                .entry((zoom, tile_x, tile_y))
                                .or_insert_with(Vec::new)
                                .push((layer_name, idx, poly.clone()));
                        }
                    }
                }
            }
        }
    }
    
    // Create PMTiles writer
    let mut pm = PMTiles::new(TileType::Mvt, PmtilesCompression::GZip);
    
    pm.min_zoom = global_min_zoom;
    pm.max_zoom = global_max_zoom;
    pm.min_longitude = global_min_lon;
    pm.min_latitude = global_min_lat;
    pm.max_longitude = global_max_lon;
    pm.max_latitude = global_max_lat;
    pm.center_zoom = (global_min_zoom + global_max_zoom) / 2;
    pm.center_longitude = (global_min_lon + global_max_lon) / 2.0;
    pm.center_latitude = (global_min_lat + global_max_lat) / 2.0;
    
    // Metadata with vector_layers for all layers
    pm.meta_data.insert("name".into(), serde_json::json!("OpenMander geometries"));
    pm.meta_data.insert("format".into(), serde_json::json!("pbf"));
    pm.meta_data.insert("type".into(), serde_json::json!("overlay"));
    pm.meta_data.insert("minzoom".into(), serde_json::json!(global_min_zoom));
    pm.meta_data.insert("maxzoom".into(), serde_json::json!(global_max_zoom));
    
    let vector_layers: Vec<_> = layers.iter().map(|(layer_name, _, _, min_zoom, max_zoom)| {
        serde_json::json!({
            "id": layer_name,
            "fields": {"index": "String"},
            "minzoom": min_zoom,
            "maxzoom": max_zoom
        })
    }).collect();
    pm.meta_data.insert("vector_layers".into(), serde_json::json!(vector_layers));
    
    let extent = 4096u32;
    let extent_f = extent as f64;
    let buffer = 256.0;
    
    // Group geometries by tile, then by layer
    let mut tiles_by_coord: HashMap<(u8, u64, u64), HashMap<&str, Vec<(usize, Polygon<f64>)>>> = HashMap::new();
    for ((zoom, tile_x, tile_y), geoms) in tile_geometries.iter() {
        let tile_layers = tiles_by_coord.entry((*zoom, *tile_x, *tile_y)).or_insert_with(HashMap::new);
        for (layer_name, idx, poly) in geoms {
            tile_layers.entry(layer_name).or_insert_with(Vec::new).push((*idx, poly.clone()));
        }
    }
    
    // Create tiles
    for ((zoom, tile_x, tile_y), layer_geoms) in tiles_by_coord.iter() {
        let mut tile = Tile::new(extent);
        
        // Create a layer for each geometry layer in this tile
        for (layer_name, polygons) in layer_geoms.iter() {
            let mut layer = tile.create_layer(layer_name);
            
            for (idx, poly) in polygons {
                let mut encoder = GeomEncoder::new(GeomType::Polygon);
                
                // Process exterior ring
                let exterior_coords: Vec<_> = poly.exterior().coords().collect();
                if exterior_coords.len() < 3 {
                    continue;
                }
                
                let exterior_tile_coords_raw: Vec<(f64, f64)> = exterior_coords.iter()
                    .filter(|coord| coord.x.is_finite() && coord.y.is_finite())
                    .map(|coord| world_to_tile_coords(coord.x, coord.y, *zoom, *tile_x, *tile_y, extent_f))
                    .collect();
                
                let exterior_clipped = clip_ring_to_tile(&exterior_tile_coords_raw, extent_f, buffer);
                let mut exterior_tile_coords: Vec<(f64, f64)> = exterior_clipped.iter()
                    .map(|(x, y)| (x.round(), y.round()))
                    .collect();
                exterior_tile_coords = clean_ring(exterior_tile_coords);
                if exterior_tile_coords.len() < 3 {
                    continue;
                }
                
                for (x, y) in exterior_tile_coords.iter() {
                    encoder = encoder.point(*x, *y)?;
                }
                encoder = encoder.complete()?;
                
                // Process interior rings
                for interior in poly.interiors() {
                    let interior_coords: Vec<_> = interior.coords().collect();
                    if interior_coords.len() < 3 {
                        continue;
                    }
                    
                    let interior_tile_coords_raw: Vec<(f64, f64)> = interior_coords.iter()
                        .filter(|coord| coord.x.is_finite() && coord.y.is_finite())
                        .map(|coord| world_to_tile_coords(coord.x, coord.y, *zoom, *tile_x, *tile_y, extent_f))
                        .collect();
                    
                    let interior_clipped = clip_ring_to_tile(&interior_tile_coords_raw, extent_f, buffer);
                    let mut interior_tile_coords: Vec<(f64, f64)> = interior_clipped.iter()
                        .map(|(x, y)| (x.round(), y.round()))
                        .collect();
                    interior_tile_coords = clean_ring(interior_tile_coords);
                    if interior_tile_coords.len() < 3 {
                        continue;
                    }
                    
                    for (x, y) in interior_tile_coords.iter() {
                        encoder = encoder.point(*x, *y)?;
                    }
                    encoder = encoder.complete()?;
                }
                
                let geom_data = encoder.encode()?;
                let mut feature = layer.into_feature(geom_data);
                feature.set_id(*idx as u64);
                feature.add_tag_string("index", &idx.to_string());
                layer = feature.into_layer();
            }
            
            tile.add_layer(layer)?;
        }
        
        // Encode and compress tile
        let tile_data = tile.to_bytes()?;
        let mut encoder = GzEncoder::new(Vec::new(), Flate2Compression::default());
        encoder.write_all(&tile_data)?;
        let compressed = encoder.finish()?;
        
        let tile_id = tile_id(*zoom, *tile_x, *tile_y);
        pm.add_tile(tile_id, compressed)?;
    }
    
    // Write PMTiles to bytes
    let mut buffer = Cursor::new(Vec::new());
    pm.to_writer(&mut buffer)?;
    
    Ok(buffer.into_inner())
}

/// This implementation converts geometries to MVT (Mapbox Vector Tiles) format
/// and stores them in a PMTiles archive at specified zoom levels.
/// 
/// Each geometry is stored in ALL tiles it intersects at each zoom level in the range.
/// This enables efficient tile-based loading where only visible tiles need to be
/// decoded, and allows visibility from zoomed-out to zoomed-in views.
/// 
/// The zoom levels determine tile resolution:
/// - z4: Good for large states (Alaska, California) visible very far zoomed out
/// - z6: Good for state outlines (visible when zoomed out)
/// - z8: Good for county boundaries
/// - z10: Good for tracts/VTDs
/// - z12: Good for block groups
/// - z14: Good for census blocks (visible when zoomed in)
#[cfg(feature = "pmtiles")]
pub(crate) fn write_to_pmtiles_bytes(geoms: &[MultiPolygon<f64>], geo_ids: Option<&[String]>, min_zoom: u8, max_zoom: u8) -> Result<Vec<u8>> {
    use pmtiles2::{PMTiles, TileType, Compression as PmtilesCompression};
    use pmtiles2::util::tile_id;
    use mvt::{Tile, GeomEncoder, GeomType};
    use flate2::write::GzEncoder;
    use flate2::Compression as Flate2Compression;
    use std::io::Write;
    use std::collections::HashMap;
    
    if geoms.is_empty() {
        return Err(anyhow::anyhow!("Cannot write empty geometry list to PMTiles"));
    }
    
    // Calculate overall bounds from all geometries (using actual bounds, not centroids)
    let mut min_lon = f64::INFINITY;
    let mut min_lat = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    
    for mp in geoms {
        for poly in &mp.0 {
            let (pmin_lon, pmin_lat, pmax_lon, pmax_lat) = polygon_bounds(poly);
            if pmin_lon.is_finite() && pmin_lat.is_finite() && 
               pmax_lon.is_finite() && pmax_lat.is_finite() {
                min_lon = min_lon.min(pmin_lon);
                min_lat = min_lat.min(pmin_lat);
                max_lon = max_lon.max(pmax_lon);
                max_lat = max_lat.max(pmax_lat);
            }
        }
    }
    
    /// Calculate simplification tolerance for a given zoom level.
    /// Tolerance is in degrees and scales with tile size at that zoom level.
    /// Lower zoom levels get higher tolerance (more aggressive simplification).
    /// Returns 0.0 (no simplification) at max zoom to preserve full detail.
    fn calculate_tolerance_for_zoom(zoom: u8, max_zoom: u8) -> f64 {
        // No simplification at max zoom - preserve full detail
        if zoom >= max_zoom {
            return 0.0;
        }
        
        // Approximate tile size in degrees at this zoom level
        // At zoom z, one tile covers roughly 360 / 2^z degrees
        let tile_size_degrees = 360.0 / (2.0_f64.powi(zoom as i32));
        
        // Use a small fraction of tile size as tolerance
        // Using 1/1000th of tile size for very conservative simplification
        // This preserves detail while still reducing file size
        // Higher zoom = smaller tiles = smaller tolerance = less simplification
        tile_size_degrees / 1000.0
    }
    
    /// Simplify a MultiPolygon using Douglas-Peucker algorithm.
    /// Returns a new simplified MultiPolygon.
    fn simplify_multipolygon(mp: &MultiPolygon<f64>, tolerance: f64) -> MultiPolygon<f64> {
        let simplified_polygons: Vec<Polygon<f64>> = mp.0.iter()
            .map(|poly| {
                // Simplify exterior ring
                let simplified_exterior = poly.exterior().simplify(&tolerance);
                
                // Simplify interior rings (holes)
                let simplified_interiors: Vec<LineString<f64>> = poly.interiors()
                    .iter()
                    .map(|ring| ring.simplify(&tolerance))
                    .collect();
                
                Polygon::new(simplified_exterior, simplified_interiors)
            })
            .collect();
        
        MultiPolygon(simplified_polygons)
    }
    
    // Build a map of (zoom, tile_x, tile_y) -> list of (geometry_index, polygon)
    // Each geometry is assigned to ALL tiles its bounding box intersects at each zoom level
    // This ensures geometries crossing tile boundaries appear in all relevant tiles
    // We simplify geometries once per zoom level before assigning to tiles
    let mut tile_geometries: HashMap<(u8, u64, u64), Vec<(usize, Polygon<f64>)>> = HashMap::new();
    
    // Process geometries per zoom level, simplifying once per zoom
    for zoom in min_zoom..=max_zoom {
        let tolerance = calculate_tolerance_for_zoom(zoom, max_zoom);
        
        // Simplify all geometries for this zoom level
        let simplified_geoms: Vec<MultiPolygon<f64>> = geoms.iter()
            .map(|mp| simplify_multipolygon(mp, tolerance))
            .collect();
        
        // Assign simplified geometries to tiles at this zoom level
        for (idx, mp) in simplified_geoms.iter().enumerate() {
            for poly in &mp.0 {
                // Get bounding box of this polygon
                let (poly_min_lon, poly_min_lat, poly_max_lon, poly_max_lat) = polygon_bounds(poly);
                
                // Skip invalid polygons
                if !poly_min_lon.is_finite() || !poly_min_lat.is_finite() || 
                   !poly_max_lon.is_finite() || !poly_max_lat.is_finite() {
                    continue;
                }
                
                // Calculate tile range that this polygon's bounding box spans
                let tile_min_x = lon_to_tile_x(poly_min_lon, zoom);
                let tile_max_x = lon_to_tile_x(poly_max_lon, zoom);
                let tile_min_y = lat_to_tile_y(poly_max_lat, zoom); // Note: tile Y inverted (north = lower Y)
                let tile_max_y = lat_to_tile_y(poly_min_lat, zoom);
                
                // Add polygon to all tiles it intersects
                for tile_x in tile_min_x..=tile_max_x {
                    for tile_y in tile_min_y..=tile_max_y {
                        tile_geometries
                            .entry((zoom, tile_x, tile_y))
                            .or_insert_with(Vec::new)
                            .push((idx, poly.clone()));
                    }
                }
            }
        }
    }
    
    // Create PMTiles writer
    let mut pm = PMTiles::new(TileType::Mvt, PmtilesCompression::GZip);
    
    // Set metadata
    pm.min_zoom = min_zoom;
    pm.max_zoom = max_zoom;
    pm.min_longitude = min_lon;
    pm.min_latitude = min_lat;
    pm.max_longitude = max_lon;
    pm.max_latitude = max_lat;
    
    // Center - use midpoint zoom for centering
    pm.center_zoom = (min_zoom + max_zoom) / 2;
    pm.center_longitude = (min_lon + max_lon) / 2.0;
    pm.center_latitude = (min_lat + max_lat) / 2.0;
    
    // Metadata - include vector_layers for proper viewer support
    pm.meta_data.insert("name".into(), serde_json::json!("OpenMander geometries"));
    pm.meta_data.insert("format".into(), serde_json::json!("pbf"));
    pm.meta_data.insert("type".into(), serde_json::json!("overlay"));
    pm.meta_data.insert("minzoom".into(), serde_json::json!(min_zoom));
    pm.meta_data.insert("maxzoom".into(), serde_json::json!(max_zoom));
    
    // Vector layer description for viewers (must be a JSON array, not a string)
    let vector_layers = serde_json::json!([{
        "id": "geometries",
        "fields": {"index": "String"},
        "minzoom": min_zoom,
        "maxzoom": max_zoom
    }]);
    pm.meta_data.insert("vector_layers".into(), vector_layers);
    
    let extent = 4096u32;
    let extent_f = extent as f64;
    
    // CRITICAL: Add buffer zone to prevent phantom tile boundaries
    // Buffer of 256 pixels (6.25% of 4096 extent) ensures geometries crossing
    // tile boundaries are rendered seamlessly without visible seams
    let buffer = 256.0;
    
    // Create a tile for each tile that has geometries
    for ((zoom, tile_x, tile_y), polygons) in tile_geometries.iter() {
        let mut tile = Tile::new(extent);
        let mut layer = tile.create_layer("geometries");
        
        // Convert each polygon to MVT features
        for (idx, poly) in polygons {
            // Create geometry encoder for polygon
            let mut encoder = GeomEncoder::new(GeomType::Polygon);
            
            // Process exterior ring
            let exterior_coords: Vec<_> = poly.exterior().coords().collect();
            if exterior_coords.len() < 3 {
                continue; // Skip invalid polygons
            }
            
            // Transform to tile coordinates (not rounded yet, to preserve precision for clipping)
            let exterior_tile_coords_raw: Vec<(f64, f64)> = exterior_coords.iter()
                .filter(|coord| coord.x.is_finite() && coord.y.is_finite())
                .map(|coord| world_to_tile_coords(coord.x, coord.y, *zoom, *tile_x, *tile_y, extent_f))
                .collect();
            
            // Clip ring to tile boundaries (interpolates intersection points properly)
            // Include buffer zone to prevent phantom boundaries at tile edges
            let exterior_clipped = clip_ring_to_tile(&exterior_tile_coords_raw, extent_f, buffer);
            
            // Round and clean the clipped ring
            let mut exterior_tile_coords: Vec<(f64, f64)> = exterior_clipped.iter()
                .map(|(x, y)| (x.round(), y.round()))
                .collect();
            exterior_tile_coords = clean_ring(exterior_tile_coords);
            if exterior_tile_coords.len() < 3 {
                continue; // Skip if ring became invalid after clipping/cleaning
            }
            
            // Note: We skip winding order enforcement because the MVT encoder 
            // and/or decoder libraries handle this automatically
            // Original winding from source data is preserved
            
            // Add exterior ring points
            for (x, y) in exterior_tile_coords.iter() {
                encoder = encoder.point(*x, *y)?;
            }
            
            // Complete the exterior ring
            encoder = encoder.complete()?;
            
            // Process interior rings (holes)
            for interior in poly.interiors() {
                let interior_coords: Vec<_> = interior.coords().collect();
                if interior_coords.len() < 3 {
                    continue;
                }
                
                // Transform to tile coordinates (not rounded yet)
                let interior_tile_coords_raw: Vec<(f64, f64)> = interior_coords.iter()
                    .filter(|coord| coord.x.is_finite() && coord.y.is_finite())
                    .map(|coord| world_to_tile_coords(coord.x, coord.y, *zoom, *tile_x, *tile_y, extent_f))
                    .collect();
                
                // Clip ring to tile boundaries (with buffer)
                let interior_clipped = clip_ring_to_tile(&interior_tile_coords_raw, extent_f, buffer);
                
                // Round and clean
                let mut interior_tile_coords: Vec<(f64, f64)> = interior_clipped.iter()
                    .map(|(x, y)| (x.round(), y.round()))
                    .collect();
                interior_tile_coords = clean_ring(interior_tile_coords);
                if interior_tile_coords.len() < 3 {
                    continue;
                }
                
                // Note: Winding order enforcement skipped - see comment above
                
                for (x, y) in interior_tile_coords.iter() {
                    encoder = encoder.point(*x, *y)?;
                }
                
                encoder = encoder.complete()?;
            }
            
            // Encode the complete polygon geometry
            let geom = encoder.encode()?;
            
            // Create feature from layer
            let mut feature = layer.into_feature(geom);
            feature.set_id(*idx as u64);
            feature.add_tag_string("index", &idx.to_string());
            
            // Add geo_id as a property if available (for feature identification in MapLibre)
            if let Some(geo_ids) = geo_ids {
                if let Some(geo_id) = geo_ids.get(*idx) {
                    feature.add_tag_string("geo_id", geo_id);
                }
            }
            
            layer = feature.into_layer();
        }
        
        // Add the layer to the tile
        tile.add_layer(layer)?;
        
        // Encode tile to MVT bytes
        let mvt_bytes = tile.to_bytes()?;
        
        // Compress the tile data
        let mut encoder = GzEncoder::new(Vec::new(), Flate2Compression::default());
        encoder.write_all(&mvt_bytes)?;
        let compressed_bytes = encoder.finish()?;
        
        // Add tile to PMTiles
        let tid = tile_id(*zoom, *tile_x, *tile_y);
        pm.add_tile(tid, compressed_bytes)?;
    }
    
    // Write PMTiles to bytes
    let mut out = Cursor::new(Vec::new());
    pm.to_writer(&mut out)?;
    Ok(out.into_inner())
}

/// Read geometries from PMTiles format bytes.
/// 
/// Since geometries may be stored across multiple tiles (each geometry in all tiles
/// it intersects), we need to deduplicate by feature ID when reading.
#[cfg(feature = "pmtiles")]
pub(crate) fn read_from_pmtiles_bytes(bytes: &[u8]) -> Result<Vec<MultiPolygon<f64>>> {
    use pmtiles2::{PMTiles, Compression as PmtilesCompression};
    use mvt_reader::Reader;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use std::collections::HashMap;
    
    // Read PMTiles file
    let mut reader = Cursor::new(bytes);
    let mut pmtiles = PMTiles::from_reader(&mut reader)
        .context("Failed to read PMTiles file")?;
    
    // Use a HashMap to collect all clipped pieces of each geometry by feature ID
    // (since the same geometry appears in multiple tiles as clipped pieces)
    // We'll merge them later to reconstruct the full geometry
    let mut geoms_by_id: HashMap<u64, Vec<MultiPolygon<f64>>> = HashMap::new();
    
    // Get the zoom level and bounds from metadata
    let z = pmtiles.min_zoom;
    
    // Calculate tile range from bounds
    let min_tile_x = lon_to_tile_x(pmtiles.min_longitude, z);
    let max_tile_x = lon_to_tile_x(pmtiles.max_longitude, z);
    let min_tile_y = lat_to_tile_y(pmtiles.max_latitude, z); // Note: Y is flipped
    let max_tile_y = lat_to_tile_y(pmtiles.min_latitude, z);
    
    // Iterate through all tiles in the bounds
    for tile_x in min_tile_x..=max_tile_x {
        for tile_y in min_tile_y..=max_tile_y {
            // Try to get the tile
            match pmtiles.get_tile(tile_x, tile_y, z) {
                Ok(Some(tile_data)) => {
                    // Decompress if needed
                    let decoded_data = if pmtiles.tile_compression == PmtilesCompression::GZip {
                        let mut decoder = GzDecoder::new(&tile_data[..]);
                        let mut decompressed = Vec::new();
                        decoder.read_to_end(&mut decompressed)?;
                        decompressed
                    } else {
                        tile_data
                    };
                    
                    // Decode MVT tile using mvt-reader
                    let mvt_reader = match Reader::new(decoded_data) {
                        Ok(r) => r,
                        Err(_) => continue, // Skip corrupted tiles
                    };
                    
                    // Get layer names
                    let layer_names = match mvt_reader.get_layer_names() {
                        Ok(names) => names,
                        Err(_) => continue,
                    };
                    
                    // Process each layer
                    for (layer_idx, _layer_name) in layer_names.iter().enumerate() {
                        let features = match mvt_reader.get_features(layer_idx) {
                            Ok(f) => f,
                            Err(_) => continue,
                        };
                        
                        for feature in features {
                            // Feature ID is optional; skip features without ID
                            let feature_id = match feature.id {
                                Some(id) => id,
                                None => continue,
                            };
                            
                            let tile_geom = feature.get_geometry();
                            let extent = 4096.0_f32;
                            let world_geom = geometry_tile_to_world(z, tile_x, tile_y, extent, tile_geom);
                            
                            // Collect all clipped pieces for this feature
                            // (features spanning multiple tiles will have multiple clipped pieces)
                            if let Some(mp) = geometry_to_multipolygon(&world_geom) {
                                geoms_by_id
                                    .entry(feature_id)
                                    .or_insert_with(Vec::new)
                                    .push(mp);
                            }
                        }
                    }
                },
                Ok(None) => continue, // Tile doesn't exist
                Err(_) => continue,   // Error getting tile
            }
        }
    }
    
    // Merge all clipped pieces for each feature to reconstruct full geometries
    let mut merged_geoms: Vec<(u64, MultiPolygon<f64>)> = Vec::new();
    for (feature_id, pieces) in geoms_by_id {
        if pieces.is_empty() {
            continue;
        }
        
        // If only one piece, use it directly (no merging needed)
        // Otherwise, union all pieces to reconstruct the full geometry
        let merged = if pieces.len() == 1 {
            pieces.into_iter().next().unwrap()
        } else {
            // Merge all pieces using union operation
            // Start with the first piece and union with each subsequent piece
            let mut result = pieces[0].clone();
            for piece in pieces.iter().skip(1) {
                result = result.union(piece);
            }
            result
        };
        
        merged_geoms.push((feature_id, merged));
    }
    
    // Find the maximum feature ID to determine the size of the output vector
    let max_feature_id = merged_geoms.iter()
        .map(|(id, _)| *id)
        .max()
        .unwrap_or(0);
    
    // Create a vector where index i corresponds to feature ID i
    // Fill missing feature IDs with empty MultiPolygons
    let mut all_geoms: Vec<MultiPolygon<f64>> = Vec::new();
    all_geoms.resize_with((max_feature_id + 1) as usize, || MultiPolygon::new(vec![]));
    
    // Fill in the actual geometries at their feature ID indices
    for (feature_id, merged) in merged_geoms {
        if (feature_id as usize) < all_geoms.len() {
            all_geoms[feature_id as usize] = merged;
        }
    }
    
    // Return empty vector if no geometries found (geometry files are optional)
    // This allows layers without geometry to load successfully
    Ok(all_geoms)
}

/// Placeholder implementations when pmtiles feature is not enabled
#[cfg(not(feature = "pmtiles"))]
pub(crate) fn write_to_pmtiles_bytes(_geoms: &[MultiPolygon<f64>], _geo_ids: Option<&[String]>, _min_zoom: u8, _max_zoom: u8) -> Result<Vec<u8>> {
    Err(anyhow::anyhow!("PMTiles format requires 'pmtiles' feature to be enabled"))
}

#[cfg(not(feature = "pmtiles"))]
pub(crate) fn read_from_pmtiles_bytes(_bytes: &[u8]) -> Result<Vec<MultiPolygon<f64>>> {
    Err(anyhow::anyhow!("PMTiles format requires 'pmtiles' feature to be enabled"))
}
