//! PMTiles writing operations.

use std::{f64::consts::PI, io::Cursor};

use anyhow::Result;
use geo::Polygon; // used by polygon_bounds and encode_poly_for_tile closure

/// Convert longitude to Web Mercator X coordinate (in radians)
pub(super) fn lon_to_mercator_x(lon: f64) -> f64 { lon.to_radians() }

/// Convert latitude to Web Mercator Y coordinate (in radians)
pub(super) fn lat_to_mercator_y(lat: f64) -> f64 { (PI / 4.0 + lat.to_radians() / 2.0).tan().ln() }

/// Convert longitude to tile X coordinate at a given zoom level
pub(super) fn lon_to_tile_x(lon: f64, zoom: u8) -> u64 {
    let n = 2.0_f64.powi(zoom as i32);
    ((lon + 180.0) / 360.0 * n).floor() as u64
}

/// Convert latitude to tile Y coordinate at a given zoom level
pub(super) fn lat_to_tile_y(lat: f64, zoom: u8) -> u64 {
    let n = 2.0_f64.powi(zoom as i32);
    let lat_rad = lat.to_radians();
    ((1.0 - lat_rad.tan().asinh() / PI) / 2.0 * n).floor() as u64
}

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
pub(super) fn world_to_tile_coords(lon: f64, lat: f64, z: u8, x: u64, y: u64, extent: f64) -> (f64, f64) {
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

/// Clean a ring: remove duplicates, remove closing duplicate, ensure minimum points
fn clean_ring(ring: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    if ring.is_empty() {
        return ring;
    }
    
    // Remove consecutive duplicates
    let mut cleaned = Vec::new();
    cleaned.push(ring[0]);
    for &point in ring.iter().skip(1) {
        let prev = cleaned.last().unwrap();
        // Only add if different from previous point
        if (point.0 - prev.0).abs() > f64::EPSILON || (point.1 - prev.1).abs() > f64::EPSILON {
            cleaned.push(point);
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
        for &point in cleaned.iter().skip(1) {
            let prev_idx = if deduped.len() >= 2 { deduped.len() - 2 } else { 0 };
            let prev = deduped[prev_idx];
            // Skip if this point equals the point before the previous (A-B-A pattern)
            if !(deduped.len() >= 2 && (point.0 - prev.0).abs() < f64::EPSILON && (point.1 - prev.1).abs() < f64::EPSILON) {
                deduped.push(point);
            }
        }
        cleaned = deduped;
    }
    
    // Need at least 3 distinct points for a valid ring
    if cleaned.len() < 3 {
        return Vec::new();
    }
    
    cleaned
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
/// - layers: Vec of (layer_name, region, geo_ids, min_zoom, max_zoom)
///
/// Simplification is topology-preserving: each arc (maximal chain of half-edges whose
/// interior vertices have out-degree 2) is simplified exactly once, so adjacent units
/// always share the same simplified coordinates with no gaps at shared boundaries.
///
/// Returns: PMTiles file as bytes
#[cfg(feature = "pmtiles")]
pub(crate) fn write_to_pmtiles_bytes(
    layers: Vec<(&str, &geograph::Region, Option<&[String]>, u8, u8)>
) -> Result<Vec<u8>> {
    use pmtiles2::{PMTiles, TileType, Compression as PmtilesCompression};
    use pmtiles2::util::tile_id;
    use mvt::{Tile, GeomEncoder, GeomType, GeomData};
    use flate2::write::GzEncoder;
    use flate2::Compression as Flate2Compression;
    use std::io::Write;
    use std::collections::HashMap;

    if layers.is_empty() {
        return Err(anyhow::anyhow!("Cannot write empty layer list to PMTiles"));
    }

    // Calculate overall bounds from all layers
    let mut global_min_lon  = f64::INFINITY;
    let mut global_min_lat  = f64::INFINITY;
    let mut global_max_lon  = f64::NEG_INFINITY;
    let mut global_max_lat  = f64::NEG_INFINITY;
    let mut global_min_zoom = u8::MAX;
    let mut global_max_zoom = u8::MIN;

    for (_, region, _, min_zoom, max_zoom) in &layers {
        global_min_zoom = global_min_zoom.min(*min_zoom);
        global_max_zoom = global_max_zoom.max(*max_zoom);

        for unit in region.unit_ids() {
            for poly in &region.geometry(unit).0 {
                let (pmin_lon, pmin_lat, pmax_lon, pmax_lat) = polygon_bounds(poly);
                if pmin_lon.is_finite() && pmin_lat.is_finite()
                    && pmax_lon.is_finite() && pmax_lat.is_finite()
                {
                    global_min_lon = global_min_lon.min(pmin_lon);
                    global_min_lat = global_min_lat.min(pmin_lat);
                    global_max_lon = global_max_lon.max(pmax_lon);
                    global_max_lat = global_max_lat.max(pmax_lat);
                }
            }
        }
    }
    
    /// Simplification divisor relative to tile size.
    /// tolerance = tile_size_degrees / SIMPLIFICATION_DIVISOR
    ///
    /// Higher → less simplification → fewer gaps between adjacent polygons,
    /// larger tile files.  Set to f64::INFINITY to disable entirely.
    ///
    /// Topology-preserving simplification shares arc coordinates between adjacent
    /// polygons, so raising this value reduces file size without introducing gaps.
    const SIMPLIFICATION_DIVISOR: f64 = 1000.0; // increase to reduce simplification; f64::INFINITY to disable

    /// Calculate simplification tolerance for a given zoom level.
    /// Returns 0.0 (no simplification) at max zoom or when SIMPLIFICATION_DIVISOR is infinite.
    fn calculate_tolerance_for_zoom(zoom: u8, max_zoom: u8) -> f64 {
        if zoom >= max_zoom || SIMPLIFICATION_DIVISOR.is_infinite() {
            return 0.0;
        }
        let tile_size_degrees = 360.0 / (2.0_f64.powi(zoom as i32));
        tile_size_degrees / SIMPLIFICATION_DIVISOR
    }

    // Maximum tiles a single polygon's bounding box may span along either axis at
    // a given zoom level.  Polygons exceeding this (huge open-water or tundra
    // census blocks) are skipped — they are degenerate for redistricting and
    // would produce tens of thousands of tile copies, causing OOM on large states.
    // At zoom 14 each tile ≈ 2.4 km, so 32 tiles ≈ 76 km; blocks wider than
    // that are filtered.  They still appear at lower zoom levels where the tile
    // size is larger than MAX_TILE_SPREAD × tile_km.
    const MAX_TILE_SPREAD: u64 = 64;

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

    let extent: u32 = 4096;
    let extent_f = extent as f64;
    let buffer: f64 = 256.0;

    // Helper: encode one polygon into MVT geometry bytes for a specific tile.
    // Returns None if the polygon clips to fewer than 3 points.
    let encode_poly_for_tile = |poly: &Polygon<f64>, zoom: u8, tile_x: u64, tile_y: u64| -> Result<Option<GeomData>> {
        let mut encoder = GeomEncoder::new(GeomType::Polygon);

        // Exterior ring
        let ext_raw: Vec<(f64, f64)> = poly.exterior().coords()
            .filter(|c| c.x.is_finite() && c.y.is_finite())
            .map(|c| world_to_tile_coords(c.x, c.y, zoom, tile_x, tile_y, extent_f))
            .collect();
        let ext_clipped = clean_ring(
            clip_ring_to_tile(&ext_raw, extent_f, buffer).into_iter()
                .map(|(x, y)| (x.round(), y.round())).collect()
        );
        if ext_clipped.len() < 3 { return Ok(None); }
        for (x, y) in &ext_clipped { encoder = encoder.point(*x, *y)?; }
        encoder = encoder.complete()?;

        // Interior rings
        for interior in poly.interiors() {
            let int_raw: Vec<(f64, f64)> = interior.coords()
                .filter(|c| c.x.is_finite() && c.y.is_finite())
                .map(|c| world_to_tile_coords(c.x, c.y, zoom, tile_x, tile_y, extent_f))
                .collect();
            let int_clipped = clean_ring(
                clip_ring_to_tile(&int_raw, extent_f, buffer).into_iter()
                    .map(|(x, y)| (x.round(), y.round())).collect()
            );
            if int_clipped.len() < 3 { continue; }
            for (x, y) in &int_clipped { encoder = encoder.point(*x, *y)?; }
            encoder = encoder.complete()?;
        }

        Ok(Some(encoder.encode()?))
    };

    // Process one zoom level at a time so each zoom level's tile data is dropped
    // before the next is processed, bounding peak memory to a single zoom level.
    //
    // tile_coords -> layer_name -> [(feature_idx, encoded_geom_bytes)]
    // Storing pre-encoded bytes (∼3 bytes/coord) rather than Polygon<f64> clones
    // (∼16 bytes/coord) cuts per-tile memory by ~5×, which is critical for large
    // states with complex geometries (Alaska, etc.).
    for zoom in global_min_zoom..=global_max_zoom {
        let mut zoom_tiles: HashMap<(u64, u64), HashMap<&str, Vec<(usize, GeomData)>>> = HashMap::new();

        for (layer_name, region, _geo_ids, min_zoom, max_zoom) in &layers {
            if zoom < *min_zoom || zoom > *max_zoom {
                continue;
            }
            let tolerance = calculate_tolerance_for_zoom(zoom, *max_zoom);

            // Stream one unit at a time via callback — never materialises the
            // full simplified-geometry array simultaneously.
            let mut skipped_count = 0usize;
            region.for_each_simplified_geometry(tolerance, |idx, mp| -> Result<()> {
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

                    // Skip polygons whose bbox spans too many tiles (huge open-water
                    // or tundra census blocks) — they would OOM large states.
                    if tile_max_x.saturating_sub(tile_min_x) > MAX_TILE_SPREAD
                        || tile_max_y.saturating_sub(tile_min_y) > MAX_TILE_SPREAD
                    {
                        skipped_count += 1;
                        let _ = idx; // suppress unused-variable warning
                        continue;
                    }

                    for tile_x in tile_min_x..=tile_max_x {
                        for tile_y in tile_min_y..=tile_max_y {
                            // Clip and encode immediately — store bytes, not Polygon clones.
                            if let Some(geom_bytes) = encode_poly_for_tile(poly, zoom, tile_x, tile_y)? {
                                zoom_tiles
                                    .entry((tile_x, tile_y))
                                    .or_default()
                                    .entry(layer_name)
                                    .or_default()
                                    .push((idx, geom_bytes));
                            }
                        }
                    }
                }
                Ok(())
            })?;
            if skipped_count > 0 {
                eprintln!(
                    "[pmtiles] z{zoom} layer {layer_name}: skipped {skipped_count} polygon(s) \
                     whose bbox exceeds MAX_TILE_SPREAD ({MAX_TILE_SPREAD} tiles) — \
                     large open-water or rural blocks omitted at this zoom level"
                );
            }
        }

        // Build and write all tiles for this zoom level, then drop zoom_tiles.
        for ((tile_x, tile_y), layer_geoms) in zoom_tiles.into_iter() {
            let mut tile = Tile::new(extent);

            for (layer_name, features) in layer_geoms.into_iter() {
                let mut layer = tile.create_layer(layer_name);

                for (idx, geom_data) in features {
                    let mut feature = layer.into_feature(geom_data);
                    feature.set_id(idx as u64);
                    feature.add_tag_string("index", &idx.to_string());
                    layer = feature.into_layer();
                }

                tile.add_layer(layer)?;
            }

            // Encode and compress tile
            let tile_data = tile.to_bytes()?;
            let mut gz = GzEncoder::new(Vec::new(), Flate2Compression::default());
            gz.write_all(&tile_data)?;
            let compressed = gz.finish()?;

            let tid = tile_id(zoom, tile_x, tile_y);
            pm.add_tile(tid, compressed)?;
        }
        // zoom_tiles is dropped here, freeing all encoded bytes for this zoom level.
    }

    // Write PMTiles to bytes
    let mut buffer = Cursor::new(Vec::new());
    pm.to_writer(&mut buffer)?;

    Ok(buffer.into_inner())
}
