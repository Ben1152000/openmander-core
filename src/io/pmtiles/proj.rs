//! Projection utilities for PMTiles encoding and decoding.

use std::f64::consts::PI;

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

/// Convert tile coordinates to world coordinates
pub(super) fn tile_to_world_coords(tile_x: f64, tile_y: f64, z: u8, x: u64, y: u64, extent: f64) -> (f64, f64) {
    let (tile_min_x, tile_min_y, tile_max_x, tile_max_y) = tile_bounds(z, x, y);

    let merc_x = tile_min_x + (tile_x / extent) * (tile_max_x - tile_min_x);
    let merc_y = tile_max_y - (tile_y / extent) * (tile_max_y - tile_min_y); // Y is flipped (down)

    let lon = mercator_x_to_lon(merc_x);
    let lat = mercator_y_to_lat(merc_y);

    (lon, lat)
}
