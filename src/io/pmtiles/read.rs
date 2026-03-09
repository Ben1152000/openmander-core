//! PMTiles reading operations.

use std::io::Cursor;

use anyhow::{Context, Result};
use geo::{BooleanOps, MultiPolygon, Coord, LineString, Polygon};
#[cfg(feature = "pmtiles")]
use geo_types::{GeometryCollection, LineString as GeoLineString, MultiLineString, MultiPoint, MultiPolygon as GeoMultiPolygon, Point, Polygon as GeoPolygon};

use super::proj;

/// Convert a single tile-local point (px, py) into lon/lat
fn tile_point_to_lonlat(z: u8, tx: u64, ty: u64, extent: f32, px: f32, py: f32) -> (f64, f64) {
    proj::tile_to_world_coords(px as f64, py as f64, z, tx, ty, extent as f64)
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
    let min_tile_x = proj::lon_to_tile_x(pmtiles.min_longitude, z);
    let max_tile_x = proj::lon_to_tile_x(pmtiles.max_longitude, z);
    let min_tile_y = proj::lat_to_tile_y(pmtiles.max_latitude, z); // Note: Y is flipped
    let max_tile_y = proj::lat_to_tile_y(pmtiles.min_latitude, z);

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

/// Placeholder implementation when pmtiles feature is not enabled
#[cfg(not(feature = "pmtiles"))]
pub(crate) fn read_from_pmtiles_bytes(_bytes: &[u8]) -> Result<Vec<MultiPolygon<f64>>> {
    Err(anyhow::anyhow!("PMTiles format requires 'pmtiles' feature to be enabled"))
}
