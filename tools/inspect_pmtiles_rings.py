#!/usr/bin/env python3
"""
inspect_pmtiles_rings.py -- Check if PMTiles block tiles contain interior rings (holes).

Scans all tiles at a given zoom level and reports how many block features
have interior rings (holes) vs. only exterior rings.

Usage:
    python3 tools/inspect_pmtiles_rings.py <webpack_dir> [--zoom ZOOM] [--max-tiles N] [--layer LAYER]

Example:
    python3 tools/inspect_pmtiles_rings.py packs/NJ/NJ_2020_webpack
    python3 tools/inspect_pmtiles_rings.py packs/NJ/NJ_2020_webpack --zoom 14 --max-tiles 500
"""

import argparse
import gzip
import sys
import os

try:
    import pmtiles.reader as pmtiles_reader
    from pmtiles.reader import MmapSource, all_tiles
except ImportError:
    print("ERROR: pmtiles not installed. Run: python3 -m pip install pmtiles")
    sys.exit(1)

try:
    import mapbox_vector_tile
except ImportError:
    print("ERROR: mapbox-vector-tile not installed. Run: python3 -m pip install mapbox-vector-tile")
    sys.exit(1)


def get_signed_area(coords):
    """Shoelace signed area. Positive=CCW, Negative=CW."""
    n = len(coords)
    area = 0.0
    for i in range(n):
        x0, y0 = coords[i]
        x1, y1 = coords[(i + 1) % n]
        area += x0 * y1 - x1 * y0
    return area / 2.0


def inspect_pmtiles(webpack_dir: str, zoom: int = 14, max_tiles: int = 200, layer: str = 'block'):
    pmtiles_path = os.path.join(webpack_dir, 'geom', 'geometries.pmtiles')
    if not os.path.exists(pmtiles_path):
        print(f"ERROR: PMTiles not found at {pmtiles_path}")
        sys.exit(1)

    print(f"Opening PMTiles: {pmtiles_path}")
    f = open(pmtiles_path, 'rb')
    source = MmapSource(f)

    reader = pmtiles_reader.Reader(source)
    header = reader.header()
    min_zoom = header.get('min_zoom', 0)
    max_zoom = header.get('max_zoom', 14)
    print(f"Zoom range: {min_zoom} - {max_zoom}")

    actual_zoom = min(zoom, max_zoom)
    if actual_zoom != zoom:
        print(f"Requested zoom {zoom}, using {actual_zoom} (max available)")

    tiles_scanned = 0
    tiles_with_layer = 0
    total_features = 0
    features_with_interior_rings = 0
    hole_examples = []  # (geo_id/index, n_holes)

    print(f"\nScanning zoom-{actual_zoom} tiles for '{layer}' layer...")

    for zxy, tile_data in all_tiles(source):
        z, x, y = zxy
        if z != actual_zoom:
            continue

        tiles_scanned += 1
        if tiles_scanned > max_tiles:
            print(f"  (Reached max_tiles={max_tiles} limit; pass --max-tiles for more)")
            break

        if tile_data[:2] == b'\x1f\x8b':
            try:
                tile_data = gzip.decompress(tile_data)
            except Exception:
                continue

        try:
            decoded = mapbox_vector_tile.decode(tile_data)
        except Exception:
            continue

        if layer not in decoded:
            continue

        tiles_with_layer += 1

        for feature in decoded[layer]['features']:
            total_features += 1
            geom = feature['geometry']
            geom_type = geom['type']
            feat_id = feature.get('properties', {}).get('index', feature.get('id', '?'))

            if geom_type == 'Polygon':
                rings = geom['coordinates']
                n_interior = len(rings) - 1
                if n_interior > 0:
                    features_with_interior_rings += 1
                    # Check winding orders
                    exterior_area = get_signed_area(rings[0])
                    interior_areas = [get_signed_area(r) for r in rings[1:]]
                    if len(hole_examples) < 5:
                        hole_examples.append({
                            'id': feat_id,
                            'type': 'Polygon',
                            'n_rings': len(rings),
                            'n_interior': n_interior,
                            'ext_area': exterior_area,
                            'int_areas': interior_areas,
                        })

            elif geom_type == 'MultiPolygon':
                for poly_rings in geom['coordinates']:
                    n_interior = len(poly_rings) - 1
                    if n_interior > 0:
                        features_with_interior_rings += 1
                        exterior_area = get_signed_area(poly_rings[0])
                        interior_areas = [get_signed_area(r) for r in poly_rings[1:]]
                        if len(hole_examples) < 5:
                            hole_examples.append({
                                'id': feat_id,
                                'type': 'MultiPolygon',
                                'n_rings': len(poly_rings),
                                'n_interior': n_interior,
                                'ext_area': exterior_area,
                                'int_areas': interior_areas,
                            })

    print(f"\n=== Results from {tiles_scanned} zoom-{actual_zoom} tiles ===")
    print(f"  Tiles with '{layer}' layer: {tiles_with_layer}")
    print(f"  Total features:            {total_features}")
    print(f"  Features with holes:       {features_with_interior_rings}")

    if features_with_interior_rings == 0 and total_features > 0:
        print(f"\nFINDING: No interior rings in PMTiles. Bug is in PMTiles ENCODING.")
    elif features_with_interior_rings > 0:
        print(f"\nFINDING: PMTiles DOES contain interior rings.")
        print("  If holes don't render, the bug is in the frontend or MapLibre.")
        print("\n  Winding order of example hole features (in tile coords, Y-down):")
        print("  MVT spec: exterior=CW (positive area in Y-down), interior=CCW (negative area)")
        for ex in hole_examples:
            ext_winding = "CW (positive)" if ex['ext_area'] > 0 else "CCW (negative)"
            int_windings = ["CW" if a > 0 else "CCW" for a in ex['int_areas']]
            print(f"    id={ex['id']}: exterior area={ex['ext_area']:.1f} ({ext_winding}), "
                  f"interior areas={[f'{a:.1f}' for a in ex['int_areas']]} ({int_windings})")
        print()
        print("  For correct MVT holes: exterior should be CW (positive), interiors CCW (negative)")
    else:
        print(f"\nNo '{layer}' features found in {tiles_scanned} tiles at zoom {actual_zoom}.")
        print("  Try a different zoom level.")

    f.close()


def main():
    parser = argparse.ArgumentParser(description='Inspect PMTiles ring structure.')
    parser.add_argument('webpack_dir', help='Path to webpack (PMTiles) directory')
    parser.add_argument('--zoom', type=int, default=14, help='Zoom level (default: 14)')
    parser.add_argument('--max-tiles', type=int, default=200, help='Max tiles to scan (default: 200)')
    parser.add_argument('--layer', default='block', help='Layer name (default: block)')
    args = parser.parse_args()

    inspect_pmtiles(args.webpack_dir, zoom=args.zoom, max_tiles=args.max_tiles, layer=args.layer)


if __name__ == '__main__':
    main()
