#!/usr/bin/env python3
"""
inspect_geometry.py — Diagnostic tool for checking per-unit geometry in a pack.

Prints a summary of geometry statistics for each unit in a layer, highlighting
any units that have holes (donut-shaped blocks) or multiple polygons.

Requires a parquet pack (e.g. NJ_2020_pack/), not a webpack. Webpack packs have
chunked files that the Python reader does not reassemble. After running build_pack.py
both formats exist side-by-side: use the _pack directory, not _webpack.

Usage:
    python inspect_geometry.py <pack_dir> [--layer LAYER] [--holes-only]

Examples:
    python inspect_geometry.py packs/NJ/NJ_2020_pack
    python inspect_geometry.py packs/NJ/NJ_2020_pack --layer block --holes-only
    python inspect_geometry.py packs/NJ/NJ_2020_pack --layer county
"""

import argparse
import sys
from pathlib import Path

try:
    import openmander
except ImportError:
    print("ERROR: Could not import openmander. Make sure the Python bindings are built.")
    print("  Run: cd openmander-core && make python-dev")
    sys.exit(1)


def _suggest_parquet(pack_dir: str) -> str | None:
    """If pack_dir looks like a webpack, return the sibling parquet pack path."""
    p = Path(pack_dir)
    if p.name.endswith('_webpack'):
        parquet = p.parent / p.name.replace('_webpack', '_pack')
        if parquet.is_dir():
            return str(parquet)
    return None


def inspect_geometry(pack_dir: str, layer: str = 'block', holes_only: bool = False):
    print(f"Loading pack from: {pack_dir}")
    m = None
    try:
        m = openmander.Map(pack_dir)
    except Exception as e:
        suggestion = _suggest_parquet(pack_dir)
        if suggestion:
            print(f"ERROR loading pack: {e}")
            print(f"Note: webpack packs with chunked files cannot be read directly.")
            print(f"Trying sibling parquet pack: {suggestion}")
            try:
                m = openmander.Map(suggestion)
                print(f"Loaded parquet pack successfully.")
            except Exception as e2:
                print(f"ERROR loading parquet pack: {e2}")
                sys.exit(1)
        else:
            print(f"ERROR loading pack: {e}")
            sys.exit(1)

    print(f"Fetching geometry stats for layer: {layer}")
    try:
        stats = m.geometry_stats(layer=layer)
    except Exception as e:
        print(f"ERROR getting geometry stats: {e}")
        sys.exit(1)

    total = len(stats)
    multipolygon_count = sum(1 for s in stats if s['num_polygons'] > 1)
    has_holes_count = sum(1 for s in stats if any(h > 0 for h in s['holes_per_polygon']))
    exterior_count = sum(1 for s in stats if s['is_exterior'])
    total_holes = sum(sum(s['holes_per_polygon']) for s in stats)

    print(f"\n=== Layer: {layer} ===")
    print(f"  Total units:         {total}")
    print(f"  Exterior units:      {exterior_count}")
    print(f"  MultiPolygon units:  {multipolygon_count}")
    print(f"  Units with holes:    {has_holes_count}")
    print(f"  Total holes:         {total_holes}")

    if holes_only and has_holes_count == 0:
        print("\n  (No units with holes found)")
        return

    print()
    header = f"{'geo_id':<20} {'idx':>6} {'polys':>6} {'holes':>20} {'exterior':>9}"
    print(header)
    print("-" * len(header))

    shown = 0
    for s in stats:
        has_holes = any(h > 0 for h in s['holes_per_polygon'])
        is_multi = s['num_polygons'] > 1

        if holes_only and not has_holes and not is_multi:
            continue

        holes_str = str(s['holes_per_polygon']) if s['holes_per_polygon'] else '[]'
        ext_str = 'yes' if s['is_exterior'] else ''
        print(f"{s['geo_id']:<20} {s['idx']:>6} {s['num_polygons']:>6} {holes_str:>20} {ext_str:>9}")
        shown += 1

    if holes_only:
        print(f"\n  Showing {shown} units with holes/multi-polygon out of {total} total.")
    else:
        print(f"\n  Showing all {total} units.")


def main():
    parser = argparse.ArgumentParser(description='Inspect per-unit geometry in a pack.')
    parser.add_argument('pack_dir', help='Path to the pack directory')
    parser.add_argument('--layer', default='block',
                        choices=['state', 'county', 'tract', 'group', 'vtd', 'block'],
                        help='Layer to inspect (default: block)')
    parser.add_argument('--holes-only', action='store_true',
                        help='Only show units with holes or multiple polygons')
    args = parser.parse_args()

    inspect_geometry(args.pack_dir, layer=args.layer, holes_only=args.holes_only)


if __name__ == '__main__':
    main()
