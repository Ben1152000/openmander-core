#!/usr/bin/env python3
"""Render SVGs from CSV district assignments."""
import argparse
from pathlib import Path

import openmander as om

SCRIPT_DIR = Path(__file__).resolve().parent
PACKS_DIR = SCRIPT_DIR.parent / "packs"


def main():
    parser = argparse.ArgumentParser(description="Render SVGs from CSV district assignments")
    parser.add_argument("--state", required=True, help="Two-letter state code (e.g. NC)")
    parser.add_argument("--districts", type=int, required=True, help="Number of districts")
    parser.add_argument("--pack-dir", type=Path, default=PACKS_DIR, help="Directory for packs")
    parser.add_argument("--out-dir", type=Path, default=SCRIPT_DIR / "artifacts", help="Output directory")
    parser.add_argument("--partisan", action="store_true", help="Color by partisan lean")
    parser.add_argument("csvs", nargs="+", help="CSV files to render (names or paths; .csv extension optional)")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    pack_path = args.pack_dir / f"{args.state}_2020_pack"
    if not pack_path.exists():
        pack_path = Path(om.download_pack(args.state, str(args.pack_dir), verbose=1))

    print(f"Loading map from {pack_path}")
    mp = om.Map(str(pack_path))

    for csv_arg in args.csvs:
        csv_path = Path(csv_arg)
        if not csv_path.is_absolute():
            csv_path = args.out_dir / csv_path
        if not csv_path.suffix:
            csv_path = csv_path.with_suffix(".csv")
        svg_path = args.out_dir / csv_path.with_suffix(".svg").name

        print(f"Loading {csv_path.name}")
        plan = om.Plan(mp, num_districts=args.districts)
        plan.load_csv(str(csv_path))

        print(f"Writing {svg_path.name}")
        plan.to_svg(str(svg_path), color_partisan=args.partisan)

    print("Done.")


if __name__ == "__main__":
    main()
