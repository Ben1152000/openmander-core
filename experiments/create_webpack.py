#!/usr/bin/env python3
"""
Build a map pack from source data and convert it to PMTiles (webpack) format.

Downloads census data if needed, builds the parquet pack, then converts
to PMTiles for web display.

Usage:
    python create_webpack.py [state_code] [--no-vtd] [--verbose]

Example:
    python create_webpack.py IL
    python create_webpack.py HI --no-vtd --verbose
"""

import sys
from pathlib import Path

# Add the bindings to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "bindings" / "python"))

try:
    import openmander
except ImportError:
    print("Error: Could not import openmander. Make sure the Python bindings are built.")
    print("Run: cd bindings/python && maturin develop")
    sys.exit(1)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]

    state_code = args[0].upper() if args else "IL"
    has_vtd = "--no-vtd" not in flags
    verbose = 1 if "--verbose" in flags else 0

    print(f"Creating webpack for {state_code}")
    print("=" * 60)

    experiments_dir = Path(__file__).parent
    packs_dir = experiments_dir / "packs"
    packs_dir.mkdir(parents=True, exist_ok=True)
    original_pack_dir = packs_dir / f"{state_code}_2020_pack"

    # Step 1: Build the initial pack if it doesn't exist
    if not original_pack_dir.exists():
        print(f"\n[Step 1] Pack not found at {original_pack_dir}")
        print(f"  Downloading data and building pack for {state_code}...")
        try:
            result_path = openmander.build_pack(
                state_code,
                path=str(packs_dir),
                has_vtd=has_vtd,
                verbose=verbose,
            )
            print(f"  Built pack at {result_path}")
        except Exception as e:
            print(f"  Error building pack: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"\n[Step 1] Using existing pack: {original_pack_dir}")

    # Step 2: Convert parquet -> PMTiles
    pmtiles_pack_dir = packs_dir / f"{state_code}_2020_webpack"
    print(f"\n[Step 2] Converting parquet -> PMTiles...")

    try:
        map_original = openmander.Map(str(original_pack_dir))
        print(f"  Read pack (parquet format)")

        pmtiles_pack_dir.mkdir(parents=True, exist_ok=True)
        map_original.to_pack(str(pmtiles_pack_dir), format="pmtiles")
        print(f"  Wrote PMTiles pack to {pmtiles_pack_dir}")

        map_pmtiles = openmander.Map.from_pack(str(pmtiles_pack_dir), format="pmtiles")
        print(f"  Verified PMTiles pack loads correctly")

    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 60)
    print(f"Done! Webpack written to: {pmtiles_pack_dir}")
    print(f"\nThe webpack ({state_code}_2020_webpack) contains:")
    print(f"  - CSV data files (for WASM compatibility)")
    print(f"  - PMTiles geometry files (for efficient web display)")
    print(f"  - WKB hull files (for convex hull operations)")
    print(f"  - CSR adjacency files (for graph operations)")



if __name__ == "__main__":
    main()
