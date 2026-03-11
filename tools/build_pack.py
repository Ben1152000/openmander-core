#!/usr/bin/env python3
"""
Build a state pack, convert it to PMTiles (webpack) format, and copy it to the app.

Downloads census data if needed, builds the parquet pack, converts to PMTiles,
then copies the webpack into openmander-app/public/packs/.

Usage:
    python build_pack.py [state_code] [--no-vtd] [--verbose] [--no-copy]

Example:
    python build_pack.py IL
    python build_pack.py HI --no-vtd --verbose
    python build_pack.py TX --no-copy  # skip copying to app
    python build_pack.py IL --rebuild  # rebuild even if pack already exists
"""

import shutil
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
    copy_to_app = "--no-copy" not in flags
    rebuild = "--rebuild" in flags

    print(f"Building pack for {state_code}")
    print("=" * 60)

    # Paths relative to this script
    core_dir = Path(__file__).parent.parent
    packs_dir = core_dir / "packs"
    packs_dir.mkdir(parents=True, exist_ok=True)
    original_pack_dir = packs_dir / f"{state_code}_2020_pack"

    # Step 1: Build the initial pack if it doesn't exist (or --rebuild)
    if rebuild and original_pack_dir.exists():
        print(f"\n[Step 1] Removing existing pack for rebuild...")
        shutil.rmtree(original_pack_dir)

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

    # Step 3: Copy webpack to openmander-app
    if copy_to_app:
        app_packs_dir = core_dir.parent / "openmander-app" / "public" / "packs"
        app_pack_dest = app_packs_dir / f"{state_code}_2020_webpack"
        print(f"\n[Step 3] Copying webpack to app...")
        print(f"  Destination: {app_pack_dest}")

        if not app_packs_dir.exists():
            print(f"  Warning: App packs directory not found at {app_packs_dir}")
            print(f"  Skipping copy. Run with the openmander-app repo present.")
        else:
            if app_pack_dest.exists():
                shutil.rmtree(app_pack_dest)
            shutil.copytree(pmtiles_pack_dir, app_pack_dest)
            print(f"  Copied to {app_pack_dest}")
    else:
        print(f"\n[Step 3] Skipping copy to app (--no-copy)")

    print("\n" + "=" * 60)
    print(f"Done! Webpack written to: {pmtiles_pack_dir}")
    if copy_to_app and app_packs_dir.exists():
        print(f"      Also copied to:   {app_pack_dest}")
    print(f"\nThe webpack ({state_code}_2020_webpack) contains:")
    print(f"  - CSV data files (for WASM compatibility)")
    print(f"  - PMTiles geometry files (for efficient web display)")
    print(f"  - WKB hull files (for convex hull operations)")
    print(f"  - CSR adjacency files (for graph operations)")


if __name__ == "__main__":
    main()
