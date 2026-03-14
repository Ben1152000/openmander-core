#!/usr/bin/env python3
"""
Build a state pack, convert it to PMTiles (webpack) format, and copy it to the app.

Downloads census data if needed, builds the parquet pack, converts to PMTiles,
then copies the webpack into openmander-app/public/packs/.

Usage:
    python build_pack.py [state_code] [--no-vtd] [--verbose] [--no-copy] [--no-split] [--split-size=MB]

Example:
    python build_pack.py IL
    python build_pack.py HI --no-vtd --verbose
    python build_pack.py TX --no-copy        # skip copying to app
    python build_pack.py IL -f               # rebuild even if pack already exists
    python build_pack.py IL --no-split       # skip file splitting
    python build_pack.py IL --split-size=25  # split at 25 MB instead of 50 MB
"""

import hashlib
import json
import shutil
import sys
from pathlib import Path

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB


def split_large_files(pack_dir: Path, chunk_size: int = CHUNK_SIZE) -> None:
    """Split any file in pack_dir that exceeds chunk_size into .part000, .part001, ...
    and update manifest.json to list the part files instead of the originals."""
    manifest_path = pack_dir / "manifest.json"
    if not manifest_path.exists():
        return

    with open(manifest_path) as f:
        manifest = json.load(f)

    files_map: dict = manifest.get("files", {})
    changed = False

    for rel_path in list(files_map.keys()):
        file_path = pack_dir / rel_path
        if not file_path.exists() or file_path.stat().st_size <= chunk_size:
            continue

        print(f"  Splitting {rel_path} ({file_path.stat().st_size / 1024 / 1024:.1f} MB)...")
        data = file_path.read_bytes()
        parts = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        for idx, part_data in enumerate(parts):
            part_name = f"{rel_path}.part{idx:03d}"
            part_path = pack_dir / part_name
            part_path.parent.mkdir(parents=True, exist_ok=True)
            part_path.write_bytes(part_data)
            sha256 = hashlib.sha256(part_data).hexdigest()
            files_map[part_name] = {"sha256": sha256}
            print(f"    Wrote {part_name} ({len(part_data) / 1024 / 1024:.1f} MB)")

        del files_map[rel_path]
        file_path.unlink()
        changed = True
        print(f"  Split into {len(parts)} parts, removed original.")

    if changed:
        manifest["files"] = files_map
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"  Updated manifest.json")

# Add the bindings to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "bindings" / "python"))

try:
    import openmander
except ImportError:
    print("Error: Could not import openmander. Make sure the Python bindings are built.")
    print("Run: cd bindings/python && maturin develop")
    sys.exit(1)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]

    if not args:
        print(__doc__)
        sys.exit(0)
    state_code = args[0].upper()
    has_vtd = "--no-vtd" not in flags
    verbose = 1 if "--verbose" in flags else 0
    copy_to_app = "--no-copy" not in flags
    rebuild = "--force" in flags or "-f" in flags
    do_split = "--no-split" not in flags
    split_size_mb = next(
        (int(f.split("=", 1)[1]) for f in flags if f.startswith("--split-size=")),
        50,
    )
    chunk_size = split_size_mb * 1024 * 1024

    print(f"Building pack for {state_code}")
    print("=" * 60)

    # Paths relative to this script
    core_dir = Path(__file__).parent.parent
    packs_dir = core_dir / "packs"
    packs_dir.mkdir(parents=True, exist_ok=True)
    original_pack_dir = packs_dir / f"{state_code}_2020_pack"

    # Step 1: Build the initial pack if it doesn't exist (or --rebuild)
    if rebuild and original_pack_dir.exists():
        print(f"\n[Step 1] Removing existing pack (--force)...")
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

        if do_split:
            print(f"\n[Step 2b] Splitting large files (>{split_size_mb} MB)...")
            split_large_files(pmtiles_pack_dir, chunk_size)
        else:
            print(f"\n[Step 2b] Skipping file splitting (--no-split)")

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
            print(f"  (large files already split in source; copy reflects split state)")
    else:
        print(f"\n[Step 3] Skipping copy to app (--no-copy)")

    print("\n" + "=" * 60)
    print(f"Done! Webpack written to: {pmtiles_pack_dir}")
    if copy_to_app and app_packs_dir.exists():
        print(f"      Also copied to:   {app_pack_dest}")
    print(f"\nThe webpack ({state_code}_2020_webpack) contains:")
    print(f"  - CSV data files (for WASM compatibility)")
    print(f"  - PMTiles geometry files (for efficient web display)")
    print(f"  - Region files (.region.gz, for geometry and adjacency)")


if __name__ == "__main__":
    main()
