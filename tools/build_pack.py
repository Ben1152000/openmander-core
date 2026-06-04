#!/usr/bin/env python3
"""
Build a state pack, convert it to PMTiles (webpack) format, and optionally copy it to the app.

Downloads census data if needed, builds the parquet pack, then converts to PMTiles.

Requires openmander to be installed in the current Python environment.

Usage:
    python build_pack.py [state_code] [--no-vtd] [--verbose] [--no-split] [--split-size=MB] [-f]

Example:
    python build_pack.py IL
    python build_pack.py HI --no-vtd --verbose
    python build_pack.py IL -f               # rebuild even if pack already exists
    python build_pack.py IL --no-split       # skip file splitting
    python build_pack.py IL --split-size=25  # split at 25 MiB instead of 50 MiB
"""

import hashlib
import json
import shutil
import sys
import traceback
from pathlib import Path

import openmander

# repo root (one level above this tools/ directory); packs/ lives here
DATA_DIR = Path(__file__).parent.parent

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MiB


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

        print(f"  Splitting {rel_path} ({file_path.stat().st_size / 1024 / 1024:.1f} MiB)...")
        data = file_path.read_bytes()
        parts = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        for idx, part_data in enumerate(parts):
            part_name = f"{rel_path}.part{idx:03d}"
            part_path = pack_dir / part_name
            part_path.parent.mkdir(parents=True, exist_ok=True)
            part_path.write_bytes(part_data)
            sha256 = hashlib.sha256(part_data).hexdigest()
            files_map[part_name] = {"sha256": sha256}
            print(f"    Wrote {part_name} ({len(part_data) / 1024 / 1024:.1f} MiB)")

        del files_map[rel_path]
        file_path.unlink()
        changed = True
        print(f"  Split into {len(parts)} parts, removed original.")

    if changed:
        manifest["files"] = files_map
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"  Updated manifest.json")


def build_pack_for_state(
    state_code: str,
    *,
    has_vtd: bool = True,
    verbose: int = 0,
    rebuild: bool = False,
    do_split: bool = True,
    split_size_mb: int = 50,
) -> None:
    """Build a single state pack end-to-end. Raises on failure."""
    chunk_size = split_size_mb * 1024 * 1024

    print(f"Building pack for {state_code}")
    print("=" * 60)

    state_dir = DATA_DIR / "packs" / state_code
    state_dir.mkdir(parents=True, exist_ok=True)
    original_pack_dir = state_dir / f"{state_code}_2020_pack"

    # Step 1: Build the initial pack if it doesn't exist (or --force)
    if rebuild and original_pack_dir.exists():
        print(f"\n[Step 1] Removing existing pack (--force)...")
        shutil.rmtree(original_pack_dir)

    if not original_pack_dir.exists():
        print(f"\n[Step 1] Pack not found at {original_pack_dir}")
        print(f"  Downloading data and building pack for {state_code}...")
        result_path = openmander.build_pack(
            state_code,
            path=str(state_dir),
            has_vtd=has_vtd,
            verbose=verbose,
        )
        print(f"  Built pack at {result_path}")
    else:
        print(f"\n[Step 1] Using existing pack: {original_pack_dir}")

    # Step 2: Convert parquet -> PMTiles
    pmtiles_pack_dir = state_dir / f"{state_code}_2020_webpack"
    print(f"\n[Step 2] Converting parquet -> PMTiles...")

    map_original = openmander.Map(str(original_pack_dir))
    print(f"  Read pack (parquet format)")

    pmtiles_pack_dir.mkdir(parents=True, exist_ok=True)
    map_original.to_pack(str(pmtiles_pack_dir), format="pmtiles")
    print(f"  Wrote PMTiles pack to {pmtiles_pack_dir}")

    openmander.Map.from_pack(str(pmtiles_pack_dir), format="pmtiles")
    print(f"  Verified PMTiles pack loads correctly")

    if do_split:
        print(f"\n[Step 3] Splitting large files (>{split_size_mb} MiB)...")
        split_large_files(pmtiles_pack_dir, chunk_size)
    else:
        print(f"\n[Step 3] Skipping file splitting (--no-split)")

    print("\n" + "=" * 60)
    print(f"Done! Webpack written to: {pmtiles_pack_dir}")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]

    if not args:
        print(__doc__)
        sys.exit(0)

    state_code = args[0].upper()

    try:
        build_pack_for_state(
            state_code,
            has_vtd="--no-vtd" not in flags,
            verbose=1 if "--verbose" in flags else 0,
            rebuild="--force" in flags or "-f" in flags,
            do_split="--no-split" not in flags,
            split_size_mb=next(
                (int(f.split("=", 1)[1]) for f in flags if f.startswith("--split-size=")),
                50,
            ),
        )
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
