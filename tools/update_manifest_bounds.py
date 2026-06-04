#!/usr/bin/env python3
"""
Update existing webpack pack manifests with computed bounds.

Round-trips each webpack pack through the openmander library (read then write),
which causes the Rust code to recompute bounds from the state layer geometry and
write them into manifest.json — no full pack rebuild needed.

Usage:
    python update_manifest_bounds.py [packs_root]

    packs_root defaults to ../../../openmander-data/packs (relative to this
    script's location in openmander-core/tools/).

The script looks for directories named *_webpack/ under packs_root/<STATE>/.
"""

import json
import sys
import tempfile
from pathlib import Path

import openmander

DATA_DIR = Path(__file__).parent.parent.parent  # repo root
DEFAULT_PACKS_ROOT = DATA_DIR / "openmander-data" / "packs"

STATE_NAMES = {
    "AL": "Alabama",        "AK": "Alaska",         "AZ": "Arizona",
    "AR": "Arkansas",       "CA": "California",     "CO": "Colorado",
    "CT": "Connecticut",    "DE": "Delaware",        "FL": "Florida",
    "GA": "Georgia",        "HI": "Hawaii",          "ID": "Idaho",
    "IL": "Illinois",       "IN": "Indiana",         "IA": "Iowa",
    "KS": "Kansas",         "KY": "Kentucky",        "LA": "Louisiana",
    "ME": "Maine",          "MD": "Maryland",        "MA": "Massachusetts",
    "MI": "Michigan",       "MN": "Minnesota",       "MS": "Mississippi",
    "MO": "Missouri",       "MT": "Montana",         "NE": "Nebraska",
    "NV": "Nevada",         "NH": "New Hampshire",   "NJ": "New Jersey",
    "NM": "New Mexico",     "NY": "New York",        "NC": "North Carolina",
    "ND": "North Dakota",   "OH": "Ohio",            "OK": "Oklahoma",
    "OR": "Oregon",         "PA": "Pennsylvania",    "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota",    "TN": "Tennessee",
    "TX": "Texas",          "UT": "Utah",            "VT": "Vermont",
    "VA": "Virginia",       "WA": "Washington",      "WV": "West Virginia",
    "WI": "Wisconsin",      "WY": "Wyoming",         "DC": "District of Columbia",
    "PR": "Puerto Rico",
}

STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "PR": "72",
}


def get_metadata_from_parquet_pack(pack_dir: Path, state: str) -> dict | None:
    """Load a parquet pack, round-trip to get bounds, and return a metadata dict
    with 'bounds', 'name', and 'fips'. Returns None on failure."""
    print(f"  Loading {pack_dir.name} (parquet)...")
    try:
        mp = openmander.Map.from_pack(str(pack_dir), format="parquet")
    except Exception as e:
        print(f"  [error] failed to load parquet pack: {e}")
        return None

    # Round-trip through a temp dir so Rust computes and writes bounds.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp) / pack_dir.name
        tmp_path.mkdir()
        try:
            mp.to_pack(str(tmp_path), format="parquet")
        except Exception as e:
            print(f"  [error] failed to write temp pack: {e}")
            return None

        new_manifest_path = tmp_path / "manifest.json"
        if not new_manifest_path.exists():
            print(f"  [error] no manifest.json in temp output")
            return None

        with open(new_manifest_path) as f:
            new_manifest = json.load(f)

    bounds = new_manifest.get("bounds")
    if bounds is None:
        print(f"  [error] no bounds in temp manifest")
        return None

    # name/fips come from Python lookups (Rust doesn't set state_abbr when
    # reading from disk, so the temp manifest won't contain them).
    return {
        "bounds": bounds,
        "name":   STATE_NAMES.get(state),
        "fips":   STATE_FIPS.get(state),
    }


def insert_metadata(manifest: dict, meta: dict) -> dict:
    """Return a new dict with name/fips after pack_id and bounds after formats."""
    result = {}
    for key, value in manifest.items():
        if key in ("name", "fips", "bounds"):
            continue  # will be re-inserted in the right position
        result[key] = value
        if key == "pack_id":
            if meta.get("name"):
                result["name"] = meta["name"]
            if meta.get("fips"):
                result["fips"] = meta["fips"]
        if key == "formats":
            result["bounds"] = meta["bounds"]
    if "bounds" not in result:
        result["bounds"] = meta["bounds"]
    return result


def update_manifest(pack_dir: Path, meta: dict) -> None:
    """Inject name, fips, and bounds into an existing manifest.json."""
    manifest_path = pack_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"  [skip] no manifest.json in {pack_dir}")
        return

    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest = insert_metadata(manifest, meta)

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"  Updated {pack_dir.name}/manifest.json")


def main():
    packs_root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PACKS_ROOT

    if not packs_root.exists():
        print(f"Packs root not found: {packs_root}")
        sys.exit(1)

    print(f"Scanning {packs_root} for webpack packs...")

    parquet_packs = sorted(packs_root.glob("*/*_pack"))
    if not parquet_packs:
        print("No *_pack directories found.")
        sys.exit(0)

    for parquet_dir in parquet_packs:
        state = parquet_dir.parent.name
        print(f"\n[{state}]")

        meta = get_metadata_from_parquet_pack(parquet_dir, state)
        if meta is None:
            print(f"  [skip] could not compute metadata")
            continue

        print(f"  name={meta['name']}, fips={meta['fips']}, bounds={meta['bounds']}")

        update_manifest(parquet_dir, meta)

        # Copy metadata into the corresponding webpack manifest.
        webpack_dir = parquet_dir.parent / parquet_dir.name.replace("_pack", "_webpack")
        if webpack_dir.exists():
            update_manifest(webpack_dir, meta)
        else:
            print(f"  [skip] no webpack dir at {webpack_dir.name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
