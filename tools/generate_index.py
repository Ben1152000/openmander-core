#!/usr/bin/env python3
"""
Generate packs.json by merging all webpack pack manifests.

Reads each webpack pack's inner manifest.json, strips the per-file hash table
(not needed for discovery), adds packDir and districts, and writes a merged
packs.json to the specified output path.

Usage:
    python generate_index.py [packs_root] [output_path] [--verbose]

    packs_root   defaults to ../packs (relative to openmander-core/)
    output_path  defaults to packs.json in the current directory
    --verbose    print each pack as it is processed
"""

import json
import sys
from pathlib import Path

DEFAULT_PACKS_ROOT = Path(__file__).parent.parent / "packs"

# Congressional district counts by state (2020 apportionment).
DISTRICTS = {
    "AL":  7, "AK":  1, "AZ":  9, "AR":  4, "CA": 52, "CO":  8,
    "CT":  5, "DE":  1, "DC":  1, "FL": 28, "GA": 14, "HI":  2,
    "ID":  2, "IL": 17, "IN":  9, "IA":  4, "KS":  4, "KY":  6,
    "LA":  6, "ME":  2, "MD":  8, "MA":  9, "MI": 13, "MN":  8,
    "MS":  4, "MO":  8, "MT":  2, "NE":  3, "NV":  4, "NH":  2,
    "NJ": 12, "NM":  3, "NY": 26, "NC": 14, "ND":  1, "OH": 15,
    "OK":  5, "OR":  6, "PA": 17, "RI":  2, "SC":  7, "SD":  1,
    "TN":  9, "TX": 38, "UT":  4, "VT":  1, "VA": 11, "WA": 10,
    "WV":  2, "WI":  8, "WY":  1, "PR":  0,
}

# Reverse lookup: FIPS → state abbreviation
_FIPS_TO_ABBR = {v: k for k, v in {
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
}.items()}


def main():
    args    = [a for a in sys.argv[1:] if not a.startswith("-")]
    verbose = "--verbose" in sys.argv or "-v" in sys.argv

    packs_root  = Path(args[0]) if len(args) > 0 else DEFAULT_PACKS_ROOT
    output_path = Path(args[1]) if len(args) > 1 else Path("packs.json")

    if not packs_root.exists():
        print(f"Packs root not found: {packs_root}")
        sys.exit(1)

    index: dict = {}
    for webpack_dir in sorted(packs_root.glob("*/*_webpack")):
        manifest_path = webpack_dir / "manifest.json"
        if not manifest_path.exists():
            continue

        with open(manifest_path) as f:
            manifest = json.load(f)

        name = manifest.get("name")
        fips = manifest.get("fips")
        bounds = manifest.get("bounds")
        if not name or not fips or not bounds:
            if verbose:
                print(f"  [skip] {webpack_dir.name}: missing name/fips/bounds")
            continue

        state_abbr = _FIPS_TO_ABBR.get(fips, "")
        districts = DISTRICTS.get(state_abbr, 1)
        pack_dir = f"{webpack_dir.parent.name}/{webpack_dir.name}"

        # Copy all manifest fields except 'files' (per-file hashes, not needed
        # for discovery), then add packDir and districts.
        entry = {k: v for k, v in manifest.items() if k != "files"}
        entry["packDir"] = pack_dir
        entry["districts"] = districts
        # Convert bounds to [[west, south], [east, north]] for MapLibre fitBounds.
        entry["bounds"] = [[bounds["west"], bounds["south"]], [bounds["east"], bounds["north"]]]

        pack_id = manifest.get("pack_id", webpack_dir.name)
        index.setdefault(state_abbr, {})[pack_id] = entry
        if verbose:
            print(f"  {name} / {pack_id}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(index, f, indent=2)
        f.write("\n")

    if verbose:
        print(f"\nWrote {sum(len(v) for v in index.values())} entries to {output_path}")


if __name__ == "__main__":
    main()
