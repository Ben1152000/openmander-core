#!/usr/bin/env python3
"""
Run dhat heap profiling for a state pack build pipeline.
Builds with: make python-dev-dhat  (from openmander-core/)
Then run from anywhere:
    python3 /path/to/openmander-data/tools/dhat_profile.py TX

Output: dhat-heap.json written next to this script.
Open at https://nnethercote.github.io/dh_view/dh_view.html
"""

import os
import sys
from pathlib import Path

# All paths are anchored to this script's location — works from any cwd.
TOOLS_DIR = Path(__file__).resolve().parent
DATA_DIR  = TOOLS_DIR.parent

import openmander

# Write dhat-heap.json next to this script so it's easy to find.
os.chdir(TOOLS_DIR)


def main():
    state_code  = (sys.argv[1] if len(sys.argv) > 1 else "TX").upper()
    state_dir   = DATA_DIR / "packs" / state_code
    pack_dir    = state_dir / f"{state_code}_2020_pack"
    webpack_dir = state_dir / f"{state_code}_2020_webpack"

    if not pack_dir.exists():
        print(f"ERROR: pack not found at {pack_dir}. Run build_pack.py first.")
        sys.exit(1)

    print(f"dhat heap profile for {state_code}")
    print(f"Pack:    {pack_dir}")
    print(f"Webpack: {webpack_dir}")
    print(f"Output:  {TOOLS_DIR / 'dhat-heap.json'}")
    print("=" * 60)

    print("[1] Reading parquet pack ...")
    map_obj = openmander.Map(str(pack_dir))
    print("    done")

    print("[2] Writing PMTiles pack ...")
    webpack_dir.mkdir(parents=True, exist_ok=True)
    map_obj.to_pack(str(webpack_dir), format="pmtiles")
    print("    done")

    print("[3] Verifying PMTiles load ...")
    del map_obj
    map_pmtiles = openmander.Map.from_pack(str(webpack_dir), format="pmtiles")
    del map_pmtiles
    print("    done")

    print("\nFinalizing profiler -> dhat-heap.json ...")
    openmander.finish_profiling()
    print("Done. Open dhat-heap.json at https://nnethercote.github.io/dh_view/dh_view.html")


if __name__ == "__main__":
    main()
