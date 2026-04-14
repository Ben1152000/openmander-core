#!/usr/bin/env python3
"""
Memory profile for webpack (PMTiles) pack loading — what WASM does at startup.
Loads the webpack layer-by-layer using the same code path as the browser worker.

Usage:
    python profile_webpack_load.py TX
"""

import os
import resource
import subprocess
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
DATA_DIR  = TOOLS_DIR.parent

import openmander


def rss_mb() -> float:
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True, text=True,
        )
        return int(out.stdout.strip()) / 1024.0
    except Exception:
        return 0.0


def peak_rss_mb() -> float:
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return v / 1_048_576.0
    return v / 1024.0


def checkpoint(label: str) -> None:
    print(f"  [{label:40s}] current={rss_mb():5.0f} MiB  peak={peak_rss_mb():5.0f} MiB")


def main():
    state_code  = (sys.argv[1] if len(sys.argv) > 1 else "TX").upper()
    state_dir   = DATA_DIR / "packs" / state_code
    webpack_dir = state_dir / f"{state_code}_2020_webpack"

    if not webpack_dir.exists():
        print(f"ERROR: webpack not found at {webpack_dir}")
        print("Run: python build_pack.py", state_code)
        sys.exit(1)

    print(f"Webpack load memory profile for {state_code}")
    print(f"Pack: {webpack_dir}")
    print("=" * 70)
    checkpoint("start")

    print("\n[Stage 1] Load full webpack (all layers) ...")
    map_obj = openmander.Map.from_pack(str(webpack_dir), format="pmtiles")
    checkpoint("after full load")

    print("\n[Stage 2] Create Plan (38 districts) ...")
    plan = openmander.Plan(map_obj, 38)
    checkpoint("after Plan creation")

    print("\n[Stage 3] Randomize ...")
    plan.randomize()
    checkpoint("after randomize")

    del plan
    del map_obj
    print("\n" + "=" * 70)
    checkpoint("final (all freed)")


if __name__ == "__main__":
    main()
