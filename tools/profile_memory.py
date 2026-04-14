#!/usr/bin/env python3
"""
Memory profiling for pack build stages.
Measures current RSS at each step using `ps`, and peak RSS from resource module.
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
    """Current RSS of this process in MiB (via ps)."""
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True, text=True,
        )
        return int(out.stdout.strip()) / 1024.0
    except Exception:
        return 0.0


def peak_rss_mb() -> float:
    """Peak RSS of this process since start (macOS: bytes, Linux: KB)."""
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS returns bytes; Linux returns KB
    if sys.platform == "darwin":
        return v / 1_048_576.0
    return v / 1024.0


def checkpoint(label: str) -> None:
    print(f"  [{label}] current={rss_mb():.0f} MiB  peak={peak_rss_mb():.0f} MiB")


def main():
    state_code = (sys.argv[1] if len(sys.argv) > 1 else "TX").upper()
    state_dir   = DATA_DIR / "packs" / state_code
    pack_dir    = state_dir / f"{state_code}_2020_pack"
    webpack_dir = state_dir / f"{state_code}_2020_webpack"

    if not pack_dir.exists():
        print(f"ERROR: pack not found at {pack_dir}")
        print("Run build_pack.py first.")
        sys.exit(1)

    print(f"Memory profile for {state_code}")
    print("=" * 60)
    checkpoint("start")

    # Stage 1: Read parquet pack
    print("\n[Stage 1] Map.read_from_pack (parquet) ...")
    map_obj = openmander.Map(str(pack_dir))
    checkpoint("after read parquet")

    # Stage 2: Convert to PMTiles
    print("\n[Stage 2] map.to_pack (pmtiles) ...")
    webpack_dir.mkdir(parents=True, exist_ok=True)
    map_obj.to_pack(str(webpack_dir), format="pmtiles")
    checkpoint("after write pmtiles")

    # Stage 3: Drop the original map and verify load
    print("\n[Stage 3] Drop map + verify PMTiles load ...")
    del map_obj
    map_pmtiles = openmander.Map.from_pack(str(webpack_dir), format="pmtiles")
    checkpoint("after load pmtiles")

    del map_pmtiles
    print("\n" + "=" * 60)
    checkpoint("final")


if __name__ == "__main__":
    main()
