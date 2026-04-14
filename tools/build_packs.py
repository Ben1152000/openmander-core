#!/usr/bin/env python3
"""
Build packs for multiple states in parallel.

Wraps build_pack.py and runs it concurrently across states using a thread pool.

Usage:
    python build_packs.py [--no-vtd] [--no-split] [--split-size=MB]
                          [--workers=N] [--states=IL,IA,IN]

Flags:
    --no-vtd          Skip VTD layer
    --no-split        Skip splitting large files
    --split-size=MB   Split threshold in MiB (default: 50)
    --workers=N       Max parallel workers (default: 4)
    --states=X,Y,Z    Comma-separated list of state codes to build (default: all below)
    -f / --force      Rebuild even if pack already exists

Example:
    python build_packs.py --states=IL,IA,IN
    python build_packs.py --workers=2 --no-split
"""

import concurrent.futures
import sys
import traceback
from pathlib import Path

# Import from build_pack.py in the same directory
sys.path.insert(0, str(Path(__file__).parent))
from build_pack import build_pack_for_state

DEFAULT_STATE_CODES = [
    "AL", "AZ", "AR",       "CO", "CT", "DE", "FL", "GA", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA",       "MD", "MA", "MI",
    "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK",       "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA",       "WI", "WY",
]


def build_state(state_code: str, **kwargs) -> tuple[str, bool, str | None]:
    try:
        build_pack_for_state(state_code, **kwargs)
        return state_code, True, None
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[{state_code}] ERROR: {type(e).__name__}: {e}\n{tb}")
        return state_code, False, f"{type(e).__name__}: {e}"


def main():
    flags = [a for a in sys.argv[1:] if a.startswith("-")]

    if "--help" in flags or "-h" in flags:
        print(__doc__)
        sys.exit(0)

    # Parse --states=IL,IA,IN
    states_flag = next((f for f in flags if f.startswith("--states=")), None)
    state_codes = (
        [s.strip().upper() for s in states_flag.split("=", 1)[1].split(",")]
        if states_flag
        else DEFAULT_STATE_CODES
    )

    max_workers = next(
        (int(f.split("=", 1)[1]) for f in flags if f.startswith("--workers=")),
        4,
    )
    split_size_mb = next(
        (int(f.split("=", 1)[1]) for f in flags if f.startswith("--split-size=")),
        50,
    )

    kwargs = dict(
        has_vtd="--no-vtd" not in flags,
        do_split="--no-split" not in flags,
        rebuild="--force" in flags or "-f" in flags,
        split_size_mb=split_size_mb,
    )

    print(f"Building {len(state_codes)} state(s) with up to {max_workers} workers")
    print(f"States: {', '.join(state_codes)}")
    print("=" * 60)

    successes: list[str] = []
    failures: dict[str, str] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_state = {
            executor.submit(build_state, code, **kwargs): code
            for code in state_codes
        }
        for future in concurrent.futures.as_completed(future_to_state):
            code, ok, error = future.result()
            if ok:
                successes.append(code)
            else:
                failures[code] = error or "Unknown error"

    print("\n=== SUMMARY ===")
    print(f"Total: {len(state_codes)}  Successful: {len(successes)}  Failed: {len(failures)}")

    if successes:
        print(f"\nSucceeded: {', '.join(sorted(successes))}")
    if failures:
        print("\nFailed:")
        for code, msg in sorted(failures.items()):
            print(f"  {code}: {msg}")


if __name__ == "__main__":
    main()
