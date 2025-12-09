#!/usr/bin/env python3
import os
import signal
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import openmander as om
from merge_svgs import merge_svgs_geo, write_svg

# Directory containing this script
SCRIPT_DIR = Path(__file__).resolve().parent

# Project root = parent of the directory containing this script
OPENMANDER_ROOT = SCRIPT_DIR.parent.parent

# Build paths relative to the root
BASE_PATH = OPENMANDER_ROOT / "packs"
SVG_PATH  = OPENMANDER_ROOT / "openmander-core" / "experiments" / "images"

STATES = {
    "CA": 52, "TX": 38, "FL": 28, "NY": 26, "IL": 17, "PA": 17, "OH": 15, "GA": 14,
    "NC": 14, "MI": 13, "NJ": 12, "VA": 11, "WA": 10, "AZ": 9, "IN": 9, "MA": 9,
    "TN": 9, "CO": 8, "MD": 8, "MN": 8, "MO": 8, "WI": 8, "AL": 7, "SC": 7,
    "KY": 6, "LA": 6, "OR": 6, "CT": 5, "OK": 5, "AR": 4, "IA": 4, "KS": 4,
    "MS": 4, "NV": 4, "UT": 4, "NE": 3, "NM": 3, "ID": 2, "ME": 2, "MT": 2,
    "NH": 2, "RI": 2, "WV": 2, "DE": 1, "ND": 1, "SD": 1, "VT": 1, "WY": 1,
}

# Use New England states as lightweight test:
STATES = { "MA": 9, "CT": 5, "ME": 2, "NH": 2, "RI": 2, "VT": 1 }

# --- Worker function ---------------------------------------------------------

def build_and_save_state(state: str, num_districts: int) -> str:
    print(f"[START] {state} ({num_districts} districts)")
    pack_path = os.path.join(BASE_PATH, f"{state}_2020_pack")

    mp = om.Map(pack_path)
    plan = om.Plan(mp, num_districts)

    plan.randomize()
    if num_districts > 1:
        plan.equalize("T_20_CENS_Total", tolerance=0.001, max_iter=20_000)
        
        objective = om.Objective(metrics=[
            om.Metric.population_deviation_smooth("T_20_CENS_Total"),
            om.Metric.compactness_polsby_popper(),
        ], weights=[0.9, 0.1])

        # plan.anneal(
        #     objective=objective,
        #     max_iter=2_000_000 * num_districts,
        #     init_temp=1.0,
        #     cooling_rate=0.000001 / num_districts,
        #     early_stop_iters=10000,
        #     window_size=1000,
        #     log_every=1_000_000,
        # )

    svg_file = os.path.join(SVG_PATH, f"{state}.svg")
    plan.to_svg(svg_file)

    print(f"[DONE ] {state}")
    return state


# --- Main logic with Ctrl-C cancellation -------------------------------------

def main() -> None:
    os.makedirs(SVG_PATH, exist_ok=True)
    total = len(STATES)

    print(f"Starting redistricting for {total} states… (Ctrl-C to cancel)")

    finished = 0

    # Use "with" so processes are cleaned even in exception
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(build_and_save_state, state, nd): state
            for state, nd in STATES.items()
        }

        try:
            for fut in as_completed(futures):
                state = futures[fut]
                try:
                    fut.result()
                    finished += 1
                    print(f"[PROGRESS] {finished}/{total} completed (last: {state})")
                except Exception as e:
                    print(f"[ERROR] {state}: {e}")

        except KeyboardInterrupt:
            print("\n[CTRL-C] Caught interrupt — terminating all workers…")

            # Cancel all futures that haven't started
            for f in futures:
                f.cancel()

            # Force-kill subprocesses immediately
            executor.shutdown(wait=False, cancel_futures=True)

            print("[EXIT] All worker processes were killed.")
            return  # Leave without merging

    # If we reach here, everything completed successfully
    print("Merging SVGs…")
    svg_files = [os.path.join(SVG_PATH, f"{state}.svg") for state in STATES]
    merged = merge_svgs_geo(svg_files, width=2400, margin=10)
    out_path = os.path.join(SVG_PATH, "merged_conus.svg")
    write_svg(merged, out_path)
    print(f"[MERGED] {out_path}")


if __name__ == "__main__":
    main()
