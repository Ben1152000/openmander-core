#!/usr/bin/env python3
import os
import signal
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import openmander as om
from merge_svgs import merge_svgs_geo, write_svg

# Directory containing this script
SCRIPT_DIR = Path(__file__).resolve().parent

# Project root = parent of the directory containing this script
OPENMANDER_ROOT = SCRIPT_DIR.parent.parent

# Build paths relative to the root
BASE_PATH = OPENMANDER_ROOT / "openmander-core" / "experiments" / "packs"
SVG_PATH  = OPENMANDER_ROOT / "openmander-core" / "experiments" / "images"

STATES = {
    "CA": 52, "TX": 38, "FL": 28,  "PA": 17, "OH": 15, "GA": 14,
    "NC": 14, "MI": 13, "NJ": 12, "VA": 11, "WA": 10, "AZ": 9, "IN": 9, "MA": 9,
    "TN": 9, "CO": 8, "MD": 8, "MN": 8, "MO": 8, "WI": 8, "AL": 7, "SC": 7,
    "KY": 6, "LA": 6, "OR": 6, "CT": 5, "OK": 5, "AR": 4, "IA": 4, "KS": 4,
    "MS": 4, "NV": 4, "UT": 4, "NE": 3, "NM": 3, "ID": 2, "ME": 2, "MT": 2,
    "WV": 2, "DE": 1, "ND": 1, "SD": 1, "VT": 1, "WY": 1,
}

# --- Worker function ---------------------------------------------------------

def build_and_save_state(state: str, num_districts: int, iteration: int) -> str:
    print(f"[START] {state} ({num_districts} districts)")

    pack_path = os.path.join(BASE_PATH, f"{state}_2020_pack")
    if not os.path.exists(pack_path):
        pack_path = om.download_pack(state, str(BASE_PATH), verbose=1)

    mp = om.Map(pack_path)
    plan = om.Plan(mp, num_districts)

    plan.randomize()
    if num_districts > 1:
        plan.equalize("T_20_CENS_Total", tolerance=0.001, max_iter=20_000)
        
        objective = om.Objective(metrics=[
            om.Metric.population_deviation_smooth("T_20_CENS_Total"),
            om.Metric.compactness_polsby_popper(),
        ], weights=[0.8, 0.2])

        plan.anneal(
            objectives=[objective],
            max_iter=10_000_000 * num_districts,
            phase_start_probs=[0.9],
            phase_end_probs=[0.05],
            phase_cooling_rates=[0.0005 / math.sqrt(num_districts)],
            init_temp=1.0,
            early_stop_iters=10000,
            temp_search_batch_size=1000,
            batch_size=1000
        )

    svg_file = os.path.join(SVG_PATH, f"{state}_{iteration}.svg")
    plan.to_svg(svg_file)

    print(f"[DONE ] {state}")
    return state


# --- Main logic with Ctrl-C cancellation -------------------------------------
def generate_svgs(iteration: int, max_workers: int | None = None) -> None:
    """
    Run redistricting in parallel for all STATES, but cap the number of worker
    processes via max_workers to avoid exhausting CPU/RAM (especially in WSL).

    Args:
        iteration: some iteration index you already use
        max_workers: maximum number of worker processes to use. If None,
                     defaults to roughly half the available CPU cores.
    """
    if max_workers is None:
        # Heuristic: use half the cores, at least 1
        cpu_count = os.cpu_count() or 2
        max_workers = max(1, cpu_count // 2)

    print(f"Starting redistricting for {len(STATES)} states… "
          f"({max_workers} workers, Ctrl-C to cancel)")

    finished = 0

    # Limit the process pool size with max_workers
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(build_and_save_state, state, nd, iteration): state
            for state, nd in STATES.items()
        }

        try:
            for fut in as_completed(futures):
                state = futures[fut]
                try:
                    fut.result()
                    finished += 1
                    print(f"[PROGRESS] {finished}/{len(STATES)} completed (last: {state})")
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


def merge_svgs() -> None:
    if len(STATES) > 1:
        print("Merging SVGs…")

        svg_files = [os.path.join(SVG_PATH, f"{state}.svg") for state in STATES]
        merged = merge_svgs_geo(svg_files, width=2400, margin=10)
        out_path = os.path.join(SVG_PATH, "merged_conus.svg")
        write_svg(merged, out_path)
        
        print(f"[MERGED] {out_path}")


def main() -> None:
    os.makedirs(SVG_PATH, exist_ok=True)
    generate_svgs(iteration=1, max_workers=4)
    generate_svgs(iteration=2, max_workers=4)
    generate_svgs(iteration=3, max_workers=4)
    # merge_svgs()


if __name__ == "__main__":
    main()
