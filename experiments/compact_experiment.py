#!/usr/bin/env python3
"""Run parallel compactness optimization experiments for a state."""
import argparse
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import openmander as om

SCRIPT_DIR = Path(__file__).resolve().parent
PACKS_DIR = SCRIPT_DIR.parent / "packs"


def run_experiment(state: str, num_districts: int, population: str,
                   pack_dir: str, out_dir: str, iteration: int) -> str:
    print(f"[START] {state} run {iteration} ({num_districts} districts)")

    pack_path = os.path.join(pack_dir, f"{state}_2020_pack")
    if not os.path.exists(pack_path):
        pack_path = om.download_pack(state, pack_dir, verbose=1)

    mp = om.Map(pack_path)
    plan = om.Plan(mp, num_districts)
    plan.randomize()

    if num_districts > 1:
        plan.equalize(population, tolerance=0.001, max_iter=20_000)

        plan.anneal(
            objectives=[om.Objective(metrics=[
                om.Metric.population_deviation_sharp(population),
                om.Metric.compactness_polsby_popper(),
            ], weights=[0.6, 0.4])],
            max_iter=10_000_000 * num_districts,
            phase_start_probs=[0.8],
            phase_end_probs=[0.05],
            phase_cooling_rates=[0.0005 / math.sqrt(num_districts)],
            init_temp=1.0,
            early_stop_iters=10_000,
            temp_search_batch_size=10_000,
            batch_size=10_000,
        )

        plan.anneal(
            objectives=[om.Objective(metrics=[
                om.Metric.population_deviation_sharp(population),
                om.Metric.compactness_polsby_popper(),
            ], weights=[0.8, 0.2])],
            max_iter=1_000_000 * num_districts,
            phase_start_probs=[0.8],
            phase_end_probs=[0.05],
            phase_cooling_rates=[0.0005 / math.sqrt(num_districts)],
            init_temp=1.0,
            early_stop_iters=10_000,
            temp_search_batch_size=10_000,
            batch_size=10_000,
        )

    svg_path = os.path.join(out_dir, f"{state}_{iteration}.svg")
    csv_path = os.path.join(out_dir, f"{state}_{iteration}.csv")
    plan.to_svg(svg_path)
    plan.to_csv(csv_path)

    pop_dev = plan.compute_metric(om.Metric.population_deviation_absolute(population))
    compactness = plan.compute_metric(om.Metric.compactness_polsby_popper())
    max_dev = max(pop_dev) if pop_dev else 0
    avg_compact = sum(compactness) / len(compactness) if compactness else 0
    print(f"[METRICS] {state} run {iteration}: max_pop_dev={max_dev:.4f}, avg_compactness={avg_compact:.4f}")
    print(f"[DONE] {state} run {iteration}")
    return f"{state}_{iteration}"


def main():
    parser = argparse.ArgumentParser(description="Run parallel compactness optimization experiments")
    parser.add_argument("--state", required=True, help="Two-letter state code (e.g. NJ)")
    parser.add_argument("--districts", type=int, required=True, help="Number of districts")
    parser.add_argument("--runs", type=int, default=1, help="Number of independent runs (default: 1)")
    parser.add_argument("--workers", type=int, default=None, help="Max parallel workers (default: cpu_count // 2)")
    parser.add_argument("--population", default="T_20_CENS_Total", help="Population series name")
    parser.add_argument("--pack-dir", type=Path, default=PACKS_DIR, help="Directory for packs")
    parser.add_argument("--out-dir", type=Path, default=SCRIPT_DIR / "artifacts", help="Output directory")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    max_workers = args.workers or max(1, (os.cpu_count() or 2) // 2)

    print(f"Running {args.runs} experiment(s) for {args.state} "
          f"({args.districts} districts), {max_workers} workers")

    finished = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                run_experiment, args.state, args.districts, args.population,
                str(args.pack_dir), str(args.out_dir), i + 1
            ): i + 1
            for i in range(args.runs)
        }
        try:
            for fut in as_completed(futures):
                run_id = futures[fut]
                try:
                    fut.result()
                    finished += 1
                    print(f"[PROGRESS] {finished}/{args.runs} completed")
                except Exception as e:
                    print(f"[ERROR] run {run_id}: {e}")
        except KeyboardInterrupt:
            print("\n[CTRL-C] Terminating workers…")
            for f in futures:
                f.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            print("[EXIT] Cancelled.")
            return

    print(f"Done. {finished}/{args.runs} runs completed.")


if __name__ == "__main__":
    main()
