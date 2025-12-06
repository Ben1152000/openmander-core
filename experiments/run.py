#!/usr/bin/env python3
"""
Run redistricting optimization with command line arguments.
"""
import os
import sys
import json
from pathlib import Path
from contextlib import contextmanager
import fire
import openmander as om


@contextmanager
def redirect_fds_to_file(filepath):
    """Redirect file descriptors 1 (stdout) and 2 (stderr) to a file."""
    # Flush any pending output
    sys.stdout.flush()
    sys.stderr.flush()

    # Save original file descriptors
    old_stdout_fd = os.dup(1)
    old_stderr_fd = os.dup(2)

    try:
        # Open file in unbuffered mode
        fd = os.open(filepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.dup2(fd, 1)
        os.dup2(fd, 2)
        os.close(fd)
        yield
    finally:
        # Restore original fds
        os.dup2(old_stdout_fd, 1)
        os.dup2(old_stderr_fd, 2)
        os.close(old_stdout_fd)
        os.close(old_stderr_fd)
        
        # Flush again
        sys.stdout.flush()
        sys.stderr.flush()


def get_next_run_number(state: str, num_districts: int, artifacts_dir: Path) -> int:
    """Find the next available run number for this state/num_districts combination."""
    pattern = f"{state}_{num_districts}_"
    existing = [f for f in artifacts_dir.glob(f"{pattern}*")]
    
    if not existing:
        return 1
    
    # Extract run numbers from filenames
    run_numbers = []
    for f in existing:
        parts = f.stem.split('_')
        if len(parts) >= 3:
            try:
                run_numbers.append(int(parts[2]))
            except ValueError:
                continue
    
    return max(run_numbers, default=0) + 1


def save_plan_image(plan: om.Plan, filepath: Path):
    """Save plan as SVG then convert to image format."""
    svg_path = filepath.with_suffix('.svg')
    plan.to_svg(str(svg_path))
    
    # For now, just keep the SVG
    # TODO: Add conversion to PNG/JPEG if needed
    if filepath.suffix in ['.png', '.jpeg', '.jpg']:
        # Would need to add conversion here (e.g., using cairosvg or similar)
        print(f"Note: Saved as SVG at {svg_path}. PNG/JPEG conversion not yet implemented.")


def main(
    state: str,
    num_districts: int,
    max_iter: int,
    init_temp: float,
    cooling_rate: float,
    early_stop_iters: int,
    pop_weight: float,
    compactness_weight: float,
    competitiveness_weight: float,
    competitiveness_threshold: float,
    window_size: int = 1000,
    log_every: int = 1000,
    base_path: str = None,
    log_file: str = None,
    artifacts_path: str = None,
):
    """
    Run redistricting optimization with adaptive annealing.
    
    Args:
        state: State abbreviation (e.g., CT, IL)
        num_districts: Number of districts to create
        max_iter: Safety maximum iterations (prevents infinite loops)
        init_temp: Initial temperature guess for phase 1
        cooling_rate: Geometric cooling rate (temp *= rate each iteration)
        early_stop_iters: Stop phase 3 after this many iterations without improvement
        pop_weight: Weight for population deviation metric
        compactness_weight: Weight for compactness metric
        competitiveness_weight: Weight for competitiveness metric
        competitiveness_threshold: Threshold for competitiveness metric
        window_size: Rolling window size for measuring acceptance rates (default: 1000)
        base_path: Base path for packs (default: ../../packs/)
        log_file: Path to log file for annealing output (default: None = stdout)
        artifacts_path: Path to artifacts directory (default: ./artifacts)
    """
    # Setup paths
    if base_path is None:
        # From demo/run.py: demo -> python -> bindings -> openmander-core -> workspace_root
        base_path = Path(__file__).parent.parent.parent / 'packs'
    else:
        base_path = Path(base_path)
    
    if artifacts_path is None:
        artifacts_dir = Path(__file__).parent / 'artifacts'
    else:
        artifacts_dir = Path(artifacts_path)
    
    artifacts_dir.mkdir(exist_ok=True, parents=True)
    
    pack_path = base_path / f"{state}_2020_pack"
    
    # Get next run number
    run_number = get_next_run_number(state, num_districts, artifacts_dir)

    out_dir = artifacts_dir / f"{state}_{num_districts}_{run_number}"
    out_dir.mkdir(exist_ok=True, parents=True)

    # Print command line arguments
    print('\n' + ' '.join([sys.argv[0].split('/')[-1]] + sys.argv[1:]))
    print(f"\n{'='*80}")
    print(f"Run #{run_number}: {state} with {num_districts} districts")
    print(f"{'='*80}\n")
    
    # Load map and create plan (silently)
    map_obj = om.Map(str(pack_path))
    plan = om.Plan(map_obj, num_districts=num_districts)
    
    # Define metrics (only if weight > 0)
    metrics = []
    weights = []
    metric_names = []
    
    if pop_weight > 0:
        metrics.append(om.Metric.population_deviation("T_20_CENS_Total"))
        weights.append(pop_weight)
        metric_names.append("pop_dev")
    
    if compactness_weight > 0:
        metrics.append(om.Metric.compactness_polsby_popper())
        weights.append(compactness_weight)
        metric_names.append("compactness")
    
    if competitiveness_weight > 0:
        metrics.append(om.Metric.competitiveness(
            dem_series="E_20_PRES_Dem",
            rep_series="E_20_PRES_Rep",
            threshold=competitiveness_threshold
        ))
        weights.append(competitiveness_weight)
        metric_names.append("competitiveness")
    
    if not metrics:
        raise ValueError("At least one metric weight must be > 0")
    
    objective = om.Objective(metrics=metrics, weights=weights)
    
    def score_plan(plan, label=""):
        """Compute and return metrics for the plan."""
        objective_score = plan.compute_objective(objective)
        
        result = {"objective": objective_score}
        
        if label:
            print(f"{label}:")
            print(f"  Objective: {objective_score:.4f}")
        
        for i, (metric, name) in enumerate(zip(metrics, metric_names)):
            scores = plan.compute_metric(metric)
            metric_score = plan.compute_metric_score(metric)
            result[name] = scores
            result[f"{name}_score"] = metric_score
            if label:
                scores_str = ' '.join([f'{x:.3f}' for x in scores])
                print(f"  {name:12s}: [{scores_str}] score={metric_score:.4f}")
        
        return result
    
    # Randomize and score initial plan
    plan.randomize()
    init_rand_metrics = score_plan(plan, "Initial Random")
    
    # Save random initial plan
    rand_init_svg_path = out_dir / f"{state}_{num_districts}_{run_number}_rand_init.svg"
    rand_init_csv_path = out_dir / f"{state}_{num_districts}_{run_number}_rand_init.csv"
    save_plan_image(plan, rand_init_svg_path)
    plan.to_csv(path=str(rand_init_csv_path))
    
    # Equalize population before annealing
    print("\nEqualizing population...")
    plan.equalize(series="T_20_CENS_Total", tolerance=0.00005, max_iter=1000)
    equalized_metrics = score_plan(plan, "After Equalization")
    
    # Save equalized plan
    equalized_svg_path = out_dir / f"{state}_{num_districts}_{run_number}_equalized.svg"
    equalized_csv_path = out_dir / f"{state}_{num_districts}_{run_number}_equalized.csv"
    save_plan_image(plan, equalized_svg_path)
    plan.to_csv(path=str(equalized_csv_path))
    
    # Run annealing with optional log file redirection
    print(f"\nAdaptive Annealing: max_iter={max_iter:,}, init_temp={init_temp}, cooling_rate={cooling_rate}, early_stop_iters={early_stop_iters:,}")
    
    sentinel_path = None
    if log_file:
        # Redirect to log file
        log_path = Path(log_file)
        sentinel_path = log_path.parent / f"{log_path.stem}.running"
        
        # Create sentinel file to signal process is running
        sentinel_path.touch()
        
        # Redirect file descriptors to log file (captures Rust println! output)
        with redirect_fds_to_file(log_path):
            plan.anneal(
                objective=objective,
                max_iter=max_iter,
                init_temp=init_temp,
                cooling_rate=cooling_rate,
                early_stop_iters=early_stop_iters,
                window_size=window_size,
                log_every=log_every
            )
    else:
        # Print to stdout
        plan.anneal(
            objective=objective,
            max_iter=max_iter,
            init_temp=init_temp,
            cooling_rate=cooling_rate,
            early_stop_iters=early_stop_iters,
            window_size=window_size,
            log_every=log_every
        )
    
    # Score and save final plan
    print()
    final_metrics = score_plan(plan, "Final")
    
    # Save final plan
    final_svg_path = out_dir / f"{state}_{num_districts}_{run_number}_final.svg"
    final_csv_path = out_dir / f"{state}_{num_districts}_{run_number}_final.csv"
    save_plan_image(plan, final_svg_path)
    plan.to_csv(path=str(final_csv_path))
    
    # Save metrics to JSON
    metrics_path = out_dir / f"{state}_{num_districts}_{run_number}_metrics.json"
    metrics_data = {
        "state": state,
        "num_districts": num_districts,
        "run_number": run_number,
        "parameters": {
            "max_iter": max_iter,
            "init_temp": init_temp,
            "cooling_rate": cooling_rate,
            "early_stop_iters": early_stop_iters,
            "window_size": window_size,
            "pop_weight": pop_weight,
            "compactness_weight": compactness_weight,
            "competitiveness_weight": competitiveness_weight,
            "competitiveness_threshold": competitiveness_threshold,
        },
        "init_rand_metrics": init_rand_metrics,
        "equalized_metrics": equalized_metrics,
        "final_metrics": final_metrics,
    }
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics_data, f, indent=2)
    
    print(f"\n{'='*80}")
    saved_files = [
        rand_init_svg_path.name, rand_init_csv_path.name,
        equalized_svg_path.name, equalized_csv_path.name,
        final_svg_path.name, final_csv_path.name,
        metrics_path.name
    ]
    if log_file:
        saved_files.append(Path(log_file).name)
    print(f"Saved: {', '.join(saved_files)}")
    print(f"{'='*80}\n")
    
    # Remove sentinel file to signal completion (after all files are saved)
    if sentinel_path and sentinel_path.exists():
        sentinel_path.unlink()


if __name__ == "__main__":
    fire.Fire(main)
