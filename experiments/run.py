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
    config: str = None,
    base_path: str = None,
    log_file: str = None,
    artifacts_path: str = None,
):
    """
    Run multi-phase redistricting optimization with adaptive annealing.
    
    Args:
        config: Path to JSON config file (default: ./config.json)
        base_path: Base path for packs (default: ../../packs/)
        log_file: Path to log file for annealing output (default: None = stdout)
        artifacts_path: Path to artifacts directory (default: ./artifacts)
    """
    # Load config
    if config is None:
        config = Path(__file__).parent / 'config.json'
    else:
        config = Path(config)
    
    if not config.exists():
        raise FileNotFoundError(f"Config file not found: {config}")
    
    with open(config) as f:
        cfg = json.load(f)
    
    # Extract top-level parameters
    state = cfg['state']
    num_districts = cfg['num_districts']
    max_iter = cfg['max_iter']
    batch_size = cfg['batch_size']
    competitiveness_threshold = cfg['competitiveness_threshold']
    
    # Extract phase configurations
    phases = cfg['phases']
    if not phases:
        raise ValueError("Config must specify at least one phase")
    
    # Validate phase names are unique
    phase_names = [phase.get('name', f'phase_{i}') for i, phase in enumerate(phases)]
    if len(phase_names) != len(set(phase_names)):
        duplicates = [name for name in phase_names if phase_names.count(name) > 1]
        raise ValueError(f"Duplicate phase names found: {set(duplicates)}")
    
    # Extract parameters
    init_temp = cfg.get('init_temp', 1.0)
    temp_search_batch_size = cfg.get('temp_search_batch_size', batch_size)
    default_cooling_rate = cfg.get('cooling_rate', 0.01)  # Fallback if phase doesn't specify
    
    # Extract start_prob, end_prob, cooling_rate, and early_stop_iters for each phase
    phase_start_probs = []
    phase_end_probs = []
    phase_cooling_rates = []
    early_stop_iters = None
    
    for phase in phases:
        start_prob = phase.get('start_prob')
        end_prob = phase.get('end_prob')
        cooling_rate = phase.get('cooling_rate', default_cooling_rate)
        
        if start_prob is None:
            raise ValueError(f"Phase '{phase.get('name')}' must have a start_prob")
        
        phase_start_probs.append(start_prob)
        phase_end_probs.append(end_prob)  # Can be None
        phase_cooling_rates.append(cooling_rate)
        
        # If end_prob is None, check for early_stop_iters
        if end_prob is None and 'early_stop_iters' in phase:
            early_stop_iters = phase['early_stop_iters']
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
    
    # Save config to output directory
    config_copy_path = out_dir / f"{state}_{num_districts}_{run_number}_config.json"
    with open(config_copy_path, 'w') as f:
        json.dump(cfg, f, indent=2)

    # Print command line arguments
    print('\n' + ' '.join([sys.argv[0].split('/')[-1]] + sys.argv[1:]))
    print(f"\n{'='*80}")
    print(f"Run #{run_number}: {state} with {num_districts} districts")
    print(f"Config: {config.name}")
    print(f"{'='*80}\n")
    
    # Load map and create plan (silently)
    map_obj = om.Map(str(pack_path))
    plan = om.Plan(map_obj, num_districts=num_districts)
    
    # Create objectives for each phase
    objectives = []
    for phase_idx, phase in enumerate(phases, 1):
        phase_weights = phase['weights']
        metrics = []
        weights = []
        
        # Include metric if weight is not None (weight of 0 means track but don't optimize)
        if 'population' in phase_weights and phase_weights['population'] is not None:
            metrics.append(om.Metric.population_deviation("T_20_CENS_Total"))
            weights.append(phase_weights['population'])
        
        if 'compactness' in phase_weights and phase_weights['compactness'] is not None:
            metrics.append(om.Metric.compactness_polsby_popper())
            weights.append(phase_weights['compactness'])
        
        if 'competitiveness' in phase_weights and phase_weights['competitiveness'] is not None:
            metrics.append(om.Metric.competitiveness(
                dem_series="E_20_PRES_Dem",
                rep_series="E_20_PRES_Rep",
                threshold=competitiveness_threshold
            ))
            weights.append(phase_weights['competitiveness'])
        
        if not metrics:
            raise ValueError(f"Phase {phase_idx}: At least one metric must be specified (use weight=0 to track without optimizing)")
        
        objectives.append(om.Objective(metrics=metrics, weights=weights))
    
    # Use first objective for initial scoring
    objective = objectives[0]
    first_phase_weights = phases[0]['weights']
    metric_names = []
    if 'population' in first_phase_weights and first_phase_weights['population'] is not None:
        metric_names.append("pop_dev")
    if 'compactness' in first_phase_weights and first_phase_weights['compactness'] is not None:
        metric_names.append("compactness")
    if 'competitiveness' in first_phase_weights and first_phase_weights['competitiveness'] is not None:
        metric_names.append("competitiveness")
    
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
    print(f"\nMulti-Phase Annealing: {len(objectives)} phases")
    print(f"  max_iter={max_iter:,}, init_temp={init_temp}")
    print(f"  phase_start_probs={phase_start_probs}")
    print(f"  phase_end_probs={phase_end_probs}")
    print(f"  phase_cooling_rates={phase_cooling_rates}")
    print(f"  DEBUG: Cooling rates by phase:")
    for i, rate in enumerate(phase_cooling_rates):
        print(f"    Phase {i+1}: {rate}")
    if early_stop_iters:
        print(f"  early_stop_iters={early_stop_iters:,}")
    print(f"  temp_search_batch_size={temp_search_batch_size}, batch_size={batch_size}")
    
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
                objectives=objectives,
                max_iter=max_iter,
                phase_start_probs=phase_start_probs,
                phase_end_probs=phase_end_probs,
                phase_cooling_rates=phase_cooling_rates,
                init_temp=init_temp,
                early_stop_iters=early_stop_iters,
                temp_search_batch_size=temp_search_batch_size,
                batch_size=batch_size
            )
    else:
        # Print to stdout
        plan.anneal(
            objectives=objectives,
            max_iter=max_iter,
            phase_start_probs=phase_start_probs,
            phase_end_probs=phase_end_probs,
            phase_cooling_rates=phase_cooling_rates,
            init_temp=init_temp,
            early_stop_iters=early_stop_iters,
            temp_search_batch_size=temp_search_batch_size,
            batch_size=batch_size
        )
    
    # Remove sentinel file to signal annealing is complete
    if sentinel_path and sentinel_path.exists():
        import time
        sentinel_path.unlink()
        
        # Wait for plotting script to finish (it creates a .plot_done file)
        plot_done_file = sentinel_path.parent / f"{sentinel_path.stem.replace('.running', '')}.plot_done"
        timeout = 30  # seconds
        start_time = time.time()
        while not plot_done_file.exists():
            if time.time() - start_time > timeout:
                print("Warning: Plotting script did not finish within timeout")
                break
            time.sleep(0.1)
        
        # Clean up the plot_done file
        if plot_done_file.exists():
            plot_done_file.unlink()
    
    # Score and save final plan (after plotting is done)
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
        "config": cfg,
        "init_rand_metrics": init_rand_metrics,
        "equalized_metrics": equalized_metrics,
        "final_metrics": final_metrics,
    }
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics_data, f, indent=2)
    
    print(f"\n{'='*80}")
    saved_files = [
        config_copy_path.name,
        rand_init_svg_path.name, rand_init_csv_path.name,
        equalized_svg_path.name, equalized_csv_path.name,
        final_svg_path.name, final_csv_path.name,
        metrics_path.name
    ]
    if log_file:
        saved_files.append(Path(log_file).name)
    print(f"Saved: {', '.join(saved_files)}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    fire.Fire(main)
