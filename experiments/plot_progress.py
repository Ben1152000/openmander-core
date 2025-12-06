#!/usr/bin/env python3
"""
Monitor annealing progress and plot in real-time.

This script:
1. Watches a log file for annealing output
2. Parses iteration, current_obj, and best_obj values
3. Updates a plot every time new data appears
4. Creates two subplots: full progress and recent zoom
"""
import sys
import time
import re
import math
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
except ImportError:
    print("ERROR: matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("ERROR: tqdm not installed. Run: pip install tqdm")
    sys.exit(1)


def parse_log_line(line):
    """Parse a log line to extract iteration and objective values."""
    # Example: Iter 0: obj 0.5234 | PopulationEquality=0.1234 CompactnessPolsbyPopper=0.2000 Competitiveness=0.2000 | best 0.5234 | temp 1.234567890123e-08 | prob 0.12345678 | curr_prob 0.12345678 | delta 0.12345678
    # Temp can be in scientific notation (e.g., 1.23e-08) or regular decimal
    # Try to match with delta and best_iter first (newest format)
    match = re.search(r'Iter (\d+): obj ([\d.\-eE]+) \| (.*?) \| best ([\d.\-eE]+) @ (\d+) \| temp ([\d.\-eE]+) \| prob ([\d.\-]+) \| curr_prob ([\d.\-]+) \| delta ([\d.\-eE]+)', line)
    if match:
        iteration = int(match.group(1))
        current_obj = float(match.group(2))
        metrics_str = match.group(3).strip()
        best_obj = float(match.group(4))
        best_iter = int(match.group(5))
        temp = float(match.group(6))
        prob = float(match.group(7))
        curr_prob = float(match.group(8))
        delta = float(match.group(9))
        
        # Parse individual metrics (handle negative numbers too)
        metrics = {}
        for metric_match in re.finditer(r'(\w+)=([\d.\-]+)', metrics_str):
            metric_name = metric_match.group(1)
            metric_value = float(metric_match.group(2))
            metrics[metric_name] = metric_value
        
        return iteration, current_obj, best_obj, metrics, temp, prob, curr_prob, delta, best_iter
    
    # Fall back to format with curr_prob but no delta or best_iter
    match = re.search(r'Iter (\d+): obj ([\d.\-eE]+) \| (.*?) \| best ([\d.\-eE]+) \| temp ([\d.\-eE]+) \| prob ([\d.\-]+) \| curr_prob ([\d.\-]+)', line)
    if match:
        iteration = int(match.group(1))
        current_obj = float(match.group(2))
        metrics_str = match.group(3).strip()
        best_obj = float(match.group(4))
        temp = float(match.group(5))
        prob = float(match.group(6))
        curr_prob = float(match.group(7))
        
        # Parse individual metrics (handle negative numbers too)
        metrics = {}
        for metric_match in re.finditer(r'(\w+)=([\d.\-]+)', metrics_str):
            metric_name = metric_match.group(1)
            metric_value = float(metric_match.group(2))
            metrics[metric_name] = metric_value
        
        return iteration, current_obj, best_obj, metrics, temp, prob, curr_prob, None, None
    
    # Fall back to old format without curr_prob
    match = re.search(r'Iter (\d+): obj ([\d.\-eE]+) \| (.*?) \| best ([\d.\-eE]+) \| temp ([\d.\-eE]+) \| prob ([\d.\-]+)', line)
    if match:
        iteration = int(match.group(1))
        current_obj = float(match.group(2))
        metrics_str = match.group(3).strip()
        best_obj = float(match.group(4))
        temp = float(match.group(5))
        prob = float(match.group(6))
        
        # Parse individual metrics (handle negative numbers too)
        metrics = {}
        for metric_match in re.finditer(r'(\w+)=([\d.\-]+)', metrics_str):
            metric_name = metric_match.group(1)
            metric_value = float(metric_match.group(2))
            metrics[metric_name] = metric_value
        
        return iteration, current_obj, best_obj, metrics, temp, prob, None, None, None
    return None


def update_plot(iterations, current_objs, best_objs, all_metrics, temps, probs, curr_probs, deltas, best_iters, output_path, max_y_log_temp=None, show_delta=True):
    """Update the progress plot."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # ===== SUBPLOT 1: Objective and Metrics =====
    # Collect all values to determine y-axis range
    all_values = list(current_objs)
    for metric_values in all_metrics.values():
        all_values.extend(metric_values)
    
    y_min = min(all_values)
    y_max = max(all_values)
    y_range = y_max - y_min
    y_margin = y_range * 0.1  # 10% margin
    
    # Plot current objective (thick blue line)
    ax1.plot(iterations, current_objs, 'b-', linewidth=2.5, alpha=0.8, label='Objective')
    
    # Plot individual metrics (dashed lines with different colors)
    colors = ['green', 'orange', 'purple', 'brown', 'pink']
    for i, (metric_name, metric_values) in enumerate(sorted(all_metrics.items())):
        color = colors[i % len(colors)]
        ax1.plot(iterations, metric_values, '--', linewidth=1.8, alpha=0.8, 
                color=color, label=metric_name)
    
    # Plot delta on a separate right-side y-axis (dashed red line)
    if show_delta and deltas and any(d is not None for d in deltas):
        ax1_delta = ax1.twinx()
        valid_deltas = [(i, it, d) for i, (it, d) in enumerate(zip(iterations, deltas)) if d is not None]
        
        # Plot all deltas with log scale: log(-delta) for negative, log(delta) for positive
        # Three colors based on epsilon threshold
        EPSILON = 1e-10
        if valid_deltas:
            # Categorize deltas into three groups
            large_neg = [(i, it, d) for i, it, d in valid_deltas if d < -EPSILON]
            small_neg = [(i, it, d) for i, it, d in valid_deltas if -EPSILON <= d < 0]
            pos_deltas = [(i, it, d) for i, it, d in valid_deltas if d > 0]
            
            # Plot large negative deltas in dark red
            if large_neg:
                _, iters, ds = zip(*large_neg)
                log_deltas = [math.log10(-d) for d in ds]
                ax1_delta.scatter(iters, log_deltas, s=20, alpha=0.6, color='darkred', 
                                 label=f'Δ < -ε (ε=1×10⁻¹⁰)')
            
            # Plot small negative deltas in magenta
            if small_neg:
                _, iters, ds = zip(*small_neg)
                log_deltas = [math.log10(-d) for d in ds]
                ax1_delta.scatter(iters, log_deltas, s=20, alpha=0.6, color='magenta', 
                                 label=f'-ε ≤ Δ < 0 (ε=1×10⁻¹⁰)')
            
            # Plot positive deltas in cyan
            if pos_deltas:
                _, iters, ds = zip(*pos_deltas)
                log_deltas = [math.log10(d) for d in ds]
                ax1_delta.scatter(iters, log_deltas, s=20, alpha=0.6, color='cyan', 
                                 label=f'Δ > 0')
            
            ax1_delta.set_ylabel(f'log10(|Δ|)  [ε = {EPSILON:.0e}]', fontsize=12)
            ax1_delta.tick_params(axis='y')
            
            # Add delta to legend
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax1_delta.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=10, loc='lower left')
        else:
            ax1.legend(fontsize=10, loc='lower left')
    else:
        # No delta, use regular legend
        ax1.legend(fontsize=10, loc='lower left')
    
    # Use the last checkpoint's best_obj and best_iter values
    if best_iters and best_iters[-1] is not None:
        actual_best_iter = best_iters[-1]
        best_obj_value = best_objs[-1]
        
        # Add vertical line at the actual best iteration
        ax1.axvline(x=actual_best_iter, color='darkred', linestyle='--', linewidth=2, alpha=0.8)
        
        # Add label for best value
        ax1.text(actual_best_iter, best_obj_value, f'  Best: {best_obj_value:.4f} @ x={actual_best_iter}',
                 verticalalignment='bottom', horizontalalignment='left',
                 fontsize=11, color='darkred', fontweight='bold',
                 bbox=dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor='darkred', alpha=0.9))
    
    # Set y-axis limits to show all data with margin
    ax1.set_ylim(y_min - y_margin, y_max + y_margin)
    
    ax1.set_xlabel('Iteration', fontsize=12)
    ax1.set_ylabel('Value', fontsize=12)
    ax1.set_title('Annealing Progress', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # ===== SUBPLOT 2: Acceptance Probability and Temperature =====
    # Create twin axis for temperature
    ax2_temp = ax2.twinx()
    
    # Plot acceptance probabilities on left axis
    if probs:
        ax2.plot(iterations, probs, 'r-', linewidth=2, alpha=0.8, label='Avg Accept Prob')
        ax2.set_ylim(-0.05, 1.05)
    
    # Plot current move probability if available
    if curr_probs and any(cp is not None for cp in curr_probs):
        # Filter out None values for plotting
        valid_curr_probs = [(i, it, cp) for i, (it, cp) in enumerate(zip(iterations, curr_probs)) if cp is not None]
        if valid_curr_probs:
            _, valid_iters, valid_cps = zip(*valid_curr_probs)
            ax2.plot(valid_iters, valid_cps, 'orange', linewidth=1.5, alpha=0.7, label='Current Move Prob')
    
    # Plot log10(temperature) on right axis
    log_temps = [math.log10(t) if t > 0 else float('-inf') for t in temps]
    ax2_temp.plot(iterations, log_temps, 'b-', linewidth=2, alpha=0.8, label='log10(Temperature)')
    
    # Set y-axis limits for temperature if max_y_log_temp is specified
    if max_y_log_temp is not None:
        # Find the minimum log temp value
        valid_log_temps = [lt for lt in log_temps if lt != float('-inf')]
        if valid_log_temps:
            min_log_temp = min(valid_log_temps)
            # Add 10% padding at the bottom
            temp_range = max_y_log_temp - min_log_temp
            padding = temp_range * 0.1
            ax2_temp.set_ylim(min_log_temp - padding, max_y_log_temp)
    
    ax2.set_xlabel('Iteration', fontsize=12)
    ax2.set_ylabel('Acceptance Probability', fontsize=12, color='r')
    ax2_temp.set_ylabel('log10(Temperature)', fontsize=12, color='b')
    ax2.set_title('Acceptance Probability and Temperature', fontsize=14, fontweight='bold')
    
    # Color the tick labels to match the lines
    ax2.tick_params(axis='y', labelcolor='r')
    ax2_temp.tick_params(axis='y', labelcolor='b')
    
    # Combine legends from both axes
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_temp.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, fontsize=10, loc='best')
    
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()


def monitor_log(log_path, plot_path, max_iter, plot_every=1, max_y_log_temp=None, show_delta=True):
    """Monitor log file and update plot in real-time."""
    
    iterations = []
    current_objs = []
    best_objs = []
    temps = []  # Temperatures
    probs = []  # Acceptance probabilities (average)
    curr_probs = []  # Current move probabilities
    deltas = []  # Delta values (old - new)
    best_iters = []  # Iteration where best was found
    all_metrics = {}  # Dict of metric_name -> list of values
    
    log_path = Path(log_path)
    
    # Wait for log file to be created (with timeout)
    timeout = 30  # seconds
    start_time = time.time()
    while not log_path.exists():
        if time.time() - start_time > timeout:
            print(f"ERROR: Log file not created after {timeout} seconds")
            return
        time.sleep(0.1)
    
    # Create progress bar
    pbar = tqdm(total=max_iter, desc="Annealing", unit="iter", 
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, best={postfix}]')
    
    last_iteration = 0
    lines_read = 0
    data_points = 0
    should_exit = False
    
    with open(log_path, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                # Check if process is still running by looking for a sentinel file
                sentinel = log_path.parent / f"{log_path.stem}.running"
                if not sentinel.exists():
                    # Process finished, do final update and exit
                    if iterations:
                        update_plot(iterations, current_objs, best_objs, all_metrics, temps, probs, curr_probs, deltas, best_iters, plot_path, max_y_log_temp, show_delta)
                        pbar.close()
                        print(f"\nPlotted {len(all_metrics)} metrics: {', '.join(all_metrics.keys())}")
                    else:
                        pbar.close()
                        print(f"\nWARNING: No data points found in log file")
                    
                    should_exit = True
                    break
                time.sleep(0.1)
                continue
            
            lines_read += 1
            parsed = parse_log_line(line)
            if parsed:
                iteration, current_obj, best_obj, metrics, temp, prob, curr_prob, delta, best_iter = parsed
                iterations.append(iteration)
                current_objs.append(current_obj)
                best_objs.append(best_obj)
                temps.append(temp)
                probs.append(prob)
                curr_probs.append(curr_prob)
                deltas.append(delta)
                best_iters.append(best_iter)
                data_points += 1
                
                # Debug: print first parsed line to see what metrics we're getting
                if data_points == 1:
                    print(f"Detected metrics: {list(metrics.keys())}")
                
                # Collect metric values
                for metric_name, metric_value in metrics.items():
                    if metric_name not in all_metrics:
                        all_metrics[metric_name] = []
                    all_metrics[metric_name].append(metric_value)
                
                # Update progress bar
                delta = iteration - last_iteration
                if delta > 0:
                    pbar.update(delta)
                    pbar.set_postfix_str(f"{best_obj:.4f}")
                    last_iteration = iteration
                
                # Update plot every N data points
                if data_points % plot_every == 0:
                    update_plot(iterations, current_objs, best_objs, all_metrics, temps, probs, curr_probs, deltas, best_iters, plot_path, max_y_log_temp, show_delta)
    
    # Create a "done" file to signal plotting is complete (after loop exits)
    if should_exit:
        done_file = log_path.parent / f"{log_path.stem}.plot_done"
        done_file.touch()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor annealing progress and plot in real-time.')
    parser.add_argument('log_path', help='Path to log file')
    parser.add_argument('plot_path', help='Path to output plot file')
    parser.add_argument('max_iter', type=int, help='Maximum iterations')
    parser.add_argument('--plot-every', type=int, default=1, help='Update plot every N data points (default: 1)')
    parser.add_argument('--max-y-log-temp', type=float, default=None, help='Maximum y-value for log10(temp) axis, e.g., -6')
    parser.add_argument('--no-delta', action='store_true', help='Hide delta dots from the plot')
    
    args = parser.parse_args()
    
    monitor_log(args.log_path, args.plot_path, args.max_iter, args.plot_every, args.max_y_log_temp, not args.no_delta)
