#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

STATE=CT
NUM_DISTRICTS=5
MAX_ITER=$((1000000))  # Safety maximum (very high)
INIT_TEMP=1.0
COOLING_RATE=0.99999 #6  # Slower cooling (closer to 1.0 = slower)
EARLY_STOP_ITERS=$((1000000))
WINDOW_SIZE=1000
LOG_EVERY=1000  # Print progress every N iterations
PLOT_EVERY=10  # Update plot every N data points
MAX_Y_LOG_TEMP="0"  # Maximum y-value for log10(temp) axis (e.g., -6), leave empty for auto
DELTA_ONLY_NEG="" # "--delta-only-neg"  # Set to "--delta-only-neg" to only plot negative deltas as log10(-delta), leave empty for all deltas

# Metric weights (set to 0 to disable a metric)
POP_WEIGHT=0.6
COMPACTNESS_WEIGHT=0.2
COMPETITIVENESS_WEIGHT=0.2
COMPETITIVENESS_THRESHOLD=0.05

ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

# Create artifacts directory if it doesn't exist
mkdir -p "$ARTIFACTS_DIR"

# Find the next run number
RUN_NUMBER=1
while ls "$ARTIFACTS_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}"* 1> /dev/null 2>&1; do
    RUN_NUMBER=$((RUN_NUMBER + 1))
done

OUT_DIR="$ARTIFACTS_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}"
LOG_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_anneal.log"
PLOT_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_progress.png"
SENTINEL_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_anneal.running"

# Cleanup function
cleanup() {
    echo ""
    echo "Interrupted! Cleaning up..."
    if [ ! -z "$RUN_PID" ]; then
        kill $RUN_PID 2>/dev/null
        echo "Killed optimization process (PID: $RUN_PID)"
    fi
    if [ -f "$SENTINEL_FILE" ]; then
        rm "$SENTINEL_FILE"
    fi
    exit 1
}

# Set up trap to catch Ctrl+C and other signals
trap cleanup INT TERM



# Start the optimization in the background
python "$SCRIPT_DIR/run.py" \
  --state=$STATE \
  --num_districts=$NUM_DISTRICTS \
  --max_iter=$MAX_ITER \
  --init_temp=$INIT_TEMP \
  --cooling_rate=$COOLING_RATE \
  --early_stop_iters=$EARLY_STOP_ITERS \
  --window_size=$WINDOW_SIZE \
  --log_every=$LOG_EVERY \
  --pop_weight=$POP_WEIGHT \
  --compactness_weight=$COMPACTNESS_WEIGHT \
  --competitiveness_weight=$COMPETITIVENESS_WEIGHT \
  --competitiveness_threshold=$COMPETITIVENESS_THRESHOLD \
  --log_file="$LOG_FILE" \
  --artifacts_path="$ARTIFACTS_DIR" &

RUN_PID=$!

# Wait for log file to be created
while [ ! -f "$LOG_FILE" ]; do
    sleep 0.1
done

# Now monitor the log file and show progress
PLOT_CMD="python -u $SCRIPT_DIR/plot_progress.py $LOG_FILE $PLOT_FILE $MAX_ITER --plot-every $PLOT_EVERY"
if [ ! -z "$MAX_Y_LOG_TEMP" ]; then
    PLOT_CMD="$PLOT_CMD --max-y-log-temp $MAX_Y_LOG_TEMP"
fi
if [ ! -z "$DELTA_ONLY_NEG" ]; then
    PLOT_CMD="$PLOT_CMD $DELTA_ONLY_NEG"
fi
eval $PLOT_CMD

# Wait for the optimization to finish
wait $RUN_PID
