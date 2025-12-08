#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Load config file (default to config.json, or use first argument)
CONFIG_FILE="${1:-$SCRIPT_DIR/config.json}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "Using config: $CONFIG_FILE"

ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

# Create artifacts directory if it doesn't exist
mkdir -p "$ARTIFACTS_DIR"

# Extract state and num_districts from config to determine run number
STATE=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['state'])")
NUM_DISTRICTS=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['num_districts'])")

# Find the next run number
RUN_NUMBER=1
while ls "$ARTIFACTS_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}"* 1> /dev/null 2>&1; do
    RUN_NUMBER=$((RUN_NUMBER + 1))
done

OUT_DIR="$ARTIFACTS_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}"
LOG_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_anneal.log"
PLOT_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_progress.png"
SENTINEL_FILE="$OUT_DIR/${STATE}_${NUM_DISTRICTS}_${RUN_NUMBER}_anneal.running"

# Extract plot settings from config
PLOT_EVERY=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['plot_every'])")
MAX_ITER=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['max_iter'])")
MAX_Y_LOG_TEMP=$(python3 -c "import json; c=json.load(open('$CONFIG_FILE')); print(c.get('max_y_log_temp', ''))")

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
  --config="$CONFIG_FILE" \
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
eval $PLOT_CMD

# Wait for the optimization to finish
wait $RUN_PID
