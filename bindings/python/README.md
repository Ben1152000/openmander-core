
# Openmander Python Bindings

This directory contains the Python bindings for Openmander.

## Installation

To install the Openmander Python bindings, you can use pip. Make sure you have Python ≥ 3.8 and pip installed on your system.

```bash
python -m pip install openmander
```

## Usage

Once installed, you can import the Openmander module in your Python scripts and start using its features.

```python
import openmander as om

# Download state pack for Illinois
pack_path = om.download("IL")
iowa_map = om.Map(pack_path)
plan = om.Plan(iowa_map, num_districts=17)

# Generate a random configuration of 17 districts.
plan.randomize()

# Balance the total population of each district.
plan.equalize("T_20_CENS_Total", tolerance=0.002, max_iter=1000)

# Output the block assignments to a csv file.
plan.to_csv("block-assign.csv")
```
