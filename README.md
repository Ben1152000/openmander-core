# OpenMander

OpenMander is a fast, memory-efficient, research-oriented toolkit for computational redistricting, implemented in Rust with Python bindings. It supports large-scale, multi-objective redistricting experiments at census-block resolution, with a focus on incremental evaluation, contiguity-aware moves, and heuristic search.

### Paper

> **OpenMander: A Multi-Objective Optimization Framework for Computational Redistricting**  
> *Under submission.*  
> Preprint: TBD

## Quickstart (Python, Rust)

### Python

Install from PyPi:

```bash
python -m pip install openmander
```

Or build locally from source (requires [maturin]):

```bash
python -m pip install -U maturin
cd bindings/python
maturin develop -r
```

Example usage:

```python
import openmander as om

# Download and prepare a state-level data pack (Illinois, 2020 census)
pack_path = om.download("IL")

# Load the map and initialize a 17-district plan
mp = om.Map(pack_path)
plan = om.Plan(mp, num_districts=17)

# Generate a random initial plan
plan.randomize()

# Balance population across districts
plan.equalize(
    "T_20_CENS_Total",
    tolerance=0.002,
    max_iter=1000,
)

# Export block-level assignments
plan.to_csv("block_assignments.csv")
```

The Python interface exposes high-level abstractions (`Map`, `Plan`, and optimization helpers) suitable for exploratory analysis, batch experiments, and integration with scientific Python tooling.

### Rust

Add OpenMander as a dependency:

```toml
# Cargo.toml
[dependencies]
openmander = { git = "https://github.com/Ben1152000/openmander-core" }
anyhow = "1"
```

Minimal example:

```rust
use std::sync::Arc;
use anyhow::Result;
use openmander::{Map, Plan};

fn main() -> Result<()> {
    // Load a previously prepared data pack
    let map = Arc::new(Map::read_from_pack("IA_2020_pack")?);

    // Create a 4-district plan
    let mut plan = Plan::new(map, 4);

    // Randomize and export assignments
    plan.randomize()?;
    plan.to_csv("plan.csv")?;

    Ok(())
}
```

## Components

### Map

A Map represents a multi-layer geographic dataset with explicit graph structure.
Each map consists of ordered layers (e.g., state → county → tract → block), where each layer contains:

* Dense node indices and stable geographic identifiers
* Tabular attributes (e.g., census variables)
* Adjacency graphs with optional shared-perimeter weights (CSR format)
* Optional geometries (stored as FlatGeobuf)

Maps are immutable after construction and shared across plans.

* **Plan** (a.k.a. `GraphPartition`)
  A partition of the node graph into parts (districts):

  * `assignments[u] → part`
  * `boundary[u]` + per-part frontier sets
  * per-part size/weight totals (`WeightMatrix`)
  * contiguity checks and articulation-aware moves
  * simulated annealing helpers (balance, optimize)

### Plan

A Plan (internally a graph partition) assigns each node in a map layer to a district, and contains:

* Incremental boundary tracking and per-district frontier sets
* Per-district aggregate weights (WeightMatrix)
* Fast contiguity checks and articulation-aware moves
* Built-in local search helpers (randomization, balancing, optimization)

Plans are designed to support heuristic search algorithms such as simulated annealing, tabu search, and beam search.

### Data Packs

OpenMander operates on preprocessed data packs, which bundle all required data for a state and census decade.

Example layout:

```
<STATE>_2020_pack/
  data/             # per-level attribute tables (parquet)
  adj/              # CSR graphs per level (*.csr.bin)
  geom/             # per-level FlatGeobuf (*.fgb)
  hull/             # per-level convex hulls (*.fgb)
  manifest.json     # schema & provenance
```

## License

License: TBD
