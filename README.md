# OpenMander

A fast, memory-efficient redistricting toolchain in Rust.

## Quickstart (CLI, Python, Rust)

### Command-line interface

Build & run:

```bash
# Build release binaries for the workspace
cargo build --release

# Fetch + prepare data (example: Iowa 2020)
./target/release/openmander-cli download IA

# Generate a plan (four districts with equal population)
./target/release/openmander-cli redistrict IA_2020_pack -o IA_out.csv -d 4
```

Or directly via Cargo:

```bash
cargo run -p openmander-cli -- download IA
cargo run -p openmander-cli -- redistrict IA_2020_pack -o IA_out.csv -d 4
```

### Python

Build and install the wheel locally (requires [maturin]):

```bash
# from repo root
python -m pip install -U maturin  # or: pipx install maturin
cd bindings/python
maturin develop -r                # builds and installs the 'openmander' module into your env
```

Use it:

```python
import openmander as om

iowa_map = om.Map("IA_2020_pack") # pack dir (see CLI quickstart to create)
plan = om.Plan(iowa_map, 4)       # 4 districts
plan.randomize()
plan.to_csv("plan.csv")
```

> Tip: if you prefer, `maturin build` then `pip install dist/openmander-*.whl`.

### Rust

Add the crate and build a tiny program:

```toml
# Cargo.toml (your app)
[dependencies]
openmander = { git = "https://github.com/Ben1152000/openmander-core" }
anyhow = "1"
```

```rust
// src/main.rs
use std::sync::Arc;
use anyhow::Result;
use openmander::{Map, Plan};

fn main() -> Result<()> {
    // Use a pack directory produced by the CLI "download" step (see CLI quickstart).
    let map = Arc::new(Map::read_from_pack("IA_2020_pack")?);

    // Create a 4-district plan, randomize, and save CSV
    let mut plan = Plan::new(map, 4);
    plan.randomize()?;
    plan.to_csv("plan.csv")?;
    Ok(())
}
```

Run it:

```bash
cargo run
```

## Components

* **Map**
  A container of layers (state → county → tract → group → VTD → block).
  Each `MapLayer` holds:

  * `geo_ids`, `index` (dense indices)
  * attributes (tabular data)
  * `adjacencies` + shared-perimeter weights (CSR)
  * optional geometries (FlatGeobuf)
* **Plan** (a.k.a. `GraphPartition`)
  A partition of the node graph into parts (districts):

  * `assignments[u] → part`
  * `boundary[u]` + per-part frontier sets
  * per-part size/weight totals (`WeightMatrix`)
  * contiguity checks and articulation-aware moves
  * simulated annealing helpers (balance, optimize)

## Pack layout (example)

```
<STATE>_2020_pack/
  data/             # per-level attribute tables (parquet)
  adj/              # CSR graphs per level (*.csr.bin)
  geom/             # per-level FlatGeobuf (*.fgb)
  manifest.json     # schema & provenance
```

## License

TBD

[maturin]: https://github.com/PyO3/maturin
