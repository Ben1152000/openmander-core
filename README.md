# openmander

A fast, memory-efficient redistricting toolchain in Rust.

## Quick start

```bash
# 1) Build
cargo build --release

# 2) Fetch + prepare data (example: Iowa 2020)
./target/release/openmander-core download IA

# 3) Generate a plan (four districts with equal population)
./target/release/openmander-core redistrict IA_2020_pack -o IA_out.csv -d 4
```

## CLI

Binary name: **`openmander-core`**

```text
Usage: openmander-core [OPTIONS] <COMMAND>

Commands:
  download    Download source data for a state (forbids stdout)
  redistrict  Build a redistricted plan (forbids stdout)

Options:
  -v, --verbose...    Increase output verbosity (-v, -vv)
  -h, --help          Print help
  -V, --version       Print version
```

### Subcommands

**download**

```text
openmander-core download [OPTIONS] <STATE>

  STATE    Two/three-letter code, e.g., IL, CA, PR

Options:
  -o, --output <DIR>   Output pack location (directory) [default: .]
```

**redistrict**

```text
openmander-core redistrict [OPTIONS] --districts <N> <PACK_DIR>

  PACK_DIR              Pack directory produced by `download`

Options:
  -d, --districts <N>   Number of districts (required)
  -o, --output <FILE>   Output plan file [default: ./plan.csv]
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

---

## License

TBD
