# geograph

A Rust crate for representing and querying **planar geographic maps**: complete subdivisions of a region into non-overlapping units such as census blocks or precincts.

Built as the geometric foundation for [openmander](https://github.com/bdarnell/openmander).

## Overview

The central type is [`Region`]: a planar map of geographic units backed by a half-edge DCEL, with pre-cached geometry metrics and CSR adjacency matrices. Build one from a `Vec<MultiPolygon<f64>>` (one geometry per unit); query it for area, adjacency, topology, and boundary geometry.

```rust
use geograph::{Region, UnitId};
use geo::MultiPolygon;

let geometries: Vec<MultiPolygon<f64>> = load_census_blocks();
let region = Region::new(geometries, None)?;

// Adjacency
let neighbors = region.neighbors(UnitId(0));
let shared_len = region.shared_boundary_length(UnitId(0), UnitId(1));

// Geometry
let area   = region.area(UnitId(0));       // m²
let perim  = region.perimeter(UnitId(0));  // m
let bounds = region.bounds(UnitId(0));     // lon/lat Rect

// Topology
let contiguous = region.is_contiguous([UnitId(0), UnitId(1), UnitId(2)]);
let enclaves   = region.enclaves([UnitId(0), UnitId(1)]);
```

## Features

**Adjacency**
- Rook (shared edge) and Queen (shared point) adjacency matrices in CSR format
- Per-edge shared boundary lengths on the Rook matrix
- `neighbors`, `are_adjacent`, `shared_boundary_length`

**Geometry** (all pre-cached at O(1) unless noted)
- Area (m²), perimeter (m), exterior boundary length, centroid, bounding box
- Subset area, perimeter, bounding box, convex hull, Polsby-Popper compactness
- `boundary_of` and `union_of` for merged boundary/polygon of any unit subset

**Topology**
- Contiguity, connected components, holes, enclaves

**Spatial queries**
- Point lookup and envelope queries via R-tree

**Simplification**
- Topology-preserving Douglas–Peucker: shared boundaries are simplified identically across adjacent units, eliminating gaps

**Serialization**
- Compact binary format via `geograph::io::{read, write}`

## Coordinate system

Input geometries must use unprojected lon/lat (EPSG:4326). Area and length results are returned in **m²** and **m** via a per-edge `cos(φ_mid)` correction applied at construction time.

## Snap tolerance

Pass `snap_tol: Some(tol)` to repair near-coincident shared-boundary vertices before DCEL construction. Pass `None` for topologically clean data (e.g. TIGER/Line GeoParquet) where shared vertices are already bitwise-identical.

| Data source       | Suggested tolerance |
|-------------------|---------------------|
| TIGER/Line GeoParquet | `None`          |
| Other GeoParquet  | `Some(1e-7)`        |
| PMTiles-quantised | `Some(1e-4)`        |

## Performance

| Operation | Complexity |
|---|---|
| `are_adjacent` | O(log deg) |
| `neighbors` | O(deg) |
| Single-unit accessors (`area`, `perimeter`, `centroid`, …) | O(1) |
| `area_of`, `bounds_of`, `exterior_boundary_length_of` | O(k) |
| `perimeter_of`, `compactness_of` | O(k · avg\_deg) |
| `boundary_of`, `union_of` | O(boundary edges) |
| `unit_at` | O(log n) |
| `is_contiguous`, `connected_components` | O(k) |
| `enclaves`, `has_holes` | O(n) |

k = subset size, n = total units.

## Internals

See [`INTERNALS.md`](INTERNALS.md) for details on the DCEL representation, construction pipeline, adjacency algorithms, serialization format, and design decisions.
