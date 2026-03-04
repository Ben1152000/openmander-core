# Geograph — Design Document

## 1. Purpose

`geograph` is a self-contained Rust crate for representing and querying **planar
maps**: complete subdivisions of a geographic region into non-overlapping units.
It exposes a geometry- and topology-only API (no attributes, no optimization) and
is designed as the geometric foundation for redistricting computations in
`openmander`.

---

## 2. Planar Map Model

A **Region** is a finite set of **Units** whose interiors are pairwise disjoint
and whose union equals the region's support polygon.

Both the region and individual units are **MultiPolygons**:

- A unit may be **multiply connected** (has interior holes).
- A unit may consist of **multiple exclaves** (disjoint pieces / multipolygon).
- The region itself may be a multipolygon (e.g. a state with islands).

The region is assumed to have **no gaps** — every point in the region's support
belongs to exactly one unit.

### 2.1 Boundary topology

Two units share a **Rook** adjacency if they share a 1-D boundary segment
(positive-length shared edge).  They share a **Queen** adjacency if they share
at least one point (including corners).

Interior holes (surrounded units not in a query subset) are topologically
distinct from the outer boundary of the region.

---

## 3. Internal Representation

### 3.1 DCEL (Doubly Connected Edge List)

The DCEL is the primary internal data structure.  It encodes the full planar
embedding: which faces are adjacent, what the boundary of each face looks like,
and which vertices are shared.

**Mapping to the planar map:**

| DCEL element | Planar map meaning |
|---|---|
| Bounded face | One polygon ring of a unit (including the exterior unit) |
| Unbounded face (`FaceId(0)`) | One face of the exterior unit |
| Half-edge `h` | One directed side of a shared or exterior boundary segment |
| `h.twin` | The same segment seen from the other side |
| Vertex | A corner point shared by ≥ 2 polygon rings |

Every DCEL face belongs to exactly one unit.  A unit may own multiple faces
(exclaves map to multiple bounded faces; holes in a unit's polygon are faces
belonging to whatever unit fills them).  This applies to the exterior unit too:

```
Exterior unit (UnitId::EXTERIOR)
  ├── Unbounded face (FaceId(0))  → the region outside the state boundary
  ├── Hole face H1                → an interior gap with no unit assigned
  └── Hole face H2                → another gap (e.g. a lake not in the data)

Regular unit U (exclave with hole)
  ├── Outer ring of exclave 1  → DCEL face F1
  ├── Outer ring of exclave 2  → DCEL face F2
  └── Hole in exclave 1        → DCEL face F3, owned by whatever unit fills it
                                  (or UnitId::EXTERIOR if the hole is empty)
```

A `face_to_unit: Vec<UnitId>` table maps every DCEL face (including the
unbounded face) to its unit.  `UnitId::EXTERIOR` is a reserved sentinel; all
other `UnitId`s refer to input units.  `UnitId::EXTERIOR` is never a valid
district assignment.

### 3.2 Adjacency tables

Two separate CSR adjacency matrices are built eagerly during construction and
stored on `Region`:

**Rook adjacency** (shared edge — positive-length boundary):

- Walk every half-edge `h` in the DCEL.
- If `face_to_unit[h.face] != face_to_unit[h.twin.face]`, emit the ordered
  pair `(face_to_unit[h.face], face_to_unit[h.twin.face])`.
- Pairs involving `UnitId::EXTERIOR` are filtered out — the adjacency matrices
  only contain real unit-to-unit edges.  Whether a unit borders the exterior is
  tracked separately via the `is_exterior` flag (see §3.3).
- Collect all such pairs, sort, deduplicate, and build CSR.

**Queen adjacency** (shared point — corner touch counts):

- Walk every DCEL vertex `v`.  For each pair of distinct units `(a, b)` that
  appear in the vertex star of `v` (via `vertex_star()`), emit `(a, b)` and
  `(b, a)`.
- Collect, sort, deduplicate, and build CSR.
- Because every shared edge is also a shared point, Rook ⊆ Queen holds
  automatically.

The two matrices are stored as separate fields:

```
adjacent: AdjacencyMatrix   // Rook
touching: AdjacencyMatrix   // Queen
```

`AdjacencyMatrix` is a plain CSR structure: `offsets: Vec<u32>` (length
`num_units + 1`) and `neighbors: Vec<UnitId>` (sorted within each row for
binary search).  Row `u` gives the sorted list of units adjacent to `u` under
the chosen mode.

### 3.3 Per-unit cache

During construction the following scalar quantities are computed once and
stored in `Vec`s indexed by `UnitId`:

- `area: Vec<f64>` — per-edge weighted shoelace formula, in m².  Each shoelace
  term is scaled by `cos(φ_mid)` where `φ_mid` is the midpoint latitude of
  that edge in radians (see §8, question 4).  Holes are subtracted.
- `perimeter: Vec<f64>` — sum of edge lengths in metres, where each edge
  length is `√(Δlat² + (Δlon·cos(φ_mid))²) × 111_320`.  Holes included.
- `exterior_boundary_length: Vec<f64>` — sum of `edge_length` values for
  half-edges whose twin belongs to `UnitId::EXTERIOR`.  Zero for interior
  units.  In metres.
- `centroid: Vec<Coord<f64>>` — approximate centroid of each unit in lon/lat,
  computed as the unweighted average of all half-edge origin vertices belonging
  to the unit.  This is a vertex-average, not the true area-weighted centroid;
  it is fast to compute and sufficient for angular ordering and display.
- `bounds: Vec<Rect<f64>>` — axis-aligned bounding box of each unit in lon/lat.
- `is_exterior: Vec<bool>` — true if the unit has any half-edge whose twin
  belongs to `UnitId::EXTERIOR` (i.e. the unit touches the region boundary).
- `bounds_all: Rect<f64>` — axis-aligned bounding box of the entire region
  (union of all per-unit bounding boxes).

An R-tree spatial index (`rstar::RTree`) over per-unit bounding boxes is also
built during construction for fast point-location and envelope queries (see
§5.14).

Shared-edge lengths are stored on the half-edge pairs:
`edge_length: Vec<f64>` indexed by `HalfEdgeId / 2` (one entry per undirected
edge), in metres using the same per-edge `cos(φ_mid)` correction.

### 3.4 Edge matching

During construction, shared polygon edges are detected using a `HashMap` keyed
on snapped vertex-pair coordinates.  After vertex snapping (§4, step 1) brings
near-coincident vertices to exact canonical positions, two directed edges from
different units that share the same (origin, dest) vertex pair — in forward or
reverse direction — are identified as the same undirected boundary segment and
twinned in the DCEL.  Edges with no matching reverse edge are twinned with the
outer face.  The map is discarded after the DCEL is built.

---

## 4. Construction Pipeline

Input: a `Vec<MultiPolygon<f64>>` (one per unit, in the order that determines
`UnitId` assignment).

```
1. Vertex snapping
   - Collect all polygon vertices.
   - Snap nearby vertices (within SNAP_TOLERANCE) to a canonical position.
   - This repairs the near-coincident-vertex artefacts common in TIGER/Line
     and PMTiles-quantised data.

2. DCEL construction
   - For each polygon ring of each unit, insert its edges as half-edge pairs.
   - Shared edges (where both endpoints are snapped to the same canonical
     vertices) are identified and their half-edges are twinned.
   - Exterior edges (no twin found) are twinned with a half-edge assigned to
     the outer face.
   - next/prev links are set by sorting outgoing half-edges at each vertex in
     CCW angular order.

3. Gap detection
   - Faces are assigned during edge creation based on ring membership.
   - Walk all cycles on OUTER_FACE.  The cycle with the most negative
     signed area (largest CW polygon) is the true outer boundary; all
     others are interior gap faces assigned to UnitId::EXTERIOR.

4. Cache pre-computation
   - Compute area, perimeter, edge_length, centroid, bounds, bounds_all,
     exterior_boundary_length, and is_exterior for all units and edges.

5. Validation (optional, cfg(debug_assertions))
   - All half-edges have valid twins (twin of twin is self).
   - All half-edge next/prev links are consistent.
   - Every face has at least one half-edge.
   - Unit areas are non-negative.
   Also available as `Region::validate()` for explicit checks after
   deserialisation.
```

**Snapping:** vertex snapping is handled by the `snap` module before DCEL
construction begins (see §8, question 1).  Only vertices connected by a
polygon edge in at least one input unit are candidates for snapping.  The
default tolerance is `1e-7` degrees (~1 cm); callers may pass a larger value
(up to ~`1e-4`, ~10 m) for coarser input such as PMTiles-quantised data.
The tolerance is a construction parameter, not a global constant.

---

## 5. Algorithm Sketches

### 5.1 `are_adjacent(a, b)`

Binary search in `a`'s row of the Rook CSR matrix for `b`.
O(log deg(a)), or O(1) with a hash set per row (trade memory for speed).

### 5.2 `neighbors(unit)`

Read `unit`'s row from the Rook CSR matrix — a sorted slice of `UnitId`s.  O(deg).

### 5.3 `area(units)` and `perimeter(units)`

All values are in metres / metres² via the per-edge `cos(φ_mid)` correction
baked into the cache at construction time (see §3.3).

```
area:      units.iter().map(|u| cached_area[u]).sum()              O(k)

perimeter: Build a set S of units.
           For each unit u in S:
             For each DCEL face f of u:
               For each half-edge h of f:
                 if face_to_unit[h.twin.face] ∉ S:
                   perimeter += edge_length[h]               O(k · avg_degree)
```

The perimeter walk naturally excludes shared internal edges; `edge_length`
values are already in metres so no further scaling is needed at query time.

### 5.4 `boundary(units)`

Same walk as `perimeter` but collect the half-edges whose twins are outside `S`.
Group them into cycles by following `next` links within the boundary.
Return as `MultiLineString`.  O(boundary length in edges).

### 5.5 `shared_boundary_length(a, b)`

Walk half-edges of unit `a`; sum `edge_length` for edges whose twin belongs to
`b`.  O(edges of `a`).

### 5.6 `is_contiguous(units)`

BFS on the Rook adjacency graph restricted to `units` (treat as a
subgraph).  O(k + edges within subgraph).

### 5.7 `connected_components(units)`

Same as `is_contiguous` but collect all components via repeated BFS from
unvisited seeds.  O(k + internal edges).

### 5.8 `has_holes(units)` / `enclaves(units)`

```
1. Let complement = all units NOT in the query set.
2. Find connected components of complement using Rook adjacency.
3. A component is a hole iff it has no unit adjacent to the outer face
   (i.e., no unit on the region's exterior boundary).
```

Exterior units are flagged at construction time: a unit is exterior if any of
its DCEL faces has a half-edge whose twin is in the outer face.

O(n) where n = total number of units.

### 5.9 `compactness(units)`

```
4π · area(units) / perimeter(units)²
```

O(k · avg_deg) — dominated by `perimeter_of` which does a boundary walk.

### 5.10 `centroid(unit)` / `bounds(unit)` / `is_exterior(unit)` / `exterior_boundary_length(unit)` / `bounds_all()`

O(1) — all pre-cached at construction time (see §3.3).

### 5.11 `bounds_of(units)`

Expand a `Rect` over the cached per-unit bounding boxes.  O(k).

### 5.12 `exterior_boundary_length_of(units)`

Sum `exterior_boundary_length` over units in the subset.  O(k).

### 5.13 `union_of(units)`

Walk the DCEL boundary of the subset (same as `boundary_of`), then classify
each cycle by signed area: positive (CCW) = outer ring, negative (CW) = hole.
Match holes to their enclosing outer ring using a point-in-ring test.  Returns
a `MultiPolygon<f64>` representing the merged shape.  O(boundary edges).

No external polygon boolean operations are needed — the DCEL already encodes
the planar subdivision, so the boundary walk extracts the merged shape directly.

### 5.14 `unit_at(point)` / `units_in_envelope(envelope)`

An R-tree over per-unit bounding boxes provides fast spatial queries:

- `unit_at(point)`: query the R-tree for candidate units whose bounding box
  contains the point (O(log n)), then perform exact point-in-polygon tests
  on candidates.  Returns the first match or `None`.
- `units_in_envelope(envelope)`: query the R-tree for all units whose bounding
  box intersects the envelope (O(log n + k)).  This is a coarse filter —
  returned units may not actually intersect geometrically.

### 5.15 `convex_hull(unit)` / `convex_hull_of(units)`

- `convex_hull(unit)`: delegates to `geo::ConvexHull` on the unit's stored
  `MultiPolygon`.  O(v log v) where v is the number of vertices.
- `convex_hull_of(units)`: collects all polygons from the input units and
  computes the convex hull of the combined geometry.  O(V log V) where V is
  the total vertex count across all input units.  This is a naive
  implementation — a future version should merge per-unit hulls incrementally
  (O(k · h) where h is the output hull size).

---

## 6. Public API Summary

```rust
// Construction
Region::new(geometries, snap_tol)       -> Result<Region>
Region::from_geojson(data, snap_tol)    -> Result<Region>       // not yet implemented
Region::from_shapefile(path, snap_tol)  -> Result<Region>       // not yet implemented

// Unit access
region.unit_ids()       -> impl Iterator<Item = UnitId>
region.num_units()      -> usize
region.geometry(unit)   -> &MultiPolygon<f64>

// Adjacency — Rook (shared edge) is the default for all graph queries.
// Raw matrix access exposes both modes for callers that need Queen.
region.are_adjacent(a, b)      -> bool               // Rook (shared edge)
region.neighbors(unit)         -> &[UnitId]          // Rook, sorted
region.adjacency()             -> &AdjacencyMatrix   // Rook CSR
region.touching()              -> &AdjacencyMatrix   // Queen CSR

// Single-unit geometry (O(1), pre-cached unless noted)
region.area(unit)                     -> f64
region.perimeter(unit)                -> f64
region.exterior_boundary_length(unit) -> f64
region.centroid(unit)                 -> Coord<f64>
region.bounds(unit)                   -> Rect<f64>
region.is_exterior(unit)              -> bool
region.boundary(unit)                 -> MultiLineString<f64>
region.convex_hull(unit)              -> Polygon<f64>           // O(v log v)

// Region-wide geometry (O(1), pre-cached)
region.bounds_all()                   -> Rect<f64>

// Subset geometry (O(k) unless noted)
region.area_of(units)                      -> f64
region.perimeter_of(units)                 -> f64
region.exterior_boundary_length_of(units)  -> f64
region.bounds_of(units)                    -> Rect<f64>
region.boundary_of(units)                  -> MultiLineString<f64>
region.compactness_of(units)               -> f64
region.union_of(units)                     -> MultiPolygon<f64>   // O(boundary edges)
region.convex_hull_of(units)               -> Polygon<f64>        // O(V log V)

// Spatial queries
region.unit_at(point)                      -> Option<UnitId>      // O(log n)
region.units_in_envelope(envelope)         -> Vec<UnitId>         // O(log n + k)

// Validation
region.validate()                          -> Result<(), RegionError>

// Edge metrics
region.shared_boundary_length(a, b)               -> f64
region.boundary_length_with(units, other_units)   -> f64

// Topology — Rook adjacency throughout
region.is_contiguous(units)          -> bool
region.connected_components(units)   -> Vec<Vec<UnitId>>
region.has_holes(units)              -> bool
region.enclaves(units)               -> Vec<Vec<UnitId>>
```

**Supporting types:**

```rust
/// A read-only CSR adjacency matrix.
pub struct AdjacencyMatrix {
    offsets:   Vec<u32>,   // length num_units + 1
    neighbors: Vec<UnitId>,   // sorted within each row
}

impl AdjacencyMatrix {
    pub fn num_units(&self) -> usize;
    pub fn neighbors(&self, unit: UnitId) -> &[UnitId];   // sorted slice
    pub fn contains(&self, unit: UnitId, other: UnitId) -> bool; // binary search
}
```

`units` parameters accept `&[UnitId]` or any `IntoIterator<Item = UnitId>`.

---

## 7. Performance Summary

| Operation | Complexity | Notes |
|---|---|---|
| `are_adjacent` | O(log deg) | Rook; O(1) with hash adjacency |
| `neighbors` | O(deg) | Rook; slice into CSR row |
| `area(unit)` | O(1) | cached |
| `perimeter(unit)` | O(1) | cached |
| `exterior_boundary_length(unit)` | O(1) | cached |
| `centroid(unit)` | O(1) | cached |
| `bounds(unit)` | O(1) | cached |
| `is_exterior(unit)` | O(1) | cached |
| `bounds_all()` | O(1) | cached |
| `area_of(units)` | O(k) | sum of cached values |
| `perimeter_of(units)` | O(k · avg_deg) | boundary walk |
| `exterior_boundary_length_of(units)` | O(k) | sum of cached values |
| `bounds_of(units)` | O(k) | expand cached bounding boxes |
| `boundary_of(units)` | O(boundary edges) | half-edge walk |
| `union_of(units)` | O(boundary edges) | DCEL boundary walk |
| `compactness_of(units)` | O(k · avg_deg) | via perimeter_of |
| `convex_hull(unit)` | O(v log v) | v = vertices of unit |
| `convex_hull_of(units)` | O(V log V) | V = total vertices; naive impl |
| `unit_at(point)` | O(log n) | R-tree + point-in-polygon |
| `units_in_envelope(envelope)` | O(log n + k) | R-tree bounding-box filter |
| `shared_boundary_length` | O(edges of a) | half-edge walk |
| `is_contiguous` | O(k) | BFS on subgraph |
| `connected_components` | O(k) | BFS on subgraph |
| `has_holes` / `enclaves` | O(n) | complement BFS |
| Construction | O(n log n) | dominated by CCW sort |
| Adjacency matrix build | O(n · avg_deg) | one half-edge pass |

k = size of query subset, n = total number of units.

---

## 8. Notes &amp; Open Questions

1. **Snapping strategy.** *(Resolved)* Use the conservative approach: only snap
   pairs of vertices that are already connected by a polygon edge in at least
   one input unit (i.e. snap along shared boundaries only, never across open
   space).  This avoids merging legitimately distinct vertices in dense areas.

   The snapping and vertex-repair logic lives in a dedicated `snap` module
   (separate from DCEL construction) with a single entry-point function:

   ```rust
   // snap.rs (pub(crate) — internal to the geograph crate)
   pub(crate) fn snap_vertices(
       rings: &mut [Vec<Vec<Coord<f64>>>],  // all polygon rings, mutated in place
       tolerance: f64,
   )
   ```

   Keeping it isolated makes it straightforward to swap the algorithm (e.g.
   switch to a different tolerance heuristic or a topology-aware repair pass)
   without touching the DCEL builder.

2. **Multipolygon hole ownership.** *(Resolved)* The exterior of the region is
   treated as a first-class unit (`UnitId::EXTERIOR`).  It owns the unbounded
   DCEL face and any interior gaps (holes in the region with no assigned unit).
   Since a regular unit can already own multiple faces (exclaves), no new
   machinery is needed — `UnitId::EXTERIOR` simply participates in
   `face_to_unit` like any other unit.  It is excluded from district
   assignment and does not appear in the adjacency matrices (filtered out
   during construction).  Whether a unit borders the exterior is tracked
   separately via the `is_exterior` flag.

3. **Queen adjacency at T-junctions.** *(Resolved)* When building the touching
   (Queen) matrix, emit all pairs `(a, b)` for every distinct combination of
   units appearing in each vertex star — not just consecutive pairs.  This
   correctly captures diagonal corner touches (e.g. two units that meet at a
   single point but share no edge) without any special-casing.

4. **Coordinate system.** *(Resolved)* Input coordinates are unprojected
   lon/lat (degrees).  Area and length results are returned in **m²** and **m**
   respectively via a per-edge `cos(φ_mid)` correction applied at construction
   time:

   - **Edge length:** `√(Δlat² + (Δlon·cos(φ_mid))²) × 111_320 m/°`
   - **Area (shoelace term):** `(lon_i·lat_{i+1} − lon_{i+1}·lat_i) · cos(φ_mid) × 111_320² m²/°²`

   where `φ_mid = (lat_start + lat_end) / 2` in radians for each edge.
   Evaluating `cos` at the midpoint of every short polygon edge is equivalent
   to a first-order numerical integration of the spherical area element
   `R² cos(φ) dφ dλ`, and keeps error well under 1% for all real-world inputs.

5. **Parallelism.** *(Out of scope)* The construction pipeline is
   single-threaded. Parallelism would add complexity that isn't warranted at
   this stage.

6. **Serialisation.** *(Resolved)* A custom binary format with no external
   dependencies.  The file is a fixed header followed by flat, fixed-width
   sections — trivial to read or write in any language with a seek and a
   bulk copy.

   **Compaction techniques:**
   - Coordinates stored as `i32` (scaled by 10^7, i.e. 1e-7° precision)
     rather than `f64` — lossless at the snap tolerance, 50% smaller.
   - `twin` field omitted from half-edge records; derived as `id ^ 1` since
     half-edges are always stored in consecutive pairs.
   - No external compression (avoids WASM-incompatible native libraries).

   **File layout:**

   ```
   [Header]
     magic:          4 bytes  ("OMRP")
     version:        1 byte
     reserved:       3 bytes
     num_vertices:   u32
     num_half_edges: u32      (always even)
     num_faces:      u32
     num_units:      u32

   [Vertices]         num_vertices  × (i32 lon, i32 lat)            8 B each
   [HalfEdges]        num_half_edges × (u32 origin, u32 next,
                                        u32 prev,   u32 face)       16 B each
   [Faces]            num_faces     × u32 half_edge                  4 B each
                        (0xFFFFFFFF = no boundary / outer face)
   [FaceToUnit]       num_faces     × u32                            4 B each
                        (0xFFFFFFFF = UnitId::EXTERIOR)
   [UnitCache]        num_units     × (f64 area_m2, f64 perimeter_m) 16 B each
   [EdgeLengths]      (num_half_edges/2) × f64                       8 B each
   [RookAdj offsets]  (num_units+1) × u32                            4 B each
   [RookAdj neighbors] num_rook_edges × u32                          4 B each
   [TouchingAdj offsets]  (num_units+1) × u32                        4 B each
   [TouchingAdj neighbors] num_touching_edges × u32                  4 B each
   ```

   **Persisted vs. derived fields:** Only `area` and `perimeter` are stored
   in the UnitCache section.  The remaining cache fields (`exterior_boundary_length`,
   `centroid`, `bounds`, `bounds_all`, `is_exterior`) and the `geometries` vector
   are re-derived from the DCEL on deserialisation.

   **Known limitation:** Geometry reconstruction on deserialisation creates one
   `Polygon` per DCEL face.  Units with holes will have the hole boundary as a
   separate outer ring rather than as an interior ring of the enclosing polygon.
   This only affects `region.geometry(unit)` after a round-trip; all other queries
   (area, perimeter, boundary, adjacency, etc.) use the DCEL directly and are
   unaffected.

   **Potential future improvement:** store vertex coordinates as deltas
   relative to a per-section reference origin to reduce magnitude and
   improve compressibility if compression is added later.

7. **Convex hull queries.** *(Resolved)* `convex_hull(unit)` delegates to
   `geo::ConvexHull` on the stored `MultiPolygon`.  `convex_hull_of(units)`
   collects all polygons and computes the hull of the combined geometry.
   The subset version is O(V log V) where V is the total vertex count —
   a future improvement should merge per-unit hulls incrementally.
