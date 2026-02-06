#!/usr/bin/env python3
"""Test the district boundary extraction."""

import time

def log(msg):
    print(msg, flush=True)

log("Importing openmander...")
import openmander as om
log("Import complete.")

PACK_PATH = "/Users/benjamin/Programming/projects/openmander/openmander-app/public/packs/IL_2020_webpack"

log(f"Loading map from pack: {PACK_PATH}")
start = time.time()
map = om.Map.from_pack(PACK_PATH)
log(f"Map loaded in {time.time() - start:.2f}s")

log(f"Creating plan with 4 districts...")
plan = om.Plan(map, 4)
log("Plan created.")

log("Randomizing plan...")
start = time.time()
plan.randomize()
log(f"Randomized in {time.time() - start:.2f}s")

log("\nCalling district_geometries_wkb()...")
start = time.time()
try:
    geometries = plan.district_geometries_wkb()
    elapsed = time.time() - start
    log(f"Completed in {elapsed:.2f}s")

    log(f"\nResults ({len(geometries)} districts):")
    for district_id, wkb_bytes in geometries:
        log(f"  District {district_id}: {len(wkb_bytes):,} bytes")

    # Validate WKB with shapely
    try:
        import shapely.wkb
        import shapely.validation
        from shapely import make_valid, unary_union
        log("\nValidating WKB with shapely...")
        for district_id, wkb_bytes in geometries:
            geom = shapely.wkb.loads(wkb_bytes)
            num_polys = len(geom.geoms) if hasattr(geom, 'geoms') else 1
            is_valid = geom.is_valid
            log(f"  District {district_id}: {geom.geom_type} with {num_polys} polygon(s), valid={is_valid}")
            if not is_valid:
                reason = shapely.validation.explain_validity(geom)
                log(f"    Reason: {reason}")
            if not geom.is_empty:
                bounds = geom.bounds
                log(f"    Bounds: ({bounds[0]:.4f}, {bounds[1]:.4f}) to ({bounds[2]:.4f}, {bounds[3]:.4f})")

        log("\nTrying shapely make_valid + unary_union...")
        for district_id, wkb_bytes in geometries:
            geom = shapely.wkb.loads(wkb_bytes)
            valid_geom = make_valid(geom)
            unified = unary_union(valid_geom)
            num_polys = len(unified.geoms) if hasattr(unified, 'geoms') else 1
            log(f"  District {district_id}: {unified.geom_type} with {num_polys} polygon(s), valid={unified.is_valid}")
    except ImportError:
        log("\n(shapely not installed, skipping WKB validation)")

except Exception as e:
    elapsed = time.time() - start
    log(f"ERROR after {elapsed:.2f}s: {e}")
    import traceback
    traceback.print_exc()

log("\nDone!")
