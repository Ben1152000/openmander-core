"""
water_blocks.py

Generates a plan for Illinois where district 1 contains exactly the blocks
whose entire area is water (land_m2 == 0). All other blocks are unassigned (0).
"""

import openmander as om
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
PACK_PATH = ROOT_DIR / "openmander-data" / "packs" / "IL" / "IL_2020_pack"

mp = om.Map(str(PACK_PATH))
plan = om.Plan(mp, num_districts=1)

# district_totals gives the sum of a series per district, but we need
# per-block values. Use set_assignments with a dict built from assignments()
# (which gives all geo_ids) filtered by land_m2 == 0.
#
# plan.series() lists available series; "land_m2" is the Census ALAND20 value
# in square metres — zero means the block is entirely water.

# Get all current assignments (all 0 = unassigned after construction).
assignments = plan.assignments()  # {geo_id: district}

# district_totals isn't per-block, so we query block-level land area via the
# node weights exposed through district_totals with a temporary per-block plan.
# Simpler: assign every block to district 1 temporarily, then use the totals
# approach isn't available per-block directly. Instead, use set_assignments to
# assign all blocks to district 1, get totals, reset — but that doesn't give
# per-block values either.
#
# The cleanest approach: assign each block individually, check if land_m2 == 0.
# We do this by assigning one block at a time and reading district_totals — but
# that's O(n^2). Instead, read the underlying CSV directly since the pack is
# already on disk and the column is plainly accessible.

def load_land_m2(pack_path: Path) -> dict[str, float]:
    """Read land_m2 per block geo_id from the block Parquet file using pandas."""
    try:
        import pandas as pd
        df = pd.read_parquet(pack_path / "data" / "block.parquet", columns=["geo_id", "land_m2"])
        return dict(zip(df["geo_id"], df["land_m2"]))
    except ImportError:
        pass
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(pack_path / "data" / "block.parquet", columns=["geo_id", "land_m2"])
        return dict(zip(table.column("geo_id").to_pylist(), table.column("land_m2").to_pylist()))
    except ImportError:
        pass
    raise ImportError("Install pandas or pyarrow to read Parquet files: pip install pandas pyarrow")

land_m2 = load_land_m2(PACK_PATH)

water_only = {geo_id: 1 for geo_id, area in land_m2.items() if area == 0.0}
print(f"Total blocks:       {len(land_m2)}")
print(f"Water-only blocks:  {len(water_only)}")

plan.set_assignments(water_only)

out_path = Path(__file__).parent / "artifacts" / "IL_water_blocks.csv"
out_path.parent.mkdir(exist_ok=True)
plan.to_csv(str(out_path))
print(f"Saved to {out_path}")
