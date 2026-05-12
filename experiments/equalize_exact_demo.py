#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo: load a plan CSV, show deviations, run exact equalization, show result.
Writes per-district CSVs (before/after) and a summary CSV with run-level
metrics (initial/final max deviation, blocks moved, fallback edges, runtime).
"""
import copy
import csv
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import openmander as om

SCRIPT_DIR  = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPT_DIR.parent.parent
PACK_PATH   = ROOT_DIR / "openmander-data" / "packs" / "IL" / "IL_2020_pack"
PLAN_PATH   = ROOT_DIR / "plans" / "il-plan-fair-3.csv"
OUT_DIR     = SCRIPT_DIR / "artifacts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PLAN = "il_fair_3"  # change to e.g. "il_fair_3" as needed

SERIES = "T_20_CENS_Total"

SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)


# -- Helpers -------------------------------------------------------------------

def district_deviations(plan: om.Plan) -> tuple[list[float], float, float]:
    """Return (deviations, target, max_abs_deviation) for each district."""
    pops = plan.district_totals(SERIES)
    T = sum(pops) / len(pops)
    devs = [p - T for p in pops]
    max_dev = max(abs(d) for d in devs)
    return devs, T, max_dev


def print_deviations(plan: om.Plan, label: str, csv_path: Path | None = None) -> float:
    """Print per-district population table.  Returns max |deviation|."""
    pops = plan.district_totals(SERIES)
    N = len(pops)
    P = sum(pops)
    T = P / N
    print(f"\n{'─'*52}")
    print(f"  {label}")
    print(f"  Total pop: {int(P):,}   Target: {T:,.2f}   N={N}")
    print(f"{'─'*52}")
    print(f"  {'District':>10}  {'Population':>12}  {'Deviation':>10}")
    print(f"  {'─'*10}  {'─'*12}  {'─'*10}")
    rows = []
    for i, pop in enumerate(pops, start=1):
        dev = pop - T
        print(f"  {i:>10}  {int(pop):>12,}  {dev:>+10.1f}")
        rows.append((i, int(pop), round(dev, 1)))
    max_dev = max(abs(p - T) for p in pops)
    print(f"  {'─'*10}  {'─'*12}  {'─'*10}")
    print(f"  Max |deviation|: {max_dev:.1f}")
    if csv_path is not None:
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["district", "population", "deviation"])
            writer.writerows(rows)
        print(f"  CSV saved to {csv_path.name}")
    return max_dev


def _dist_paths(svg_path: Path) -> list[ET.Element]:
    """Return all <path class="dist"> elements from an SVG file."""
    root = ET.parse(svg_path).getroot()
    return [el for el in root.iter(f"{{{SVG_NS}}}path")
            if el.get("class") == "dist"]


def _recolor(el: ET.Element, fill: str, opacity: float = 0.9) -> ET.Element:
    """Deep-copy a path element, replacing its fill color."""
    el = copy.deepcopy(el)
    style = el.get("style", "")
    style = re.sub(r"fill:[^;]+", f"fill:{fill}", style)
    style = re.sub(r"fill-opacity:[^;]+", f"fill-opacity:{opacity}", style)
    style = re.sub(r"stroke-width:[^;]+", "stroke-width:1.5", style)
    el.set("style", style)
    return el


def make_overlay_svg(
    before_svg: Path,
    red_svg: Path | None,
    blue_svg: Path | None,
    out_svg: Path,
) -> None:
    """
    Build a composite SVG:
      - All district fills from before_svg rendered in gray.
      - Moved blocks from red_svg overlaid in red.
      - Moved blocks from blue_svg overlaid in blue.
    """
    tree = ET.parse(before_svg)
    root = tree.getroot()

    # Gray all district fills.
    for el in root.iter(f"{{{SVG_NS}}}path"):
        if el.get("class") == "dist":
            style = el.get("style", "")
            style = re.sub(r"fill:[^;]+", "fill:#9ca3af", style)
            style = re.sub(r"fill-opacity:[^;]+", "fill-opacity:0.5", style)
            el.set("style", style)

    # Overlay moved blocks.
    if red_svg is not None:
        for path in _dist_paths(red_svg):
            root.append(_recolor(path, "#dc2626"))  # red-600
    if blue_svg is not None:
        for path in _dist_paths(blue_svg):
            root.append(_recolor(path, "#2563eb"))  # blue-600

    tree.write(str(out_svg), xml_declaration=True, encoding="unicode")
    print(f"Saved overlay SVG to {out_svg.name}")


# -- Load map & plan -----------------------------------------------------------
print(f"Loading map from {PACK_PATH}")
mp = om.Map(str(PACK_PATH))

print(f"Loading plan from {PLAN_PATH}")
plan = om.Plan(mp, num_districts=17)
with PLAN_PATH.open() as f:
    reader = csv.DictReader(f)
    plan.set_assignments({row["GEOID20"]: int(row["District"]) for row in reader})

# -- Before --------------------------------------------------------------------
before_pops = plan.district_totals(SERIES)   # districts 1..N
T_before = sum(before_pops) / len(before_pops)
# Surplus districts: above average population (will export blocks).
surplus_districts = {i + 1 for i, p in enumerate(before_pops) if p > T_before}

max_dev_before = print_deviations(plan, "BEFORE exact equalization",
                                  OUT_DIR / f"{PLAN}_before.csv")

print(f"\nSaving before SVG ...")
plan.to_svg(str(OUT_DIR / f"{PLAN}_before.svg"))

# -- Equalize ------------------------------------------------------------------
assignments_before = plan.assignments()

print("\nRunning exact equalization ...")
t0 = time.perf_counter()
blocks_moved, fallback_edges = plan.equalize_exact(SERIES)
runtime_s = time.perf_counter() - t0
print(f"Done — {blocks_moved} census block(s) moved, "
      f"{fallback_edges} fallback edge(s), {runtime_s:.1f}s elapsed.")

assignments_after = plan.assignments()

# -- After ---------------------------------------------------------------------
max_dev_after = print_deviations(plan, "AFTER exact equalization",
                                 OUT_DIR / f"{PLAN}_after.csv")

print(f"\nSaving after SVG ...")
plan.to_svg(str(OUT_DIR / f"{PLAN}_after.svg"))

# -- Summary CSV ---------------------------------------------------------------
summary_path = OUT_DIR / f"{PLAN}_summary.csv"
with summary_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "plan",
        "max_dev_before",
        "max_dev_after",
        "blocks_moved",
        "fallback_edges",
        "runtime_s",
    ])
    writer.writerow([
        PLAN,
        round(max_dev_before, 1),
        round(max_dev_after, 1),
        blocks_moved,
        fallback_edges,
        round(runtime_s, 2),
    ])
print(f"Summary CSV saved to {summary_path.name}")

# -- Moved-blocks map ----------------------------------------------------------
moved_geoids = {
    gid for gid, d in assignments_before.items()
    if assignments_after.get(gid, d) != d
}
print(f"\nBuilding moved-blocks map ({len(moved_geoids)} block(s) changed district) ...")

moved_plan = om.Plan(mp, num_districts=1)
moved_plan.set_assignments({gid: 1 for gid in moved_geoids})

print("Saving moved-blocks SVG ...")
moved_plan.to_svg(str(OUT_DIR / f"{PLAN}_moved.svg"))

# -- Overlay: gray map + red/blue moved blocks ---------------------------------
# Red  = block came from a surplus district (above-average pop -> was exporting).
# Blue = block came from a deficit district (below-average pop -> counter-swap).
moved_red  = {gid for gid in moved_geoids if assignments_before[gid] in surplus_districts}
moved_blue = moved_geoids - moved_red

print(f"\nBuilding overlay SVG ({len(moved_red)} red, {len(moved_blue)} blue) ...")

tmp_red  = OUT_DIR / "_tmp_red.svg"
tmp_blue = OUT_DIR / "_tmp_blue.svg"

if moved_red:
    p = om.Plan(mp, num_districts=1)
    p.set_assignments({gid: 1 for gid in moved_red})
    p.to_svg(str(tmp_red))
else:
    tmp_red = None

if moved_blue:
    p = om.Plan(mp, num_districts=1)
    p.set_assignments({gid: 1 for gid in moved_blue})
    p.to_svg(str(tmp_blue))
else:
    tmp_blue = None

make_overlay_svg(
    OUT_DIR / f"{PLAN}_before.svg",
    tmp_red,
    tmp_blue,
    OUT_DIR / f"{PLAN}_overlay.svg",
)

# Clean up temp files.
for f in [OUT_DIR / "_tmp_red.svg", OUT_DIR / "_tmp_blue.svg"]:
    if f.exists():
        f.unlink()

print(f"\nAll outputs written to {OUT_DIR}/")
