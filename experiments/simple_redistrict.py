import openmander as om
from pathlib import Path

# Directory containing this script
ROOT_DIR = Path.cwd().parent

# Build paths relative to the root
SVG_PATH  = ROOT_DIR / "openmander-core" / "experiments" / "images"

state = 'TX'
num_districts = 38

# pack_path = om.build_pack(state, path=str(BASE_PATH), verbose=1)
pack_path = ROOT_DIR / "openmander-data" / "packs" / state / (state + '_2020_pack')

map = om.Map(str(pack_path))

plan = om.Plan(map, num_districts)

plan.randomize()
plan.equalize(series="T_20_CENS_Total", tolerance=0.0001, max_iter=1000)

plan.to_csv("/Users/benjamin/Desktop/test.csv")
