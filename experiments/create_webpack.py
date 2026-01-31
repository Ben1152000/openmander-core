#!/usr/bin/env python3
"""
Test script to convert a pack from parquet to PMTiles format.

PMTiles are for DISPLAY ONLY in the web app, not for data storage.
This script only tests parquet -> PMTiles conversion (one-way).

Usage:
    python test_pmtiles.py [state_code]

Example:
    python test_pmtiles.py CT
"""

import sys
from pathlib import Path

# Add the bindings to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "bindings" / "python"))

try:
    import openmander
except ImportError:
    print("Error: Could not import openmander. Make sure the Python bindings are built.")
    print("Run: cd bindings/python && maturin develop")
    sys.exit(1)


def main():
    state_code = sys.argv[1] if len(sys.argv) > 1 else "CT"
    state_code = state_code.upper()
    
    print(f"Testing PMTiles conversion for {state_code}")
    print("=" * 60)
    
    # Find the original pack (parquet format)
    experiments_dir = Path(__file__).parent
    packs_dir = experiments_dir / "packs"
    original_pack_dir = packs_dir / f"{state_code}_2020_pack"
    
    if not original_pack_dir.exists():
        print(f"Error: Pack not found at {original_pack_dir}")
        print(f"Available packs: {[d.name for d in packs_dir.iterdir() if d.is_dir() and d.name.endswith('_pack')]}")
        sys.exit(1)
    
    print(f"Original pack (parquet): {original_pack_dir}")
    
    # Output directory for PMTiles pack (using webpack naming convention)
    pmtiles_pack_dir = packs_dir / f"{state_code}_2020_webpack"
    
    # Test: Convert parquet -> PMTiles
    print("\n[Test] Converting parquet -> PMTiles...")
    
    try:
        # Read original (parquet)
        map_original = openmander.Map(str(original_pack_dir))
        print(f"  ✓ Read original pack (parquet format)")
        
        # Write as PMTiles
        pmtiles_pack_dir.mkdir(parents=True, exist_ok=True)
        map_original.to_pack(str(pmtiles_pack_dir), format="pmtiles")
        print(f"  ✓ Wrote PMTiles pack to {pmtiles_pack_dir}")
        
        # Verify PMTiles pack can be loaded (basic check)
        map_pmtiles = openmander.Map.from_pack(str(pmtiles_pack_dir), format="pmtiles")
        print(f"  ✓ PMTiles pack can be loaded")
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✅ Test passed!")
    print(f"\nSummary:")
    print(f"  - Original pack (parquet): {original_pack_dir}")
    print(f"  - PMTiles webpack pack written to: {pmtiles_pack_dir}")
    print(f"  - Successfully converted parquet -> PMTiles")
    print(f"\nNote:")
    print(f"  - PMTiles include 256px buffer for seamless tile rendering")
    print(f"  - PMTiles are for DISPLAY ONLY (web app)")
    print(f"  - Use parquet format for data storage and analysis")
    print(f"  - The webpack pack ({state_code}_2020_webpack) contains:")
    print(f"    - CSV data files (for WASM compatibility)")
    print(f"    - PMTiles geometry files (for efficient web display)")
    print(f"    - WKB hull files (for convex hull operations)")
    print(f"    - CSR adjacency files (for graph operations)")



if __name__ == "__main__":
    main()
