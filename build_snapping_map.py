#!/usr/bin/env python3
"""
build_snapping_map.py

Creates a lookup table mapping Original Node Coordinates -> Nearest Wet GloFAS Pixel.
Does NOT modify the original nodes file.

Input:  graph/nodes.csv
Output: graph/discharge_point_map.csv
"""

import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
import os

# --- CONFIG ---
NODES_CSV = "graph/nodes.csv"
OUTPUT_MAP = "graph/discharge_point_map.csv"
GLOFAS_DIR = "bulk/glofas_monthly"

# Use a wet month to find the river channel
REFERENCE_FILE_PATTERN = "*_07.nc" 
SEARCH_RADIUS_DEG = 0.15  # ~15km search box

def get_reference_grid():
    files = sorted(Path(GLOFAS_DIR).glob(REFERENCE_FILE_PATTERN))
    if not files:
        files = sorted(Path(GLOFAS_DIR).glob("*.nc"))
    
    if not files:
        raise FileNotFoundError(f"No .nc files found in {GLOFAS_DIR}")
    
    ref_file = files[0]
    print(f"🌊 Using reference file: {ref_file.name}")
    
    ds = xr.open_dataset(ref_file)
    
    # Dynamic Variable Detection
    var_name = None
    for v in ['river_discharge_in_the_last_24_hours', 'dis24', 'dis']:
        if v in ds.data_vars:
            var_name = v
            break
            
    # Mean over time to get static map
    river_map = ds[var_name].mean(dim='valid_time' if 'valid_time' in ds.coords else 'time')
    return river_map, ds

def main():
    print(f"Reading {NODES_CSV}...")
    nodes = pd.read_csv(NODES_CSV)
    
    river_map, ds_handle = get_reference_grid()
    
    mapping_data = []
    
    print(f"Mapping {len(nodes)} nodes to nearest high-flow pixel...")
    
    for idx, row in nodes.iterrows():
        orig_lat = row['lat']
        orig_lon = row['lon']
        nid = row['node_id']
        
        # Default to original (if no better point found)
        best_lat, best_lon = orig_lat, orig_lon
        
        # Define Search Window
        min_lat, max_lat = orig_lat - SEARCH_RADIUS_DEG, orig_lat + SEARCH_RADIUS_DEG
        min_lon, max_lon = orig_lon - SEARCH_RADIUS_DEG, orig_lon + SEARCH_RADIUS_DEG
        
        # Handle coordinate naming
        lat_name = 'latitude' if 'latitude' in river_map.coords else 'lat'
        lon_name = 'longitude' if 'longitude' in river_map.coords else 'lon'
        
        # Slice grid
        local_grid = river_map.sel(
            {lat_name: slice(min_lat, max_lat), lon_name: slice(min_lon, max_lon)}
        )
        if local_grid.size == 0:
             local_grid = river_map.sel(
                {lat_name: slice(max_lat, min_lat), lon_name: slice(min_lon, max_lon)}
            )

        # Find Max Flow Pixel
        if local_grid.size > 0:
            max_val = local_grid.max()
            if max_val > 0.1: # If area is not completely dry
                max_loc = local_grid.where(local_grid == max_val, drop=True)
                best_lat = float(max_loc[lat_name].values[0])
                best_lon = float(max_loc[lon_name].values[0])

        mapping_data.append({
            'node_id': nid,
            'orig_lat': orig_lat,
            'orig_lon': orig_lon,
            'sample_lat': best_lat,
            'sample_lon': best_lon
        })

    ds_handle.close()
    
    # Save Map
    map_df = pd.DataFrame(mapping_data)
    map_df.to_csv(OUTPUT_MAP, index=False)
    
    # Calc stats
    diffs = np.sqrt((map_df['orig_lat'] - map_df['sample_lat'])**2 + 
                    (map_df['orig_lon'] - map_df['sample_lon'])**2) * 111
    
    print(f"\n Mapping Complete.")
    print(f"   Nodes shifted: {(diffs > 0).sum()}")
    print(f"   Saved to: {OUTPUT_MAP}")

if __name__ == "__main__":
    main()