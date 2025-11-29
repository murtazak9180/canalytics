#!/usr/bin/env python3
"""
process_discharge_mapped_standalone.py

1. Loads 'graph/discharge_point_map.csv'.
2. Extracts discharge from NetCDF files using the 'sample_lat/lon' (wet pixels).
3. Maps data back to original 'node_id'.
4. Aggregates to Weekly Average.
5. Saves to 'data/discharge/discharge_weekly_complete.csv'.
"""

import os
import pandas as pd
import xarray as xr
from pathlib import Path
from tqdm import tqdm

# --- CONFIGURATION ---
BASE_DIR = os.getcwd()
MAP_CSV = os.path.join(BASE_DIR, "graph", "discharge_point_map.csv")
GLOFAS_DIR = os.path.join(BASE_DIR, "bulk", "glofas_monthly")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "discharge")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "discharge_weekly_complete.csv")

def main():
    # 1. Setup
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"1. Loading Snapping Map from {MAP_CSV}...")
    if not os.path.exists(MAP_CSV):
        print(f" Error: Map file not found. Run 'build_snapping_map.py' first.")
        return
        
    lookup_df = pd.read_csv(MAP_CSV)
    
    # We extract based on the SAMPLE coordinates (where the water is)
    # But we map it to the NODE_ID (where the graph node is)
    node_ids = lookup_df['node_id'].values
    target_lats = xr.DataArray(lookup_df['sample_lat'].values, dims="node_idx")
    target_lons = xr.DataArray(lookup_df['sample_lon'].values, dims="node_idx")
    
    print(f"   Targeting {len(node_ids)} nodes (using snapped coordinates).")

    # 2. Find Files
    files = sorted(Path(GLOFAS_DIR).glob("*.nc"))
    if not files:
        print(f" No .nc files found in {GLOFAS_DIR}")
        return

    print(f"2. Processing {len(files)} files sequentially...")
    
    daily_records = []

    # 3. Iterate (Safe Loop)
    for f in tqdm(files, desc="Extracting"):
        try:
            ds = xr.open_dataset(f)
            
            # --- Coordinates Check ---
            if 'latitude' in ds.coords: lat_name = 'latitude'
            elif 'lat' in ds.coords: lat_name = 'lat'
            else: ds.close(); continue

            if 'longitude' in ds.coords: lon_name = 'longitude'
            elif 'lon' in ds.coords: lon_name = 'lon'
            else: ds.close(); continue

            if 'time' in ds.coords: time_name = 'time'
            elif 'valid_time' in ds.coords: time_name = 'valid_time'
            else: ds.close(); continue

            # --- Variable Check ---
            var_name = None
            for v in ['river_discharge_in_the_last_24_hours', 'dis24', 'dis']:
                if v in ds.data_vars:
                    var_name = v
                    break
            
            if not var_name:
                ds.close(); continue

            # --- Extraction (Using Snapped Coords) ---
            query = {lat_name: target_lats, lon_name: target_lons}
            sampled = ds[var_name].sel(**query, method="nearest")
            
            # Convert to Pandas
            df_chunk = sampled.to_dataframe().reset_index()
            
            # Normalize
            df_chunk.rename(columns={
                var_name: 'discharge_avg',
                time_name: 'time'
            }, inplace=True)
            
            # CRITICAL: Map 'node_idx' 0,1,2... back to the real 'node_id' strings
            df_chunk['node_id'] = df_chunk['node_idx'].map(lambda i: node_ids[i])
            
            # Keep essential data
            df_chunk = df_chunk[['node_id', 'time', 'discharge_avg']]
            
            daily_records.append(df_chunk)
            ds.close()
            
        except Exception as e:
            print(f"   Error reading {f.name}: {e}")
            continue

    if not daily_records:
        print(" Critical: No data extracted.")
        return

    # 4. Aggregation
    print("3. Aggregating and Resampling to Weekly...")
    full_daily_df = pd.concat(daily_records, ignore_index=True)
    
    full_daily_df['date'] = pd.to_datetime(full_daily_df['time'])
    full_daily_df.set_index('date', inplace=True)
    
    # Resample: Group by Node -> Weekly Mean
    weekly_df = full_daily_df.groupby('node_id')['discharge_avg'].resample('W').mean().reset_index()
    
    # Add Time Metadata
    weekly_df['year'] = weekly_df['date'].dt.year
    weekly_df['week'] = weekly_df['date'].dt.isocalendar().week
    
    # Final Sort
    weekly_df.sort_values(['node_id', 'date'], inplace=True)
    
    # 5. Save
    print(f"4. Saving to {OUTPUT_FILE}...")
    weekly_df.to_csv(OUTPUT_FILE, index=False)
    print(f" Success! Saved {len(weekly_df)} rows.")
    print(weekly_df.head())

if __name__ == "__main__":
    main()