#!/usr/bin/env python3
"""
build_final_dataset.py

Aggregates the CLEAN data into the Master GNN Dataset.

Inputs:
 1. Discharge:     'data/discharge/discharge_weekly_complete.csv'
 2. Precipitation: 'data/precepitation/precipitation_openmeteo_complete.csv'
 3. Elevation:     'data/elevation/nodes_static_features.csv' (or similar csv in that dir)

Output:
 - graph/nodes_features.csv
"""

import os
import pandas as pd
import glob

# --- CONFIG ---
BASE_DIR = os.getcwd()

# 1. Define Paths
# Using glob to find the CSVs if filenames vary slightly, otherwise assuming standard names
DISCHARGE_DIR = os.path.join(BASE_DIR, "data", "discharge")
PRECIP_DIR = os.path.join(BASE_DIR, "data", "precipitation") 
ELEV_DIR = os.path.join(BASE_DIR, "data", "elevation")
OUTPUT_FILE = os.path.join(BASE_DIR, "graph", "nodes_features.csv")

def find_csv(directory, pattern="*.csv"):
    """Helper to find the first CSV in a directory."""
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        raise FileNotFoundError(f"No CSV found in {directory}")
    return files[0]

def main():
    print("--- Building Final Graph Dataset ---")
    
    # ---------------------------------------------------------
    # 1. LOAD DISCHARGE
    # Expected: node_id, date, discharge_avg, year, week
    # ---------------------------------------------------------
    discharge_file = find_csv(DISCHARGE_DIR)
    print(f"1. Loading Discharge from {discharge_file}...")
    discharge = pd.read_csv(discharge_file)
    
    # Ensure Data Types
    discharge['date'] = pd.to_datetime(discharge['date'])
    # Ensure year/week are integers if they exist
    if 'year' in discharge.columns: discharge['year'] = discharge['year'].astype(int)
    if 'week' in discharge.columns: discharge['week'] = discharge['week'].astype(int)

    # ---------------------------------------------------------
    # 2. LOAD PRECIPITATION
    # Expected: node_id, date, week_of_year, month, rainfall_sum_mm
    # ---------------------------------------------------------
    precip_file = find_csv(PRECIP_DIR)
    print(f"2. Loading Precipitation from {precip_file}...")
    precip = pd.read_csv(precip_file)
    
    precip['date'] = pd.to_datetime(precip['date'])
    
    # Standardize Columns for Merge
    # We need 'year' and 'week' to match Discharge
    if 'year' not in precip.columns:
        print("   - Deriving 'year' from precipitation date...")
        precip['year'] = precip['date'].dt.year
    
    if 'week' not in precip.columns and 'week_of_year' in precip.columns:
        print("   - Renaming 'week_of_year' to 'week'...")
        precip.rename(columns={'week_of_year': 'week'}, inplace=True)
        
    # Select only necessary columns to avoid duplication in merge
    precip_clean = precip[['node_id', 'year', 'week', 'rainfall_sum_mm']]

    # ---------------------------------------------------------
    # 3. LOAD ELEVATION
    # Expected: node_id, lon, lat, elevation_m
    # ---------------------------------------------------------
    elev_file = find_csv(ELEV_DIR)
    print(f"3. Loading Elevation from {elev_file}...")
    elev = pd.read_csv(elev_file)
    
    # We only need node_id and elevation_m (lat/lon usually redundant if in nodes.csv, but elevation is key)
    elev_clean = elev[['node_id', 'elevation_m']]

    # ---------------------------------------------------------
    # 4. MERGE DATASETS
    # ---------------------------------------------------------
    print("4. Merging Rain + Discharge...")
    
    # Merge on Node + Time
    # Using Inner Join to ensure we only keep rows where we have BOTH input (rain) and target (flow)
    merged = pd.merge(
        precip_clean, 
        discharge, 
        on=['node_id', 'year', 'week'], 
        how='inner'
    )
    
    print(f"   Rows after dynamic merge: {len(merged)}")
    
    print("5. Attaching Static Elevation...")
    final = pd.merge(merged, elev_clean, on=['node_id'], how='left')
    
    # ---------------------------------------------------------
    # 5. CLEANUP & SAVE
    # ---------------------------------------------------------
    # Fill missing elevation with 0 if any (e.g. ocean nodes)
    final['elevation_m'] = final['elevation_m'].fillna(0.0)
    
    # Sort for sequence training (Node -> Time)
    final.sort_values(['node_id', 'year', 'week'], inplace=True)
    
    # Final Column Order
    # Structure: ID, Time info, Targets, Features
    cols = ['node_id', 'date', 'year', 'week', 'discharge_avg', 'rainfall_sum_mm', 'elevation_m']
    
    # Keep only columns that actually exist (in case 'date' was lost/renamed, though it should be there)
    cols = [c for c in cols if c in final.columns]
    final = final[cols]
    
    final.to_csv(OUTPUT_FILE, index=False)
    print(f"\n SUCCESS! Master dataset saved to:\n   {OUTPUT_FILE}")
    print("\nSample Data:")
    print(final.head())

if __name__ == "__main__":
    main()