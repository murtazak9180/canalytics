#!/usr/bin/env python3
"""
fetch_glofas_final.py

Downloads GloFAS Historical River Discharge (Reanalysis v4.0)
from the Copernicus Early Warning Data Store (EWDS).
"""

import cdsapi
import os
import concurrent.futures
import sys

# --- CONFIGURATION ---
START_YEAR = 2019 #2019
END_YEAR = 2023
OUTPUT_DIR = "bulk/glofas_monthly"
AREA = [37, 60, 23, 78]  # Pakistan Bounding Box

# --- CREDENTIALS ---
# 1. Log in to https://ewds.climate.copernicus.eu/api-how-to
# 2. Copy your UID and Key and paste below:
EWDS_URL = "https://ewds.climate.copernicus.eu/api"
EWDS_KEY = "62c5129a-71a7-441b-a1ac-d9b354dd4792"

# --- MAIN EXECUTION ---
if EWDS_KEY == "YOUR_UID:YOUR_API_KEY_HERE":
    print("❌ ERROR: You must edit the script and paste your EWDS_KEY first!")
    sys.exit(1)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Client (quiet=True stops it from spamming your terminal)
c = cdsapi.Client(url=EWDS_URL, key=EWDS_KEY, quiet=True)

def download_month(args):
    """Worker function to download a single month."""
    year, month = args
    filename = f"glofas_{year}_{month:02d}.nc"
    filepath = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(filepath):
        print(f"   [Skip] {filename} exists.")
        return

    print(f"-> [Queue] Requesting {year}-{month:02d}...")

    try:
        c.retrieve(
            'cems-glofas-historical',
            {
                'format': 'netcdf',
                'variable': 'river_discharge_in_the_last_24_hours',
                'product_type': 'consolidated',
                'system_version': 'version_4_0',
                'hydrological_model': 'lisflood',
                
                # --- THE FIX: Use 'h' prefix for time keys ---
                'hyear': str(year),
                'hmonth': f"{month:02d}",
                'hday': [f"{d:02d}" for d in range(1, 32)], 
                
                'area': AREA,
            },
            filepath
        )
        print(f"[Done] Saved {filename}")
        
    except Exception as e:
        err_msg = str(e).lower()
        if "terms" in err_msg or "license" in err_msg:
            print(f"\n CRITICAL LICENSE ERROR for {year}-{month:02d}")
            print("   You MUST go to this URL and click 'Download' -> 'Accept Terms':")
            print("   https://ewds.climate.copernicus.eu/datasets/cems-glofas-historical?tab=download")
            os._exit(1) 
        elif "valid combination" in err_msg:
            print(f"[Invalid] Parameters for {filename} were rejected by server.")
            print(f"   Debug: {e}")
        else:
            print(f"[Fail] {filename}: {e}")


def main():
    print(f"--- Starting GloFAS Download ({START_YEAR}-{END_YEAR}) ---")
    print(f"Server: {EWDS_URL}")
    print(f"Target: {OUTPUT_DIR}")
    
    # 1. Prepare List of Tasks
    tasks = []
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            tasks.append((year, month))

    # 2. Run with Safety Limits
    # MAX_WORKERS = 2 is the "Safe Limit" for free users. 
    # Increasing this often causes the server to reject your connection (429 Error).
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(download_month, tasks)

    print("\nAll downloads finished.")

if __name__ == "__main__":
    main()