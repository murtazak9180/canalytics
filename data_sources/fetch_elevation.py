import os
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from tqdm import tqdm
import time

# --- CONFIG ---
NODES_CSV = "graph/nodes.csv"  # Input
# Save outputs to data/elevation/ under the current working directory
OUT_DIR = os.path.join(os.getcwd(), "data", "elevation")
OUT_FILE = os.path.join(OUT_DIR, "nodes_static_features.csv") # Output

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# --- SETUP CLIENT ---
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# --- LOAD NODES ---
print(f"Loading nodes from {NODES_CSV}...")
nodes = pd.read_csv(NODES_CSV)
node_ids = nodes['node_id'].tolist()
lats = nodes['lat'].tolist()
lons = nodes['lon'].tolist()

# --- BATCH REQUESTS ---
# Open-Meteo allows fetching elevation for multiple points at once
BATCH_SIZE = 50 
all_elevations = []

url = "https://api.open-meteo.com/v1/elevation"

print("Fetching elevation data...")

# We loop through nodes in chunks
for i in tqdm(range(0, len(nodes), BATCH_SIZE)):
    batch_lats = lats[i : i + BATCH_SIZE]
    batch_lons = lons[i : i + BATCH_SIZE]
    batch_ids = node_ids[i : i + BATCH_SIZE]
    
    params = {
        "latitude": batch_lats,
        "longitude": batch_lons
    }
    
    try:
        # The Elevation API returns a simple JSON array, not the complex weather object
        # Note: We use the raw 'requests' session inside the client logic or just standard requests usually,
        # but here we use the openmeteo client wrapper which handles the response parsing.
        
        # However, the Elevation API is simpler than the Archive API. 
        # Let's use the .weather_api function which works for this too if we target the right URL,
        # BUT Open-Meteo's python library is mostly for Weather variables.
        # For elevation, it's safer/easier to just hit the JSON endpoint directly.
        
        import requests
        r = retry_session.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        # The response is {"elevation": [100, 102, 98...]}
        batch_elevs = data['elevation']
        
        # Combine ID with Elevation
        for nid, elev in zip(batch_ids, batch_elevs):
            all_elevations.append({'node_id': nid, 'elevation_m': elev})
            
        time.sleep(0.5) # Be nice to the API

    except Exception as e:
        print(f"Error in batch {i}: {e}")

# --- SAVE ---
if all_elevations:
    elev_df = pd.DataFrame(all_elevations)
    
    # Merge with original node data to keep lat/lon
    final_df = pd.merge(nodes, elev_df, on='node_id')
    
    final_df.to_csv(OUT_FILE, index=False)
    print(f" Success! Saved static features to {OUT_FILE}")
    print(final_df.head())
else:
    print("Failed to fetch elevation.")