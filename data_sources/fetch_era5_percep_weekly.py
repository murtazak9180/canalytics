import os
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from tqdm.notebook import tqdm
import time

# --- CONFIG ---
NODES_CSV = "nodes.csv"
# Save outputs to data/precipitation/ under the current working directory
OUT_DIR = os.path.join(os.getcwd(), "data", "precipitation")
OUT_FILE = os.path.join(OUT_DIR, "precipitation_openmeteo_complete.csv")

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)
START_DATE = "2019-01-01"
END_DATE = "2023-12-31"

# --- SETUP ---
# Standard caching to avoid re-downloading things we already have
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Load Nodes
print("Loading nodes...")
nodes = pd.read_csv(NODES_CSV)
node_ids = nodes['node_id'].tolist()
lats = nodes['lat'].tolist()
lons = nodes['lon'].tolist()
print(f"Loaded {len(nodes)} nodes.")

# --- BATCH REQUESTS WITH RATE LIMIT HANDLING ---
# Smaller batch size to be safer
BATCH_SIZE = 30 
all_data = []

url = "https://archive-api.open-meteo.com/v1/archive"

# Total number of batches
num_batches = (len(nodes) + BATCH_SIZE - 1) // BATCH_SIZE

with tqdm(total=num_batches, desc="Processing Batches") as pbar:
    i = 0
    while i < len(nodes):
        # Prepare the batch
        batch_lats = lats[i : i + BATCH_SIZE]
        batch_lons = lons[i : i + BATCH_SIZE]
        batch_ids = node_ids[i : i + BATCH_SIZE]
        
        params = {
            "latitude": batch_lats,
            "longitude": batch_lons,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "daily": "precipitation_sum",
            "timezone": "UTC"
        }
        
        try:
            # Request Data
            responses = openmeteo.weather_api(url, params=params)
            
            # --- PROCESS RESPONSE ---
            for j, response in enumerate(responses):
                nid = batch_ids[j]
                
                # Extract values
                daily = response.Daily()
                precip = daily.Variables(0).ValuesAsNumpy()
                
                # Create Time Index
                start = pd.to_datetime(daily.Time(), unit = "s", origin = "unix")
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", origin = "unix")
                freq = pd.Timedelta(seconds = daily.Interval())
                date_range = pd.date_range(start=start, end=end, freq=freq, inclusive="left")
                
                # Make DataFrame
                df = pd.DataFrame({"date": date_range, "rainfall_daily_mm": precip})
                
                # Resample to Weekly
                df.set_index('date', inplace=True)
                weekly = df.resample('W').sum().reset_index()
                
                # Add Metadata
                weekly.rename(columns={'rainfall_daily_mm': 'rainfall_sum_mm'}, inplace=True)
                weekly['node_id'] = nid
                weekly['week_of_year'] = weekly['date'].dt.isocalendar().week
                weekly['month'] = weekly['date'].dt.month
                
                all_data.append(weekly[['node_id', 'date', 'week_of_year', 'month', 'rainfall_sum_mm']])
            
            # Success! Move to next batch
            i += BATCH_SIZE
            pbar.update(1)
            
            # Gentle pause to respect free tier
            time.sleep(5) 

        except Exception as e:
            # Check for Rate Limit Error
            error_str = str(e).lower()
            if "limit exceeded" in error_str or "429" in error_str:
                print(f"\nRate limit hit at batch starting index {i}. Sleeping for 70 seconds...")
                time.sleep(70)
                # We do NOT increment 'i', so the loop will retry this same batch
            else:
                print(f"\nCritical Error at batch {i}: {e}")
                # Skip this batch to avoid infinite loop on bad data
                i += BATCH_SIZE
                pbar.update(1)

# --- SAVE ---
if all_data:
    print("\nMerging data...")
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(OUT_FILE, index=False)
    print(f"Success! Saved {len(final_df)} rows to {OUT_FILE}")
    print("Download this file to your laptop.")
else:
    print(" No data collected.")