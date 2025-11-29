#!/usr/bin/env python3
"""
visualize_rivers.py

Creates a rich, interactive Folium map to visualize the
5 Major Rivers of Pakistan using the cleaned GeoJSON data.

Reads:
 - pakistan_5_rivers_merged.geojson (Local file)
 - Pakistan Border (Fetched from GitHub for context)

Produces:
 - map/pakistan_major_rivers.html
"""

import os
import sys
import geopandas as gpd
import folium

print("--- Starting River Map Visualization ---")

# --- 1. Configuration & Setup ---
RIVERS_FILE = "pakistan_5_rivers_merged.geojson"
OUT_DIR = "map"
OUT_HTML = os.path.join(OUT_DIR, "pakistan_major_rivers.html")

# URL to fetch a low-resolution GeoJSON of Pakistan's border for context
PAK_GEOJSON_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries/PAK.geo.json"

# Color mapping for the specific rivers
RIVER_COLORS = {
    "Indus": "#1f78b4",   # Dark Blue
    "Jhelum": "#a6cee3",  # Light Blue
    "Chenab": "#33a02c",  # Green
    "Ravi": "#e31a1c",    # Red
    "Sutlej": "#ff7f00"   # Orange
}

# Check if input file exists
if not os.path.exists(RIVERS_FILE):
    print(f"ERROR: Required file not found: {RIVERS_FILE}")
    print("Please ensure you ran the merge script first.")
    sys.exit(1)

# Create output directory
os.makedirs(OUT_DIR, exist_ok=True)


# --- 2. Load Data ---
try:
    print(f"Loading river data from {RIVERS_FILE}...")
    rivers_gdf = gpd.read_file(RIVERS_FILE)

    print(f"Loading Pakistan boundary from web...")
    pak_gdf = gpd.read_file(PAK_GEOJSON_URL)
    
except Exception as e:
    print(f"Error loading data: {e}")
    sys.exit(1)


# --- 3. Initialize Folium Map ---
print("Initializing map...")
# Centered roughly on Pakistan
m = folium.Map(location=[30.3, 69.3], zoom_start=6, tiles="CartoDB positron")


# --- 4. Add Map Layers ---

# A. Add Pakistan Border Layer (Context)
print("Adding Pakistan border layer...")
fg_border = folium.FeatureGroup(name="Pakistan Border", show=True)
folium.GeoJson(
    pak_gdf.to_json(),
    name="Pakistan Border",
    style_function=lambda x: {
        "color": "#000000", 
        "weight": 2, 
        "fillOpacity": 0.05, 
        "dashArray": "5, 5"
    }
).add_to(fg_border)
fg_border.add_to(m)

# B. Add River Layers
print("Adding individual river layers...")

# Iterate through the rivers in the GeoDataFrame
for idx, row in rivers_gdf.iterrows():
    river_name = row['name_en']
    
    # Determine color (default to gray if name not in dict)
    color = RIVER_COLORS.get(river_name, "#555555")
    
    # Create a FeatureGroup for this specific river
    # This allows you to toggle "Indus" on/off separately from "Ravi"
    fg_river = folium.FeatureGroup(name=f"River {river_name}", show=True)
    
    # Add the geometry
    folium.GeoJson(
        row.geometry,
        name=river_name,
        tooltip=f"<b>{river_name} River</b>",
        style_function=lambda x, col=color: {
            "color": col, 
            "weight": 4, 
            "opacity": 0.8
        }
    ).add_to(fg_river)
    
    fg_river.add_to(m)
    print(f" - Added {river_name} ({color})")


# --- 5. Finalize and Save Map ---
print("Adding layer control and saving map...")
folium.LayerControl(collapsed=False).add_to(m)
m.save(OUT_HTML)

print(f"\n--- Success! ---")
print(f"Interactive map saved to: {OUT_HTML}")