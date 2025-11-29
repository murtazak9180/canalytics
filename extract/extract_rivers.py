import geopandas as gpd

# 1. Load the global shapefile you just downloaded
print("Loading Shapefile...")
# Ensure this matches the .shp file name in your ls output
shapefile_path = "ne_10m_rivers_lake_centerlines.shp"
gdf = gpd.read_file(shapefile_path)

# 2. Define the target rivers
# Natural Earth usually lists them simply as "Indus", "Ravi", etc.
target_names = ["Indus", "Jhelum", "Chenab", "Ravi", "Sutlej"]

# 3. Filter the data
# We use str.contains with a regex OR (|) to catch them even if named "Indus River"
pattern = '|'.join(target_names)
pak_rivers = gdf[gdf['name_en'].str.contains(pattern, case=False, na=False)]

# 4. Clean up columns (optional, keeps the file size small)
pak_rivers = pak_rivers[['name_en', 'geometry']]

# 5. Check what we found
print(f"Found {len(pak_rivers)} segments.")
print(pak_rivers.head())

# 6. Save as GeoJSON (Easier to use in web apps/Python than Shapefiles)
output_file = "pakistan_5_rivers.geojson"
pak_rivers.to_file(output_file, driver='GeoJSON')

print(f"Success! Saved to {output_file}")