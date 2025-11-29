import geopandas as gpd

# 1. Load your current fragmentation file
input_file = "pakistan_5_rivers.geojson"
gdf = gpd.read_file(input_file)

print(f"Original row count: {len(gdf)}")
# Output might be 8 or 9, as seen in your screenshot

# 2. DISSOLVE
# This groups everything by 'name_en' and merges the geometries
# aggfunc='first' keeps the other attributes from the first occurrence
clean_rivers = gdf.dissolve(by='name_en', as_index=False)

print(f"New row count: {len(clean_rivers)}")
# Output should now be exactly 5 (one for each river)

# 3. Save the clean file
clean_rivers.to_file("pakistan_5_rivers_merged.geojson", driver='GeoJSON')

print("Merged duplicates. You now have exactly 1 feature per river.")