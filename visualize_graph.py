#!/usr/bin/env python3
"""
visualize_final_graph.py

Generates a Folium map from the FULLY GEOMETRIC graph.
- River Edges (Type 0) -> Blue
- Spatial Edges (Type 1) -> Orange Dashed

Inputs:
  - graph/nodes.csv
  - graph/edges_enhanced.csv (Must contain WKT for all edges)

Output:
  - map/pakistan_final_network.html
"""

import pandas as pd
import folium
import os
from shapely import wkt
from shapely.geometry import mapping

# --- CONFIG ---
PWD = os.getcwd()
NODES_PATH = os.path.join(PWD, "graph", "nodes.csv")
EDGES_PATH = os.path.join(PWD, "graph", "edges_enhanced.csv")
OUT_HTML = os.path.join(PWD, "map", "pakistan_final_network.html")

def main():
    print("--- Visualizing Final Graph ---")
    
    # 1. Load Data
    if not os.path.exists(EDGES_PATH):
        print("ERROR: edges_enhanced.csv not found.")
        return

    nodes_df = pd.read_csv(NODES_PATH)
    edges_df = pd.read_csv(EDGES_PATH)
    
    # 2. Initialize Map
    center_lat = nodes_df['lat'].mean()
    center_lon = nodes_df['lon'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="CartoDB positron")

    # 3. Separate Features by Type
    river_features = []
    spatial_features = []

    print("Parsing WKT geometries...")
    
    for _, row in edges_df.iterrows():
        try:
            # Parse WKT
            geom = wkt.loads(row['wkt'])
            geom_json = mapping(geom)
            
            edge_type = int(row.get('edge_type', 0))
            
            feature = {
                'type': 'Feature',
                'geometry': geom_json,
                'properties': {
                    'edge_id': str(row['edge_id']),
                    'len': f"{float(row['length_km']):.1f}km"
                }
            }
            
            if edge_type == 0:
                river_features.append(feature)
            else:
                spatial_features.append(feature)
                
        except Exception as e:
            continue # Skip malformed rows

    # 4. Add Layers
    
    # Layer A: Rivers (Blue, Solid)
    folium.GeoJson(
        {"type": "FeatureCollection", "features": river_features},
        name="Physical Rivers",
        style_function=lambda x: {
            'color': '#0077be', 
            'weight': 3, 
            'opacity': 1.0
        },
        tooltip=folium.GeoJsonTooltip(fields=['len'], aliases=['Length'])
    ).add_to(m)

    # Layer B: Spatial (Orange, Dashed)
    folium.GeoJson(
        {"type": "FeatureCollection", "features": spatial_features},
        name="Spatial Neighbors",
        style_function=lambda x: {
            'color': '#ff5733', 
            'weight': 1.5, 
            'opacity': 0.6,
            'dashArray': '5, 5'
        },
        tooltip=folium.GeoJsonTooltip(fields=['len'], aliases=['Dist'])
    ).add_to(m)

    # 5. Add Nodes (Simple dots)
    for _, row in nodes_df.iterrows():
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=2,
            color='#333',
            fill=True,
            popup=f"Node: {row['node_id']}"
        ).add_to(m)

    # 6. Save
    folium.LayerControl().add_to(m)
    m.save(OUT_HTML)
    print(f" Map saved to: {OUT_HTML}")

if __name__ == "__main__":
    main()