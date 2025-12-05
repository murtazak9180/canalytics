#!/usr/bin/env python3
"""
update_spatial_geometries.py

Updates 'graph/edges_enhanced.csv' by generating WKT geometries
for all Spatial Edges (Type 1).

Input:
  - graph/nodes.csv (Need coordinates)
  - graph/edges_enhanced.csv (Currently missing WKT for spatial)

Output:
  - graph/edges_enhanced.csv (Overwritten with full geometries)
"""

import pandas as pd
import os
from shapely.geometry import LineString

# --- CONFIG ---
PWD = os.getcwd()
NODES_PATH = os.path.join(PWD, "graph", "nodes.csv")
EDGES_PATH = os.path.join(PWD, "graph", "edges_enhanced.csv")

def main():
    print("--- Updating Spatial Geometries ---")
    
    # 1. Load Data
    if not os.path.exists(NODES_PATH) or not os.path.exists(EDGES_PATH):
        print("ERROR: Files not found.")
        return

    nodes_df = pd.read_csv(NODES_PATH)
    edges_df = pd.read_csv(EDGES_PATH)
    
    print(f"Loaded {len(nodes_df)} nodes and {len(edges_df)} edges.")

    # 2. Build Coordinate Lookup
    # Map: node_id -> (lon, lat)
    # Ensure IDs are strings to match edge columns
    node_map = {
        str(row['node_id']): (float(row['lon']), float(row['lat'])) 
        for _, row in nodes_df.iterrows()
    }

    # 3. Update WKT for Spatial Edges
    updated_count = 0
    
    def get_geometry(row):
        nonlocal updated_count
        
        # If WKT exists (Rivers), keep it
        if isinstance(row['wkt'], str) and row['wkt'].startswith('LINESTRING'):
            return row['wkt']
        
        # If WKT is missing (Spatial), generate it
        u_id = str(row['from_node_id'])
        v_id = str(row['to_node_id'])
        
        if u_id in node_map and v_id in node_map:
            p1 = node_map[u_id] # (lon, lat)
            p2 = node_map[v_id]
            updated_count += 1
            return LineString([p1, p2]).wkt
        
        return None # Should not happen if nodes exist

    # Apply the function
    edges_df['wkt'] = edges_df.apply(get_geometry, axis=1)

    # 4. Save
    edges_df.to_csv(EDGES_PATH, index=False)
    
    print(f" Updated geometries for {updated_count} spatial edges.")
    print(f" File overwritten: {EDGES_PATH}")

if __name__ == "__main__":
    main()