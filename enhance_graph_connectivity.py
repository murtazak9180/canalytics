#!/usr/bin/env python3
"""
enhance_graph_connectivity.py

Upgrades the graph topology by adding SPATIAL EDGES.
Transforming the graph from a 'Forest of Trees' -> 'Spatial Mesh'.

Inputs:
 - graph/nodes.csv
 - graph/edges.csv (The original river connections)

Outputs:
 - graph/edges_enhanced.csv (Contains River + Spatial edges)

Logic:
 - Edge Type 0: River Flow (Directed, based on original edges)
 - Edge Type 1: Spatial Proximity (Undirected, < 50km distance)
"""

import pandas as pd
import numpy as np
import os
from math import radians, cos, sin, asin, sqrt

# --- CONFIG ---
THRESHOLD_KM = 50.0  # The "Neighbor" radius
INPUT_NODES = os.path.join("graph", "nodes.csv")
INPUT_EDGES = os.path.join("graph", "edges.csv")
OUTPUT_EDGES = os.path.join("graph", "edges_enhanced.csv")

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers.
    return c * r

def main():
    print("--- Enhancing Graph Connectivity ---")
    
    # 1. Load Data
    if not os.path.exists(INPUT_NODES) or not os.path.exists(INPUT_EDGES):
        print(f"Error: Missing input files in graph/ directory.")
        return

    nodes_df = pd.read_csv(INPUT_NODES)
    edges_df = pd.read_csv(INPUT_EDGES)
    
    print(f"Loaded {len(nodes_df)} nodes and {len(edges_df)} river edges.")

    # 2. Process Existing River Edges (Type 0)
    # We keep the structure but add the 'edge_type' column
    river_edges = edges_df.copy()
    river_edges['edge_type'] = 0  # 0 = River Flow
    
    # Keep track of existing connections so we don't duplicate a river edge as a spatial edge
    # Set of tuples (u, v)
    existing_connections = set(zip(river_edges['from_node_id'], river_edges['to_node_id']))

    # 3. Generate Spatial Edges (Type 1)
    print(f"Generating Spatial Edges (Threshold < {THRESHOLD_KM} km)...")
    
    spatial_edges = []
    nodes_list = nodes_df.to_dict('records')
    new_edge_id_start = river_edges['edge_id'].max() + 1
    
    count = 0
    
    # Brute force O(N^2) is fine for ~300 nodes (approx 45k comparisons, instant for Python)
    for i in range(len(nodes_list)):
        for j in range(i + 1, len(nodes_list)):
            node_a = nodes_list[i]
            node_b = nodes_list[j]
            
            id_a = node_a['node_id']
            id_b = node_b['node_id']
            
            # Calculate Distance
            dist = haversine(node_a['lon'], node_a['lat'], node_b['lon'], node_b['lat'])
            
            if dist < THRESHOLD_KM:
                # Check if this link already exists as a river flow
                # (We don't want to overwrite physical flow with virtual spatial links)
                is_river_ab = (id_a, id_b) in existing_connections
                is_river_ba = (id_b, id_a) in existing_connections
                
                if not is_river_ab and not is_river_ba:
                    # ADD UNDIRECTED CONNECTION (A->B and B->A)
                    
                    # A -> B
                    spatial_edges.append({
                        'edge_id': new_edge_id_start + count,
                        'from_node_id': id_a,
                        'to_node_id': id_b,
                        'river_name': 'Spatial_Link',
                        'length_km': dist,
                        'wkt': None, # Spatial edges have no geometry
                        'edge_type': 1 # 1 = Spatial Proximity
                    })
                    count += 1
                    
                    # B -> A
                    spatial_edges.append({
                        'edge_id': new_edge_id_start + count,
                        'from_node_id': id_b,
                        'to_node_id': id_a,
                        'river_name': 'Spatial_Link',
                        'length_km': dist,
                        'wkt': None,
                        'edge_type': 1
                    })
                    count += 1

    print(f"  > Created {count} spatial edges.")

    # 4. Merge and Save
    spatial_df = pd.DataFrame(spatial_edges)
    
    # Combine
    final_df = pd.concat([river_edges, spatial_df], ignore_index=True)
    
    # Save
    final_df.to_csv(OUTPUT_EDGES, index=False)
    print(f"Saved merged edges to: {OUTPUT_EDGES}")
    print(f"Total Edges: {len(final_df)} (River: {len(river_edges)}, Spatial: {len(spatial_df)})")
    
    # Quick Check
    print("\nSample Data:")
    print(final_df[['from_node_id', 'to_node_id', 'length_km', 'edge_type']].head())
    print(final_df[['from_node_id', 'to_node_id', 'length_km', 'edge_type']].tail())

if __name__ == "__main__":
    main()