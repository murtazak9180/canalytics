#!/usr/bin/env python3
"""
build_complicated_edges.py

Builds 'graph/edges_complicated.csv' using SPATIOTEMPORAL CORRELATION.

Logic:
1. Loads Node Features (Rainfall/Discharge).
2. Filters for TRAINING DATA ONLY (2019-2021).
3. Calculates Pearson Correlation Matrix for Rainfall between all nodes.
4. Creates edges if:
   - Distance < 100km (Broader search radius)
   - Rainfall Correlation > 0.6 (Statistically significant similarity)
5. Weights the edge based on Correlation and Elevation Delta.

Output:
 - graph/edges_complicated.csv
   - edge_type 0: River
   - edge_type 2: Correlated Spatial Edge
"""

import pandas as pd
import numpy as np
import os
from math import radians, cos, sin, asin, sqrt
from shapely.geometry import LineString

# --- CONFIG ---
PWD = os.getcwd()
NODES_PATH = os.path.join(PWD, "graph", "nodes.csv")
EDGES_PATH = os.path.join(PWD, "graph", "edges.csv") # Original rivers
FEATURES_PATH = os.path.join(PWD, "graph", "nodes_features.csv")
OUTPUT_PATH = os.path.join(PWD, "graph", "edges_complicated.csv")

# Criteria
TRAIN_YEARS = [2019, 2020, 2021]
DIST_THRESHOLD_KM = 100.0  # Look further than 50km because we have correlation to filter bad links
CORR_THRESHOLD = 0.6       # Minimum correlation to accept an edge

def haversine(lon1, lat1, lon2, lat2):
    """Calculate Great Circle distance in km."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    return c * 6371

def main():
    print("--- Building Complicated (Correlated) Edges ---")
    
    # 1. Load Data
    if not os.path.exists(FEATURES_PATH):
        print("ERROR: nodes_features.csv not found.")
        return

    nodes_df = pd.read_csv(NODES_PATH)
    edges_df = pd.read_csv(EDGES_PATH) # Original rivers
    feat_df = pd.read_csv(FEATURES_PATH)
    
    print(f"Loaded Features: {len(feat_df)} rows")

    # 2. Prepare Training Data Matrix (Rainfall)
    # Filter for training years only
    train_df = feat_df[feat_df['year'].isin(TRAIN_YEARS)].copy()
    print(f"Filtered Training Data (2019-2021): {len(train_df)} rows")

    # Pivot: Index=Date, Columns=NodeID, Values=Rainfall
    # We use Rainfall because that represents the "Atmosphere" linking the nodes
    print("Pivoting data for correlation analysis...")
    pivot_rain = train_df.pivot_table(index=['year', 'week'], columns='node_id', values='rainfall_sum_mm')
    
    # Fill missing with 0 (no rain)
    pivot_rain = pivot_rain.fillna(0.0)
    
    # 3. Calculate Pearson Correlation Matrix
    # Shape: (Num_Nodes, Num_Nodes)
    print("Computing Pearson Correlation Matrix...")
    corr_matrix = pivot_rain.corr(method='pearson')
    
    # 4. Build Attributes Lookup
    # We need static elevation for the weighting formula
    # Extract one row per node
    static_stats = feat_df.drop_duplicates('node_id').set_index('node_id')[['elevation_m']]
    node_coords = {str(r['node_id']): (r['lon'], r['lat']) for _, r in nodes_df.iterrows()}

    # 5. Generate Edges
    print(f"Generating Edges (Dist < {DIST_THRESHOLD_KM}km AND Corr > {CORR_THRESHOLD})...")
    
    river_edges = edges_df.copy()
    river_edges['edge_type'] = 0
    river_edges['correlation'] = 1.0 # River flow is 100% causal
    
    # Track existing to avoid duplicates
    existing_links = set(zip(river_edges['from_node_id'], river_edges['to_node_id']))
    
    complex_edges = []
    nodes_list = list(corr_matrix.columns)
    next_id = river_edges['edge_id'].max() + 1
    
    count = 0
    
    for i in range(len(nodes_list)):
        node_i = nodes_list[i]
        if str(node_i) not in node_coords: continue
        
        for j in range(i + 1, len(nodes_list)):
            node_j = nodes_list[j]
            if str(node_j) not in node_coords: continue
            
            # Check Correlation First (Fast lookups)
            corr = corr_matrix.loc[node_i, node_j]
            
            if corr > CORR_THRESHOLD:
                # Check Distance Second (Trig is expensive)
                u_coords = node_coords[str(node_i)]
                v_coords = node_coords[str(node_j)]
                dist = haversine(u_coords[0], u_coords[1], v_coords[0], v_coords[1])
                
                if dist < DIST_THRESHOLD_KM:
                    # Check duplication
                    if (node_i, node_j) in existing_links or (node_j, node_i) in existing_links:
                        continue

                    # --- THE COMPLICATED WEIGHT FORMULA ---
                    # We want to store these attributes so the GNN can use them.
                    # Elevation Delta: Rain behavior changes with altitude.
                    elev_i = static_stats.loc[node_i, 'elevation_m']
                    elev_j = static_stats.loc[node_j, 'elevation_m']
                    elev_delta = abs(elev_i - elev_j)
                    
                    # WKT Geometry
                    geom = LineString([u_coords, v_coords]).wkt

                    # Add Forward
                    complex_edges.append({
                        'edge_id': next_id + count,
                        'from_node_id': node_i,
                        'to_node_id': node_j,
                        'river_name': 'Correlated_Link',
                        'length_km': dist,
                        'wkt': geom,
                        'edge_type': 2,         # 2 = Data Driven
                        'correlation': corr,    # The strength of the link
                        'elev_delta': elev_delta
                    })
                    count += 1
                    
                    # Add Backward
                    complex_edges.append({
                        'edge_id': next_id + count,
                        'from_node_id': node_j,
                        'to_node_id': node_i,
                        'river_name': 'Correlated_Link',
                        'length_km': dist,
                        'wkt': geom,
                        'edge_type': 2,
                        'correlation': corr,
                        'elev_delta': elev_delta
                    })
                    count += 1

    print(f"  > Generated {len(complex_edges)} correlated edges.")

    # 6. Merge and Save
    if complex_edges:
        complex_df = pd.DataFrame(complex_edges)
        # Ensure river edges have the new columns too (fill with defaults)
        river_edges['elev_delta'] = 0.0 
        final_df = pd.concat([river_edges, complex_df], ignore_index=True)
    else:
        final_df = river_edges

    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"SUCCESS: Saved 'edges_complicated.csv' with {len(final_df)} total edges.")
    print("Attributes included: [length_km, edge_type, correlation, elev_delta]")

if __name__ == "__main__":
    main()