#!/usr/bin/env python3
"""
analyze_dataset_integrity.py

Performs a deep health check on 'graph/nodes_features.csv'.
Ensures data is ready for Spatio-Temporal GNN training.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# --- CONFIG ---
BASE_DIR = os.getcwd()
DATASET_CSV = os.path.join(BASE_DIR, "graph", "nodes_features.csv")
NODES_CSV = os.path.join(BASE_DIR, "graph", "nodes.csv")

def main():
    print("--- GML Dataset Health Check ---")
    
    # 1. Load Data
    if not os.path.exists(DATASET_CSV):
        print(f" Missing dataset: {DATASET_CSV}")
        return
    
    print("Loading dataset...")
    df = pd.read_csv(DATASET_CSV)
    df['date'] = pd.to_datetime(df['date'])
    
    # Load Graph Topology for comparison
    if os.path.exists(NODES_CSV):
        graph_nodes = pd.read_csv(NODES_CSV)['node_id'].unique()
    else:
        print(" Warning: graph/nodes.csv missing. Skipping topology check.")
        graph_nodes = []

    # --- 2. BASIC STATS ---
    print(f"\nShape: {df.shape}")
    print(f"   Unique Nodes: {df['node_id'].nunique()}")
    print(f"   Time Range: {df['date'].min()} to {df['date'].max()}")
    
    # --- 3. MISSING VALUES ---
    print("\n Missing Values Check:")
    nans = df.isna().sum()
    if nans.sum() == 0:
        print("    No missing values (NaNs) found.")
    else:
        print("    NaNs detected:")
        print(nans[nans > 0])

    # --- 4. PHYSICS CHECK ---
    print("\n Physics Check:")
    neg_discharge = (df['discharge_avg'] < 0).sum()
    neg_rain = (df['rainfall_sum_mm'] < 0).sum()
    
    if neg_discharge == 0:
        print("    Discharge is valid (>= 0).")
    else:
        print(f"    Found {neg_discharge} rows with negative discharge!")
        
    if neg_rain == 0:
        print("    Rainfall is valid (>= 0).")
    else:
        print(f"    Found {neg_rain} rows with negative rainfall!")

    # --- 5. TEMPORAL CONTINUITY (CRITICAL FOR LSTM) ---
    print("\n⏳ Temporal Continuity Check:")
    # Check if every node has the same number of timesteps
    counts = df.groupby('node_id').size()
    expected_count = counts.mode()[0] # Most common sequence length
    
    imperfect_nodes = counts[counts != expected_count]
    
    if len(imperfect_nodes) == 0:
        print(f"    Perfect! All nodes have exactly {expected_count} weeks of data.")
    else:
        print(f"     WARNING: {len(imperfect_nodes)} nodes have broken timelines.")
        print(f"      Expected length: {expected_count}")
        print("      Problematic nodes sample:")
        print(imperfect_nodes.head())
        print("      (This breaks batching. We must fix this by re-indexing).")

    # --- 6. SPATIAL COMPLETENESS ---
    if len(graph_nodes) > 0:
        print("\n🕸️  Spatial Completeness:")
        dataset_nodes = set(df['node_id'].unique())
        graph_node_set = set(graph_nodes)
        
        missing_in_data = graph_node_set - dataset_nodes
        missing_in_graph = dataset_nodes - graph_node_set
        
        if not missing_in_data and not missing_in_graph:
            print("    Perfect match between Graph Structure and Dataset.")
        else:
            if missing_in_data:
                print(f"    {len(missing_in_data)} nodes exist in Graph but MISSING in Data.")
                print(f"      Example: {list(missing_in_data)[:3]}")
            if missing_in_graph:
                print(f"     {len(missing_in_graph)} nodes exist in Data but NOT in Graph (Safe to ignore).")

    # --- 7. OUTLIER ANALYSIS ---
    print("\n Feature Distribution:")
    print(df[['discharge_avg', 'rainfall_sum_mm', 'elevation_m']].describe().loc[['mean', 'min', 'max', 'std']])
    
    # Correlation Check (Sanity check: Rain should correlate with Flow somewhat)
    corr = df[['rainfall_sum_mm', 'discharge_avg']].corr().iloc[0,1]
    print(f"\n Raw Correlation (Rain vs Discharge): {corr:.4f}")
    if corr < 0.05:
        print("     Low correlation. Note: This is normal if there is a time lag (rain takes weeks to flow).")
        print("       The GNN will learn this lag, but ensure units are correct.")

if __name__ == "__main__":
    main()