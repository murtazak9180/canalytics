#!/usr/bin/env python3
"""
inspect_zeros.py
Checks 'data/discharge/discharge_weekly_complete.csv' to see if 
zeros are due to dry seasons (normal) or missed pixels (error).
"""

import pandas as pd
import matplotlib.pyplot as plt

CSV_FILE = "data/discharge/discharge_weekly_complete.csv"

def main():
    print(f"Reading {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    
    # Count total readings
    total_rows = len(df)
    zero_rows = len(df[df['discharge_avg'] < 0.1]) # Near zero
    
    print(f"\n--- Global Stats ---")
    print(f"Total Weekly Records: {total_rows}")
    print(f"Zero/Near-Zero Records: {zero_rows} ({zero_rows/total_rows*100:.1f}%)")
    
    # Analyze by Node
    print("\n--- Node Analysis ---")
    node_stats = df.groupby('node_id')['discharge_avg'].apply(lambda x: (x < 0.1).mean() * 100)
    node_means = df.groupby('node_id')['discharge_avg'].mean()
    
    results = pd.DataFrame({'zero_pct': node_stats, 'mean_discharge': node_means})
    results.sort_values('zero_pct', ascending=False, inplace=True)
    
    print(results.head(20))
    print("\nINTERPRETATION:")
    print(" - 0-20% Zeros:  Healthy perennial river (Indus/Chenab).")
    print(" - 30-70% Zeros: Ephemeral/Seasonal river (Ravi/Sutlej or tributaries).")
    print(" - 99-100% Zeros: SPATIAL ERROR. Your node is missing the river pixel.")

if __name__ == "__main__":
    main()