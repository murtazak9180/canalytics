#!/usr/bin/env python3
"""
build_highres_pakistan_network.py

Builds a high-resolution, topologically connected graph of Pakistan's 5 major rivers.
It uses Natural Earth geometries but enforces connectivity and discretizes
long rivers into ~10km segments for better GML sampling.

Inputs:
 - data/clean/pakistan_5_rivers_merged.geojson (Must exist)

Outputs:
 - graph/pakistan_ne_10km_network.gpickle (NetworkX Graph Object)
 - pakistan_ne_10km_segments.gpkg (Geopackage for QGIS)  <-- remains in working dir
 - graph/nodes.csv (Node list: id, lat, lon)
 - graph/edges.csv (Edge list: id, u, v, length_km, wkt)
"""

import os
import sys
import geopandas as gpd
import pandas as pd
import networkx as nx
import pickle
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import unary_union, linemerge, substring
import numpy as np

# --- CONFIGURATION ---
PWD = os.getcwd()

DATA_DIR = os.path.join(PWD, "data", "cleaned")
GRAPH_DIR = os.path.join(PWD, "graph")

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(GRAPH_DIR, exist_ok=True)

INPUT_FILE = os.path.join(DATA_DIR, "pakistan_5_rivers_merged.geojson")

OUTPUT_GRAPH = os.path.join(GRAPH_DIR, "pakistan_ne_10km_network.gpickle")
OUTPUT_NODES_CSV = os.path.join(GRAPH_DIR, "nodes.csv")
OUTPUT_EDGES_CSV = os.path.join(GRAPH_DIR, "edges.csv")

# Geopackage is kept in working directory
OUTPUT_GPKG = os.path.join(PWD, "pakistan_ne_10km_segments.gpkg")

# Parameters
SNAP_TOLERANCE = 0.05  # Degrees (~5km). Distance to snap tributary ends to main river.
SEGMENT_LENGTH_KM = 30 # Target length for each edge in the graph.

def explode_multilinestrings(gdf):
    """Breaks MultiLineStrings into individual LineStrings."""
    print("  Exploding MultiLineStrings...")
    lines = []
    for idx, row in gdf.iterrows():
        geom = row.geometry
        name = row.get('name_en', 'Unknown')
        if geom is None:
            continue
        if geom.geom_type == 'MultiLineString':
            for part in geom.geoms:
                lines.append({'river_name': name, 'geometry': part})
        elif geom.geom_type == 'LineString':
            lines.append({'river_name': name, 'geometry': geom})
    return gpd.GeoDataFrame(lines, crs=gdf.crs)

def snap_endpoints(lines_gdf, tolerance):
    """
    Snaps the END point of a line to the nearest line (if within tolerance).
    This fixes disconnected tributaries (e.g., Jhelum not touching Chenab).
    """
    print(f"  Snapping endpoints (tolerance={tolerance} deg)...")
    sindex = lines_gdf.sindex
    new_geoms = []
    snapped_count = 0
    
    for i, row in lines_gdf.iterrows():
        geom = row.geometry
        if geom is None: 
            new_geoms.append(geom)
            continue
        coords = list(geom.coords)
        if len(coords) == 0:
            new_geoms.append(geom)
            continue
        end_point = Point(coords[-1]) # Assume flow is towards the end index
        
        # Spatial query for candidates
        possible_matches_index = list(sindex.intersection(end_point.buffer(tolerance).bounds))
        possible_matches = lines_gdf.iloc[possible_matches_index]
        
        nearest_dist = tolerance
        snap_target = None
        
        for idx, match_row in possible_matches.iterrows():
            if idx == i: continue # Don't snap to self
            if match_row.geometry is None: continue
            dist = match_row.geometry.distance(end_point)
            if dist < nearest_dist:
                nearest_dist = dist
                snap_target = match_row.geometry
        
        if snap_target is not None:
            # Project and snap
            projected_dist = snap_target.project(end_point)
            new_end_point = snap_target.interpolate(projected_dist)
            
            # Replace the last coordinate with the snapped one
            new_coords = coords[:-1] + [(new_end_point.x, new_end_point.y)]
            new_geoms.append(LineString(new_coords))
            snapped_count += 1
        else:
            new_geoms.append(geom)
            
    print(f"  Snapped {snapped_count} endpoints.")
    lines_gdf.geometry = new_geoms
    return lines_gdf

def planarize_lines(lines_gdf):
    """
    Splits lines at all intersections to create topological nodes.
    Uses unary_union to merge and split.
    """
    print("  Planarizing (creating nodes at intersections)...")
    merged = unary_union(lines_gdf.geometry.tolist())
    
    # Explode the result back into simple lines
    if merged.geom_type == 'MultiLineString':
        merged = linemerge(merged)
        if merged.geom_type == 'MultiLineString':
             new_lines = list(merged.geoms)
        else:
             new_lines = [merged]
    else:
        new_lines = [merged]
        
    return gpd.GeoDataFrame(geometry=new_lines, crs=lines_gdf.crs)

def segmentize_lines(lines_gdf, interval_km=10):
    """
    Cuts long lines into smaller segments of ~interval_km length.
    Ensures we have enough nodes for spatial sampling.
    """
    print(f"  Segmentizing lines into ~{interval_km}km chunks...")
    
    # Approx conversion: 1 degree Lat ~= 111 km
    interval_deg = interval_km / 111.0
    
    new_lines = []
    
    for idx, row in lines_gdf.iterrows():
        geom = row.geometry
        river_name = row.get('river_name', 'Unknown') if 'river_name' in row else row.get('name_en', 'Unknown')
        
        if geom is None:
            continue
        if geom.geom_type != 'LineString':
            continue
            
        line_length = geom.length
        
        if line_length <= interval_deg:
            new_lines.append({'river_name': river_name, 'geometry': geom})
            continue
            
        # Calculate number of segments
        num_segments = int(np.ceil(line_length / interval_deg))
        segment_length = line_length / num_segments
        
        for i in range(num_segments):
            start_dist = i * segment_length
            end_dist = (i + 1) * segment_length
            
            # Stop floating point errors from exceeding length
            if end_dist > line_length: end_dist = line_length
            
            segment = substring(geom, start_dist, end_dist)
            new_lines.append({'river_name': river_name, 'geometry': segment})
            
    print(f"  Expanded {len(lines_gdf)} lines into {len(new_lines)} segments.")
    return gpd.GeoDataFrame(new_lines, crs=lines_gdf.crs)

def build_graph_and_csvs(lines_gdf):
    """
    Constructs NetworkX graph and preparing DataFrames for CSV export.
    """
    print("  Building NetworkX Graph and Node/Edge lists...")
    G = nx.DiGraph()
    
    node_data = []
    edge_data = []
    
    # Add unique ID to the dataframe
    lines_gdf = lines_gdf.reset_index(drop=True)
    lines_gdf['edge_id'] = range(1, len(lines_gdf) + 1)
    
    # Track unique nodes to avoid duplicates in CSV list
    seen_nodes = set()
    
    for idx, row in lines_gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        coords = list(geom.coords)
        if len(coords) < 2:
            continue
        
        # Define Nodes by coordinates (Lon, Lat)
        u = coords[0]  # Start Node
        v = coords[-1] # End Node
        
        # Add Nodes to Graph
        # We use the coordinate tuple as the ID in NetworkX
        G.add_node(u, pos=u, type='source' if idx==0 else 'channel')
        G.add_node(v, pos=v, type='channel')
        
        # Add Edge to Graph
        # 1 degree ~ 111km approx
        length_km = geom.length * 111 
        G.add_edge(u, v, 
                   edge_id=row['edge_id'], 
                   name=row.get('river_name', row.get('name_en', 'unknown')), 
                   length_km=length_km,
                   wkt=geom.wkt)
        
        # Collect Edge Data for CSV
        edge_data.append({
            'edge_id': row['edge_id'],
            'from_node_id': f"{u[0]:.5f}_{u[1]:.5f}", # String ID for CSV readability
            'to_node_id': f"{v[0]:.5f}_{v[1]:.5f}",
            'river_name': row.get('river_name', row.get('name_en', 'unknown')),
            'length_km': length_km,
            'wkt': geom.wkt
        })
        
        # Collect Node Data for CSV
        if u not in seen_nodes:
            node_data.append({'node_id': f"{u[0]:.5f}_{u[1]:.5f}", 'lon': u[0], 'lat': u[1]})
            seen_nodes.add(u)
        if v not in seen_nodes:
            node_data.append({'node_id': f"{v[0]:.5f}_{v[1]:.5f}", 'lon': v[0], 'lat': v[1]})
            seen_nodes.add(v)
            
    print(f"  Graph Stats: {G.number_of_nodes()} Nodes, {G.number_of_edges()} Edges")
    return G, pd.DataFrame(node_data), pd.DataFrame(edge_data)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found.")
        sys.exit(1)

    # Ensure graph output directory exists
    graph_dir = os.path.dirname(OUTPUT_GRAPH) or "graph"
    os.makedirs(graph_dir, exist_ok=True)

    print("--- Starting Graph Construction ---")
    
    # 1. Load
    gdf = gpd.read_file(INPUT_FILE)
    print(f"Loaded {len(gdf)} initial river features.")

    # 2. Explode
    exploded = explode_multilinestrings(gdf)

    # 3. Snap
    snapped = snap_endpoints(exploded, SNAP_TOLERANCE)

    # 4. Planarize
    planarized = planarize_lines(snapped)

    # 5. Recover Names (Spatial Join)
    # Planarization kills attributes, so we steal them back from original
    print("  Recovering attributes via Spatial Join...")
    planarized = gpd.sjoin(planarized, gdf[['name_en', 'geometry']], how='left', predicate='covered_by')
    planarized = planarized.rename(columns={'name_en': 'river_name'})
    # Cleanup join artifacts
    planarized = planarized.drop(columns=['index_right'])
    # Remove duplicates if a segment matched multiple overlapping original lines
    planarized = planarized[~planarized.index.duplicated(keep='first')]

    # 6. Segmentize (The 10km step)
    segmented = segmentize_lines(planarized, interval_km=SEGMENT_LENGTH_KM)

    # 7. Build Graph & Data
    G, nodes_df, edges_df = build_graph_and_csvs(segmented)

    # 8. Save Outputs
    print("\n--- Saving Files ---")
    
    # Save GeoPackage (Best for QGIS visualization) - unchanged location
    segmented.to_file(OUTPUT_GPKG, driver="GPKG")
    print(f"Saved Geometries: {OUTPUT_GPKG}")

    # Save CSVs into graph/ directory
    nodes_df.to_csv(OUTPUT_NODES_CSV, index=False)
    edges_df.to_csv(OUTPUT_EDGES_CSV, index=False)
    print(f"Saved Nodes CSV:  {OUTPUT_NODES_CSV} ({len(nodes_df)} rows)")
    print(f"Saved Edges CSV:  {OUTPUT_EDGES_CSV} ({len(edges_df)} rows)")

    # Save Pickle (For your Python GML scripts) into graph/ directory
    with open(OUTPUT_GRAPH, 'wb') as f:
        pickle.dump(G, f, pickle.HIGHEST_PROTOCOL)
    print(f"Saved Graph Object: {OUTPUT_GRAPH}")

    print("\nDone! You are ready for data fetching.")
