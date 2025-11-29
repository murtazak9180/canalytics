#!/usr/bin/env python3
"""
create_interactive_map_full_graph.py

Creates an interactive Folium map visualizing the entire directed graph
stored in graph/nodes.csv and graph/edges.csv (and optionally the pickled graph).

Outputs:
  ./map/pakistan_network_full.html

Usage:
  python create_interactive_map_full_graph.py
"""

import os
import sys
import math
import pickle
import json
from shapely import wkt
from shapely.geometry import mapping, Point, LineString
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import branca.colormap as cm

# --- Config (relative to current working directory / pwd) ---
PWD = os.getcwd()
GRAPH_DIR = os.path.join(PWD, "graph")
NODES_CSV = os.path.join(GRAPH_DIR, "nodes.csv")
EDGES_CSV = os.path.join(GRAPH_DIR, "edges.csv")
GRAPH_PKL = os.path.join(GRAPH_DIR, "pakistan_ne_10km_network.gpickle")  # optional
OUT_DIR = os.path.join(PWD, "map")
OUT_HTML = os.path.join(OUT_DIR, "pakistan_network_full.html")

# Map sampling / size limits
MAX_EDGE_FEATURES = 30000  # fallback limit to avoid enormous HTML files (adjust as needed)

# Create output folder
os.makedirs(OUT_DIR, exist_ok=True)

# --- Sanity checks ---
for fpath in [NODES_CSV, EDGES_CSV]:
    if not os.path.exists(fpath):
        print(f"ERROR: required file not found: {fpath}")
        print("Ensure you have run the graph-building script and files are in ./graph/")
        sys.exit(1)

print("Loading nodes and edges...")

# --- Load nodes & edges ---
nodes_df = pd.read_csv(NODES_CSV)
edges_df = pd.read_csv(EDGES_CSV)

# Expected columns (best-effort tolerant)
# nodes_df must have: node_id, lon, lat
# edges_df must have: edge_id, from_node_id or from_node, to_node_id or to_node, wkt

# Normalize column names
nodes_df.columns = [c.strip() for c in nodes_df.columns]
edges_df.columns = [c.strip() for c in edges_df.columns]

# Accept a few possible names for the same fields
if 'node_id' not in nodes_df.columns:
    # try common alternatives
    for col in ['id', 'node', 'id_str']:
        if col in nodes_df.columns:
            nodes_df = nodes_df.rename(columns={col: 'node_id'})
            break

for name in ['lon', 'longitude', 'x']:
    if name in nodes_df.columns and 'lon' not in nodes_df.columns:
        nodes_df = nodes_df.rename(columns={name: 'lon'})
        break
for name in ['lat', 'latitude', 'y']:
    if name in nodes_df.columns and 'lat' not in nodes_df.columns:
        nodes_df = nodes_df.rename(columns={name: 'lat'})
        break

# edges: unify from/to column names
if 'from_node_id' not in edges_df.columns:
    for alt in ['from_node', 'from', 'u', 'source']:
        if alt in edges_df.columns:
            edges_df = edges_df.rename(columns={alt: 'from_node_id'})
            break
if 'to_node_id' not in edges_df.columns:
    for alt in ['to_node', 'to', 'v', 'target']:
        if alt in edges_df.columns:
            edges_df = edges_df.rename(columns={alt: 'to_node_id'})
            break

# try to find wkt column
wkt_col = None
for col in edges_df.columns:
    if col.lower() in ('wkt', 'geometry', 'geom', 'wkb'):
        wkt_col = col
        break
if wkt_col is None:
    print("ERROR: edges.csv appears to have no WKT/geometry column (expected 'wkt' or 'geometry').")
    sys.exit(1)

# Convert node coordinates to floats
nodes_df['lon'] = nodes_df['lon'].astype(float)
nodes_df['lat'] = nodes_df['lat'].astype(float)

# Build node lookup for quick geometry -> node_id mapping
node_lookup = {}
for _, r in nodes_df.iterrows():
    node_lookup[str(r['node_id'])] = (float(r['lat']), float(r['lon']))  # folium expects [lat, lon]

# --- Optionally load pickled graph to compute degrees for marker sizing ---
node_degree_map = {}
if os.path.exists(GRAPH_PKL):
    try:
        import networkx as nx
        with open(GRAPH_PKL, 'rb') as f:
            G = pickle.load(f)
        # compute degree (in + out) or in-degree/out-degree if directed
        if nx.is_directed(G):
            for n in G.nodes():
                deg = G.in_degree(n) + G.out_degree(n)
                node_degree_map[str(n)] = deg
        else:
            for n in G.nodes():
                node_degree_map[str(n)] = G.degree(n)
        print(f"Loaded graph pickle and computed degrees for {len(node_degree_map)} nodes.")
    except Exception as e:
        print(f"Warning: could not load graph pickle for degree info: {e}")
else:
    print("Graph pickle not found — marker sizes will be uniform. (Optional: put pickled graph in graph/ to compute node degrees.)")

# --- Build edge geometries from WKT ---
print("Parsing geometries from edges WKT (this may take a moment)...")
edge_features = []
skipped = 0
for i, row in edges_df.iterrows():
    try:
        geom = wkt.loads(row[wkt_col])
        # convert to GeoJSON-like mapping for folium
        geom_json = mapping(geom)
        props = {
            'edge_id': row.get('edge_id', row.get('id', i)),
            'river_name': row.get('river_name', None),
            'from_node': row.get('from_node_id', None),
            'to_node': row.get('to_node_id', None),
            'length_km': row.get('length_km', None)
        }
        edge_features.append({'type': 'Feature', 'geometry': geom_json, 'properties': props})
    except Exception as e:
        skipped += 1
        # continue parsing others
if skipped:
    print(f"Warning: skipped {skipped} edge(s) due to invalid geometry/WKT.")

print(f"Total edges parsed: {len(edge_features)}")

# Optionally sample edges if huge
if len(edge_features) > MAX_EDGE_FEATURES:
    print(f"Edge features exceed {MAX_EDGE_FEATURES} -> sampling down to that limit to keep HTML manageable.")
    import random
    random.seed(42)
    sampled_features = random.sample(edge_features, MAX_EDGE_FEATURES)
else:
    sampled_features = edge_features

# --- Determine map center ---
if len(nodes_df) > 0:
    mean_lat = nodes_df['lat'].mean()
    mean_lon = nodes_df['lon'].mean()
else:
    mean_lat, mean_lon = 30.3, 69.3  # fallback

# --- Create Folium map ---
print("Creating Folium map...")
m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6, tiles="CartoDB positron")

# 1) Add edges GeoJSON layer (sampled)
print("Adding edges layer...")
edges_fg = folium.FeatureGroup(name=f"Edges (sampled {len(sampled_features)})", show=True)
folium.GeoJson(
    {"type": "FeatureCollection", "features": sampled_features},
    name="All Edges",
    style_function=lambda feat: {
        "color": "#4a4a4a",
        "weight": 1.0,
        "opacity": 0.7
    },
    tooltip=folium.GeoJsonTooltip(fields=['edge_id', 'river_name', 'length_km'],
                                  aliases=['edge_id', 'river', 'length_km'],
                                  localize=True)
).add_to(edges_fg)
edges_fg.add_to(m)

# 2) Add nodes as clustered markers (cluster + circle markers)
print("Adding nodes layer...")
cluster_fg = folium.FeatureGroup(name="Nodes (clustered)", show=False)
marker_cluster = MarkerCluster(name="Node cluster").add_to(cluster_fg)

# Prepare a colormap for degree (if any degrees exist)
if len(node_degree_map) > 0:
    degrees = list(node_degree_map.values())
    max_deg = max(degrees) if degrees else 1
    colormap = cm.LinearColormap(['lightblue', 'blue', 'darkblue'], vmin=0, vmax=max_deg)
else:
    colormap = None

# Marker sizing function
def marker_radius_for_node(node_id_str):
    if node_degree_map and node_id_str in node_degree_map:
        # scale degree to reasonable radius [4, 18]
        deg = node_degree_map[node_id_str]
        r = 4 + (deg / max_deg) * 14 if max_deg > 0 else 6
        return max(3, min(20, r))
    else:
        return 6

# add markers
for _, r in nodes_df.iterrows():
    nid = str(r['node_id'])
    lat = float(r['lat'])
    lon = float(r['lon'])
    radius = marker_radius_for_node(nid)
    popup_html = f"Node ID: {nid}<br>Lon: {lon:.6f}<br>Lat: {lat:.6f}"
    if nid in node_degree_map:
        popup_html += f"<br>Degree: {node_degree_map[nid]}"
    # color by degree if available
    if colormap and nid in node_degree_map:
        color = colormap(node_degree_map[nid])
    else:
        color = 'crimson'
    folium.CircleMarker(
        location=[lat, lon],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.8,
        popup=folium.Popup(popup_html, max_width=300)
    ).add_to(marker_cluster)

cluster_fg.add_to(m)

# 3) Add a lightweight nodes-only layer (non-clustered) for quick visibility
nodes_fg = folium.FeatureGroup(name="Nodes (all)", show=False)
for _, r in nodes_df.iterrows():
    nid = str(r['node_id'])
    lat = float(r['lat'])
    lon = float(r['lon'])
    folium.CircleMarker(
        location=[lat, lon],
        radius=2,
        color='#222222',
        fill=True,
        fill_opacity=0.6
    ).add_to(nodes_fg)
nodes_fg.add_to(m)

# 4) Add a small legend (for degree colormap) if available
if colormap:
    colormap.caption = 'Node degree (in+out)'
    m.add_child(colormap)

# 5) Add layer control and save
folium.LayerControl(collapsed=False).add_to(m)

print(f"Saving map to {OUT_HTML} ...")
m.save(OUT_HTML)

print("\nDone!")
print(f"Open: {OUT_HTML} in a browser to explore the full graph.")
