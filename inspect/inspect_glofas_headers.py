#!/usr/bin/env python3
"""
inspect_glofas_header.py
Reads the first 10 bytes of your 'corrupt' files to reveal their true identity.
"""
import os

# Check the first available file
directory = "bulk/glofas_monthly"
files = sorted([f for f in os.listdir(directory) if f.endswith(".nc")])

if not files:
    print("No files found.")
    exit()

target = os.path.join(directory, files[0])
print(f"Inspecting: {target}")
print(f"Size: {os.path.getsize(target) / (1024*1024):.2f} MB")

with open(target, "rb") as f:
    header = f.read(10)
    print(f"Header Bytes: {header}")

    if header.startswith(b"PK"):
        print("\n DIAGNOSIS: It is a ZIP file! (The API zipped it automatically)")
    elif header.startswith(b"CDF"):
        print("\nDIAGNOSIS: It is a generic NetCDF.")
    elif header.startswith(b"\x89HDF"):
        print("\nDIAGNOSIS: It is a NetCDF4 (HDF5) file.")
    elif b"html" in header or b"{" in header:
        print("\nDIAGNOSIS: It is an Error Message (HTML/JSON) saved as a file.")
    else:
        print("\nDIAGNOSIS: Unknown format.")