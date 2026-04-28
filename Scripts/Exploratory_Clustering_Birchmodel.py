# -*- coding: utf-8 -*-
"""
Exploratory_Clustering.py

Performs unsupervised clustering on neighborhood GeoTIFF data (NDVI, CHM, CIR).
"""

import os
import argparse
from pathlib import Path
import numpy as np
import rasterio as rio
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import normalize
import pandas as pd
import xml.etree.ElementTree as ET
from matplotlib.colors import ListedColormap
from sklearn.cluster import Birch


def process_neighborhood(indir, target_name):
    indir_path = Path(indir)
    ndvi_path = next(indir_path.glob(f"*{target_name}*_NDVI.tif"), None)
    chm_path = next(indir_path.glob(f"*{target_name}*_CHM.tif"), None)
    cir_path = next(indir_path.glob(f"*{target_name}*_CIR.tif"), None)

    print(f"Loading data for {target_name}...")

    # -----------------------------
    # NDVI
    # -----------------------------
    with rio.open(ndvi_path) as src:
        ndvi = src.read(1)
        profile = src.profile
        height, width = ndvi.shape
        ndvi_mask = (ndvi == 255)

    # -----------------------------
    # CHM
    # -----------------------------
    with rio.open(chm_path) as src:
        chm = src.read(1, out_shape=(height, width))
        chm_mask = (chm < -9000)

    # -----------------------------
    # CIR (NIR + RED)
    # -----------------------------
    with rio.open(cir_path) as src:
        nir = src.read(1, out_shape=(height, width))
        red = src.read(2, out_shape=(height, width))
        cir_nodata = src.nodata or 255
        cir_mask = (nir == cir_nodata) | (red == cir_nodata)

    # -----------------------------
    # Combined mask
    # -----------------------------
    invalid_mask = ndvi_mask | chm_mask | cir_mask
    valid_indices = np.where(~invalid_mask)

    print(f"Total valid pixels: {len(valid_indices[0])}")

    # -----------------------------
    # Feature matrix
    # -----------------------------
    features = np.column_stack([
        ndvi[valid_indices],
        chm[valid_indices],
        nir[valid_indices],
        red[valid_indices]
    ]).astype(np.float32)

    return features, valid_indices, invalid_mask, (height, width), profile


def Birch_model(X, valid_indices, profile, raster_shape, target_name, output_dir,
          subset_size=2000000, threshold=1.7, branching_factor=100, n_clusters=None,
          chunk_size=1000000, random_state=42):
    # Ensure the output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # 1. Sampling
    # -----------------------------
    n_samples = X.shape[0]
    subset_size = min(subset_size, n_samples)

    idx = np.random.choice(n_samples, subset_size, replace=False)
    X_sample = X[idx]

    print(f"Using {subset_size}/{n_samples} samples")

    # -----------------------------
    # 2. Scaling
    # -----------------------------
    scaler = StandardScaler()
    X_sample_scaled = scaler.fit_transform(X_sample)

    # -----------------------------
    # 3. Train Birch
    # -----------------------------
    print(f"{'='*40}")
    print("Birch Model")
    print(f"{'='*40}")
    print("Hyperparameters: ")
    print(f"Subset Size: {subset_size}")
    print(f"Threshold: {threshold}")
    print(f"Branching Factor: {branching_factor}")
    print("Training Birch model...")
    print(f"Predicting on {n_samples} pixels in chunks...")

    birch = Birch(
        threshold=threshold,
        branching_factor=branching_factor,
        n_clusters=n_clusters
    )

    birch.fit(X_sample_scaled)

    # -----------------------------
    # 4. Predict full dataset (chunked)
    # -----------------------------
    n_total = X.shape[0]
    labels = np.empty(n_total, dtype=np.int32)

    for i in range(0, n_total, chunk_size):
        X_chunk = X[i:i + chunk_size]
        X_chunk_scaled = scaler.transform(X_chunk)

        labels[i:i + chunk_size] = birch.predict(X_chunk_scaled)

    print(f"{'='*40}")
    print(f"Output")
    print(f"{'='*40}")
    unique_clusters = len(np.unique(labels))
    print(f"Found {unique_clusters} unique clusters")

    # -----------------------------
    # 5. Reconstruct raster
    # -----------------------------
    height, width = raster_shape
    label_raster = np.full((height, width), -1, dtype=np.int32)

    label_raster[valid_indices] = labels

    # -----------------------------
    # 6. Color palette
    # -----------------------------
    palette = [
        (31, 119, 180),  # Blue
        (255, 127, 14),  # Orange
        (44, 160, 44),   # Green
        (214, 39, 40),   # Red
        (148, 103, 189), # Purple
        (140, 86, 75),   # Brown
        (227, 119, 194), # Pink
        (127, 127, 127), # Gray
        (188, 189, 34),  # Olive
        (23, 190, 207),  # Cyan
        (255, 255, 0),   # Yellow
        (0, 0, 0)        # Black
    ]

    colormap = {
        int(i): palette[i % len(palette)] + (255,)
        for i in np.unique(labels)
    }

    # -----------------------------
    # 7. GeoTIFF export
    # -----------------------------
    out_profile = profile.copy()
    out_profile.update(
        dtype=rio.uint8,
        count=1,
        nodata=255,
    )

    output_name_geotiff_path = Path(output_dir) / f"{target_name}_clusters_Birch.tif"
    with rio.open(output_name_geotiff_path, "w", **out_profile) as dst:
        dst.write(label_raster, 1)
        dst.write_colormap(1, colormap)

    print(f"Saved \u2192 {output_name_geotiff_path}")
    # -----------------------------
    # 8. Generate QML sidecar
    # -----------------------------
    qml_content = [
        "<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>",
        "<qgis version='3.28.0' styleCategories='AllStyleCategories'>",
        f"  <pipe-raster-renderer type='paletted' opacity='1' nodataColor='' band='1'>",
        "    <colorPalette>"
    ]
    for i in range(unique_clusters):
        r, g, b = palette[i % len(palette)]
        hex_color = f"#{r:02x}{g:02x}{b:02x}"
        qml_content.append(f"      <paletteEntry color='{hex_color}' value='{i}' label='Cluster {i}'/>")

    qml_content.extend([
        "    </colorPalette>",
        "  </pipe-raster-renderer>",
        "</qgis>"
    ])
    output_name_qml_path = Path(output_dir) / f"{target_name}_clusters_Birch.qml"
    with open(output_name_qml_path, 'w') as f:
        f.write("\n".join(qml_content))

    print(f"Saved \u2192 {output_name_qml_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exploratory Clustering for GeoTIFFs")
    parser.add_argument("--indir", type=str, default="outdir/municipality_survey", help="Input directory")
    parser.add_argument("--target", type=str, help="Neighborhood name/code target")
    parser.add_argument("--file", type=str, default="targets.txt", help="Path to a text file containing neighborhood targets")
    parser.add_argument("--outdir", type=str, default="outdir/exploratory_analysis", help="Base output directory for clustering results")
    parser.add_argument("--output_sub_dir", type=str, default="birch_results", help="Subdirectory within the base output directory for Birch results")
    parser.add_argument("--subset_size", type=int, default=10000000, help="Number of samples to use for Birch training")
    parser.add_argument("--threshold", type=float, default=1.8, help="Birch clustering feature threshold")
    parser.add_argument("--branching_factor", type=int, default=50, help="Birch clustering branching factor")
    parser.add_argument("--n_clusters", type=int, default=None, help="Number of clusters. If None, the number of clusters is determined by the Birch algorithm's internal thresholding.")

    args = parser.parse_args()

    targets = []
    if args.target:
        targets.append(args.target)

    if args.file:
        file_path = Path(args.file)
        if file_path.exists():
            with open(file_path, 'r') as f:
                new_targets = [line.strip() for line in f if line.strip()]
                targets.extend(new_targets)
            targets = list(dict.fromkeys(targets))
            print(f"Loaded {len(new_targets)} targets from {args.file}")
        elif args.file != "targets.txt":
            print(f"Error: File {args.file} not found.")
            exit(1)

    if not targets:
        print("Error: No targets provided. Use --target or create a targets.txt file.")
        parser.print_help()
        exit(1)

    final_output_dir = Path(args.outdir) / args.output_sub_dir

    for target in targets:
        print(f"\n{'='*40}")
        print(f"Processing Target: {target}")
        print(f"{'='*40}")
        features, valid_indices, invalid_mask, shape, profile = process_neighborhood(args.indir, target)
        Birch_model(features, valid_indices, profile, shape, target_name=target, output_dir=final_output_dir,
                    subset_size=args.subset_size, threshold=args.threshold, branching_factor=args.branching_factor,
                    n_clusters=args.n_clusters)
