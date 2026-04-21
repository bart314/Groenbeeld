# -*- coding: utf-8 -*-
"""
Exploratory_Clustering.py

Performs unsupervised clustering on neighborhood GeoTIFF data (NDVI, CHM, CIR).
Estimates optimal K using the Elbow method.
"""

import os
import argparse
from pathlib import Path
import numpy as np
import rasterio as rio
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import normalize
import matplotlib.pyplot as plt
import pandas as pd
import xml.etree.ElementTree as ET
from matplotlib.colors import ListedColormap
from pathlib import Path


def find_knee(x, y):
    """Simple heuristic to find the 'knee' in an elbow curve."""
    # Vector from first point to last point
    v = np.array([x[-1] - x[0], y[-1] - y[0]])
    v_norm = v / np.linalg.norm(v)

    distances = []
    for i in range(len(x)):
        p = np.array([x[i] - x[0], y[i] - y[0]])
        # Distance to line
        dist = np.linalg.norm(p - np.dot(p, v_norm) * v_norm)
        distances.append(dist)

    return x[np.argmax(distances)]

def process_neighborhood(indir, target_name, outdir, kmeans_type='standard'):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Try to find the files
    indir_path = Path(indir)
    ndvi_path = next(indir_path.glob(f"*{target_name}*_NDVI.tif"), None)
    chm_path = next(indir_path.glob(f"*{target_name}*_CHM.tif"), None)
    cir_path = next(indir_path.glob(f"*{target_name}*_CIR.tif"), None)

    if not all([ndvi_path, chm_path, cir_path]):
        print(f"Error: Could not find all required TIF files for {target_name} in {indir}")
        return

    print(f"Loading data for {target_name}...")

    with rio.open(ndvi_path) as src:
        ndvi = src.read(1)
        profile = src.profile
        ndvi_mask = (ndvi == 255)
        # Convert uint8 (0-200) back to float (-1 to 1) for better intuition or just keep as is
        # We'll just use it as is since we are scaling anyway.

    with rio.open(chm_path) as src:
        chm = src.read(1, out_shape=ndvi.shape) # Ensure same shape
        chm_mask = (chm < -9000)

    with rio.open(cir_path) as src:
        # CIR band 1 is usually NIR, band 2 is Red, band 3 is Green
        nir = src.read(1, out_shape=ndvi.shape)
        red = src.read(2, out_shape=ndvi.shape)
        cir_nodata = src.nodata or 255
        cir_mask = (nir == cir_nodata) | (red == cir_nodata)

    # Combined mask
    invalid_mask = ndvi_mask | chm_mask | cir_mask
    valid_indices = np.where(~invalid_mask)

    print(f"Total valid pixels: {len(valid_indices[0])}")

    # Feature Stack
    # [NDVI, CHM, NIR, RED]
    features = np.column_stack([
        ndvi[valid_indices],
        chm[valid_indices],
        nir[valid_indices],
        red[valid_indices]
    ]).astype(np.float32)

    # Scaling
    print("Scaling features...")
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    if kmeans_type == 'cosine':
        print("Normalizing features for cosine distance effect...")
        features_normalized = normalize(features_scaled, norm='l2', axis=1)
    elif kmeans_type == 'standard':
        print("Running standard Kmeans")
        features_normalized = features_scaled

    # Sampling for K-estimation
    sample_size = min(len(features_normalized), 200000)
    indices = np.random.choice(len(features_normalized), sample_size, replace=False)
    sample = features_normalized[indices]

    # Elbow Method
    print("Estimating optimal K (Elbow method)...")
    ks = range(3, 12)
    inertias = []
    for k in ks:
        kmeans = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=1024)
        kmeans.fit(sample)
        inertias.append(kmeans.inertia_)
        print(f"  K={k}, Inertia={kmeans.inertia_:.2f}")

    optimal_k = find_knee(list(ks), inertias)
    print(f"Suggested optimal K: {optimal_k}")

    # Plot Elbow
    plt.figure(figsize=(10, 6))
    plt.plot(ks, inertias, 'bo-')
    plt.axvline(x=optimal_k, color='r', linestyle='--', label=f'Optimal K={optimal_k}')
    plt.title(f'Elbow Method for {target_name}')
    plt.xlabel('Number of clusters (K)')
    plt.ylabel('Inertia')
    plt.legend()
    plt.savefig(outdir / f"{target_name}_elbow_plot.png")
    plt.close()

    # Final Clustering
    print(f"Running final clustering with K={optimal_k}...")
    kmeans_final = MiniBatchKMeans(n_clusters=optimal_k, random_state=42, batch_size=2048)
    labels_valid = kmeans_final.fit_predict(features_normalized)

    # Map labels back to full image
    full_labels = np.full(ndvi.shape, 255, dtype=np.uint8)
    full_labels[valid_indices] = labels_valid

    # Save GeoTIFF
    output_tif = outdir / f"{target_name}_clusters.tif"
    profile.update(dtype=rio.uint8, count=1, nodata=255)

    # Define color palette (12 distinct colors)
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

    with rio.open(output_tif, 'w', **profile) as dst:
        dst.write(full_labels, 1)
        # Write colormap to the first band
        # Format: {value: (r, g, b, a)}
        colormap = {i: palette[i % len(palette)] + (255,) for i in range(optimal_k)}
        dst.write_colormap(1, colormap)

    # Generate QML sidecar
    output_qml = outdir / f"{target_name}_clusters.qml"
    qml_content = [
        "<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>",
        "<qgis version='3.28.0' styleCategories='AllStyleCategories'>",
        f"  <pipe-raster-renderer type='paletted' opacity='1' nodataColor='' band='1'>",
        "    <colorPalette>"
    ]
    for i in range(optimal_k):
        r, g, b = palette[i % len(palette)]
        hex_color = f"#{r:02x}{g:02x}{b:02x}"
        qml_content.append(f"      <paletteEntry color='{hex_color}' value='{i}' label='Cluster {i}'/>")

    qml_content.extend([
        "    </colorPalette>",
        "  </pipe-raster-renderer>",
        "</qgis>"
    ])

    with open(output_qml, 'w') as f:
        f.write("\n".join(qml_content))

    # Calculate and Save Stats
    print("Calculating cluster stats...")
    cluster_centers_scaled = kmeans_final.cluster_centers_
    cluster_centers = scaler.inverse_transform(cluster_centers_scaled)

    stats_df = pd.DataFrame(cluster_centers, columns=['Mean_NDVI', 'Mean_CHM', 'Mean_NIR', 'Mean_Red'])
    stats_df['Cluster_ID'] = range(optimal_k)

    # Add pixel counts
    unique, counts = np.unique(labels_valid, return_counts=True)
    counts_dict = dict(zip(unique, counts))
    stats_df['Pixel_Count'] = stats_df['Cluster_ID'].map(counts_dict)

    stats_csv = outdir / f"{target_name}_cluster_stats.csv"
    stats_df.to_csv(stats_csv, index=False)

    # Retrieve colorcoding from .qml file for PCA plot
    qml_path = outdir / f"{target_name}_clusters.qml"
    pca_qml_colors = []
    if qml_path.exists():
        try:
            tree = ET.parse(qml_path)
            root = tree.getroot()
            for entry in root.findall('.//paletteEntry'):
                pca_qml_colors.append(entry.get('color'))
        except ET.ParseError as e:
            print(f"Error parsing QML file {qml_path} for PCA colors: {e}")
            pca_qml_colors = []

    if pca_qml_colors and len(pca_qml_colors) == optimal_k:
        pca_cmap = ListedColormap(pca_qml_colors)
    else:
        pca_cmap = plt.get_cmap('tab10', optimal_k)

    # PCA check (sample size 250k)
    max_samples = 250000  # 200k–300k is sweet spot

    n_total = len(features_normalized)
    if n_total > max_samples:
        sample_idx = np.random.choice(n_total, max_samples, replace=False)
    else:
        sample_idx = np.arange(n_total)

    features_sample = features_normalized[sample_idx]
    labels_sample = labels_valid[sample_idx]

    print(f"PCA sample size: {len(features_sample)} / {n_total}")

    pca = PCA(n_components=2, random_state=42)
    features_pca = pca.fit_transform(features_sample)

    explained = pca.explained_variance_ratio_.sum()
    print(f"PCA explained variance (PC1+PC2): {explained:.3f}")

    plt.figure(figsize=(10, 8))

    scatter = plt.scatter(
        features_pca[:, 0],
        features_pca[:, 1],
        c=labels_sample,
        cmap=pca_cmap, 
        s=2,
        alpha=0.6
      )

    plt.title(f'PCA of Clusters for {target_name} (sampled, K={optimal_k})')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.grid(True)

    cbar_ticks = np.arange(optimal_k) # Ticks voor elke cluster ID
    cbar_labels = [f'Cluster {i}' for i in range(optimal_k)] # Labels voor elke cluster

    cbar = plt.colorbar(scatter, ticks=cbar_ticks) # Creëer colorbar met de scatter plot en custom ticks
    cbar.ax.set_yticklabels(cbar_labels) # Stel custom labels in
    cbar.set_label("Cluster ID") # Stel het algemene label in voor de colorbar

    pca_plot_path = outdir / f"{target_name}_pca_clusters.png"
    plt.savefig(pca_plot_path, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Analysis complete.")
    print(f"Files saved to {outdir}:")
    print(f"  - Cluster Map: {output_tif.name}")
    print(f"  - Elbow Plot: {target_name}_elbow_plot.png")
    print(f"  - Cluster Stats: {stats_csv.name}")
    print(f"  - PCA plot: {target_name}_pca_clusters.png")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exploratory Clustering for GeoTIFFs")
    parser.add_argument("--indir", type=str, default="outdir/municipality_survey", help="Input directory")
    parser.add_argument("--target", type=str, help="Neighborhood name/code target")
    parser.add_argument("--file", type=str, default="targets.txt", help="Path to a text file containing neighborhood targets")
    parser.add_argument("--outdir", type=str, default="outdir/exploratory_analysis", help="Output directory")
    parser.add_argument("--kmeans_type", type=str, default='standard', help="K-means type to run")

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
            # Remove duplicates while preserving order
            targets = list(dict.fromkeys(targets))
            print(f"Loaded {len(new_targets)} targets from {args.file}")
        elif args.file != "targets.txt":
            print(f"Error: File {args.file} not found.")
            exit(1)

    if not targets:
        print("Error: No targets provided. Use --target or create a targets.txt file.")
        parser.print_help()
        exit(1)

    for target in targets:
        print(f"\n{'='*40}")
        print(f"Processing Target: {target}")
        print(f"{'='*40}")
        process_neighborhood(args.indir, target, args.outdir, kmeans_type=args.kmeans_type)
