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
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import pandas as pd


def find_knee(x, y):
    """Simple heuristic to find the 'knee' in an elbow curve."""
    v = np.array([x[-1] - x[0], y[-1] - y[0]])
    v_norm = v / np.linalg.norm(v)
    
    distances = []
    for i in range(len(x)):
        p = np.array([x[i] - x[0], y[i] - y[0]])
        dist = np.linalg.norm(p - np.dot(p, v_norm) * v_norm)
        distances.append(dist)
    
    return x[np.argmax(distances)]


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


def GMM(features, valid_indices, raster_shape, profile, target_name, output_dir, 
        subset_size=1000000, covariance_type='diag', chunk_size=5000000, random_state=42):
  
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("="*40)
    print("GMM: train on subset sample → predict on all data")
    print("="*40)

    # -----------------------------
    # 1. Sample
    # -----------------------------
    idx = np.random.choice(features.shape[0], size=subset_size, replace=False)
    X_train = features[idx]
    train_perc = round(X_train.shape[0]/features.shape[0]*100, 2) 

    # -----------------------------
    # 2. Scaling
    # -----------------------------
    scaler = StandardScaler()
    feature_scaled = scaler.fit_transform(X_train)

    # -----------------------------
    # 3. Train (find optimal K)
    # -----------------------------
    print(f"Training on sample size: {X_train.shape[0]}/{features.shape[0]} ({train_perc}%)")
    
    bics_results = []
    ks = range(2, 10)

    print(f"Finding optimal K-value ({min(ks)}-{max(ks)})...")
    for k in ks:
        gmm = GaussianMixture(
            n_components=k,
            covariance_type=covariance_type,
            random_state=random_state
        )
        gmm.fit(feature_scaled)
        bic = gmm.bic(feature_scaled)
        bics_results.append(bic)
        print(f"  K={k}, Bic={bic:.2f}")

    optimal_k = find_knee(list(ks), bics_results)
    print(f'optimal K-value: {optimal_k}')

    # -----------------------------
    # 4. Train (final model)
    # -----------------------------
    gmm_final = GaussianMixture(
        n_components=optimal_k,
        covariance_type=covariance_type,
        random_state=random_state
    )
    gmm_final.fit(feature_scaled)

    # -----------------------------
    # 5. Predict pixels (chunked)
    # -----------------------------
    n_samples = features.shape[0]
    labels = np.empty(n_samples, dtype=np.int32)

    print(f"Predicting {n_samples} pixels in chunks...")

    for i in range(0, n_samples, chunk_size):
        end = min(i + chunk_size, n_samples)

        X_chunk = features[i:end]
        X_chunk_scaled = scaler.transform(X_chunk)
        labels[i:end] = gmm_final.predict(X_chunk_scaled)
        print(f"  Processed {end}/{n_samples}")

    # -----------------------------
    # 6. Reconstruct raster
    # -----------------------------
    label_raster = np.full(raster_shape, -1, dtype=np.int32)
    label_raster[valid_indices] = labels

    # -----------------------------
    # 5. Export
    # -----------------------------
    out_profile = profile.copy()
    out_profile.update(
        dtype=rio.uint8,
        count=1,
        nodata=255,
        tiled=True,
        blockxsize=256,
        blockysize=256,
        compress='lzw'
    )

    output_path = Path(output_dir) / f"{target_name}_clusters_GMM.tif"

    palette = [(31,119,180),(255,127,14),(44,160,44),(214,39,40),(148,103,189),
               (140,86,75),(227,119,194),(127,127,127),(188,189,34),(23,190,207)]

    colormap = {
        int(i): palette[i % len(palette)] + (255,)
        for i in np.unique(labels)
    }

    with rio.open(output_path, "w", **out_profile) as dst:
        dst.write(label_raster.astype(np.uint8), 1)
        dst.write_colormap(1, colormap)

    print(f"Saved → {output_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exploratory Clustering for GeoTIFFs")
    parser.add_argument("--indir", type=str, default="outdir/municipality_survey", help="Input directory")
    parser.add_argument("--target", type=str, help="Neighborhood name/code target")
    parser.add_argument("--file", type=str, default="targets.txt", help="Path to a text file containing neighborhood targets")
    parser.add_argument("--outdir", type=str, default="outdir/exploratory_analysis/GMM_results", help="Output directory")
    parser.add_argument("--subset_size", type=int, default=1000000, help="Subset sample size to use for training")
    parser.add_argument("--cov_type", type=str, default="diag", help="Covariance type (full, tied, diag, spherical)")

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
        features, valid_indices, invalid_mask, shape, profile = process_neighborhood(args.indir, target)
        GMM(features, valid_indices, 
            shape, profile, 
            subset_size=args.subset_size,
            covariance_type=args.cov_type,
            target_name=target, 
            output_dir=args.outdir)
