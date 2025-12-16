# -*- coding: utf-8 -*-
"""
NDVI_Histogram.py
This script reads a geotiff with a single value layer, and plots the count of each distinct value in that layer.

Development started on Oct 2 2025
Last updated on dec 12 2025

@author: siebrant Hendriks
emails: siebrant.business@gmail.com
        s.hendriks2@sudwestfryslan.nl
"""
from pathlib import Path
import rioxarray as riox
import dask as dk
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    cwd = Path.cwd()
    infile = Path(cwd / "outdir/ndvi_swf_clipped_geen_IJsselmeer.tiff")
    # Read the geotiff.
    geo_arr = riox.open_rasterio(infile, mode="r", chunks="auto", masked=False)
    geo_arr = geo_arr.squeeze("band", drop=True)
    data_arr = geo_arr.data
    # Counts the distinct values. Using dask function for automated memory management.
    counts = dk.array.bincount(data_arr.ravel()).compute()
    # Put counts into pandas series in preparation for plotting.
    counts_ds = pd.Series(data=counts[0:201])
    # Make the plot, and add formatting.
    ax = counts_ds.plot(kind="bar", use_index=False)
    ax.set_title("NDVI Histogram", fontsize=16)
    ax.set_xlabel("Measurement Value", fontsize=12)
    ax.set_ylabel("Count of measurement", fontsize=12)
    # Have the values on the X-axis show on an interval of 10.
    ticks = list(range(0, len(counts_ds), 10))
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(t) for t in ticks])
    ax.figure.tight_layout()
    # Plot and safe the result.
    plt.show()
    ax.figure.savefig(Path(cwd / "outdir/histogram.png"), dpi=300)
