# -*- coding: utf-8 -*-
"""
Title: NDVI_jenks_V2.py

Takes a geotiff and for the binrange k=2-10 Calculates the optimal edge values to classify the data.

Created on Wed Oct 8 2025
last updated on Dec 15 2025

@author: Siebrant Hendriks

emails: siebrant.business@gmail.com
        s.hendriks2@sudwestfryslan.nl
"""
from pathlib import Path
import rioxarray as riox
import dask as dk
import numpy as np
import mapclassify as mc

if __name__ == "__main__":
    # Set the filepath.
    cwd = Path.cwd()
    infile = Path(cwd / "outdir/buurt_samples/BU19000002_ndvi.tif")
    # Read reference to file.
    geo_arr = riox.open_rasterio(infile, mode="r", chunks="auto", masked=False)
    geo_arr = geo_arr.squeeze("band", drop=True)
    data_arr = geo_arr.data
    # Count the distinct values using dask for memory management.
    counts = dk.array.bincount(data_arr.ravel()).compute()
    # Select the counts of only the measurement data (I.E. Skip the novalue 255, and all empty values in between).
    counts = counts[0:201]
    target_n = 400000
    # Size of sample. In a range of 200 distinct values statistical relevance is obtained from
    # a sample size of 60000 and upward. 400000 is quite larger already, while still manageable in calculation.
    total = int(np.sum(counts))
    # Take sample or approximation from the dataset.
    ratios = counts / total
    approximation = np.ceil(ratios * target_n).astype(int)
    # Taking the ceiling guarantees that each value with at least one measurement is represented.
    # This however also means that the sample taken will be ~100 larger than the initial target.
    # Since the both the target and this shift are safely set between the statistical minimum
    # and the computational maximum, this shift isn't of much concern.
    # This approach slightly skews the ratio in favor of values with few measurements.
    # This is no issue as long as the sample size is significantly larger than the amount of
    # possible values.
    values = np.arange(0, len(counts), 1)
    # Write the sample/approximation to a .npy file.
    jenks = np.repeat(values, approximation).astype(int)
    np.save(Path(cwd / "outdir/Jenks_counts.npy"), counts)
    # Calculates "ideal" bin division for the amount of bins in the range of 2-10 using the Fisher-Jenks algorithm.
    # Results are safed to a .txt file.
    with open(Path(cwd / "outdir/Jenks_stat_2.txt"), "w") as stats:
        for k in range(2, 11, 1):
            result = mc.FisherJenks(jenks, k)

            stats.write(f"stats for vegetation with {k} bins are:\n"
                        + f"Border values are: {result.bins}\n"
                        + "Goodness of absolute deviation of fit is: "
                        + f"{result.get_gadf()}\n\n")
