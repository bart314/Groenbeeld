# -*- coding: utf-8 -*-
"""
Titel:"NDVI_EGV.py".
    EGV: explore, group (and) vectorize.
Seperate the NDVI of an area in categories and plot it.

Development started at least on Jul 24 2025
Development Halted as of  December 15 2025

@author: Siebrant Hendriks
"""

import math
import rioxarray as riox
from pathlib import Path
import numpy as np
from geocube.vector import vectorize

if __name__ == "__main__":
    # some small bbox test cases for the default service can be found below:
    # bbox_bolsward = (162995.2992, 563126.3802, 165494.1228, 565237.1188)
    # bbox_sneekenmeer = (170233.1786, 554948.3689, 184250.9775, 562942.3701)
    # bbox_julianapark = (164477.7017, 563458.7532, 164896.2459, 563859.1646)
    # bbox_swf = (140032.13 , 534299.47 , 181862.694, 574122.007)
    # bbox_minder_dan_meer = (175068.6865, 558695.7532, 176801.4020, 560228.2752)
    # Filepath of the raster data to be accesed.
    cwd = Path.cwd()
    filepath = Path(cwd / "outdir/ndvi_swf_clipped.tiff")
    # Bbox to display.
    # bbox_inner = (175068.6865, 558695.7532, 176801.4020, 560228.2752)
    bbox_inner = (176675.3868, 558792.7784, 178298.8303, 560184.9679)
    # Block below retrieves smaller bbox from larger dataset.
    x_inner_min, y_inner_min, x_inner_max, y_inner_max = bbox_inner
    ndvi_da = riox.open_rasterio(filepath, chunks="auto", driver="GTiff", dtype="uint8")
    ndvi_da = ndvi_da.squeeze("band", drop=True)
    x_bounds_min, y_bounds_min, \
        x_bounds_max, y_bounds_max = ndvi_da.rio.bounds()
    y_length = y_bounds_max - y_bounds_min
    x_length = x_bounds_max - x_bounds_min
    y_pixel_length = y_length / ndvi_da.sizes["y"]
    x_pixel_length = x_length / ndvi_da.sizes["x"]
    row_start = math.floor((y_bounds_max - y_inner_max) / y_pixel_length)
    row_end = math.ceil((y_bounds_max - y_inner_min) / y_pixel_length)
    col_start = math.floor((x_inner_min - x_bounds_min) / x_pixel_length)
    col_end = math.ceil((x_inner_max - x_bounds_min) / x_pixel_length)
    ndvi_da = ndvi_da[row_start:row_end + 1,
                      col_start:col_end + 1].load()
    # Block below categorizes the data. Edge values are set to own informed insight.
    new_data = np.full(ndvi_da.shape, 255, dtype=np.uint8)
    grouped_da = ndvi_da.copy(data=new_data)
    no_value = ndvi_da.values == 255
    grouped_da.values[no_value] = 0
    bin_1 = np.logical_and(ndvi_da.values >= 0, ndvi_da.values <= 65)
    grouped_da.values[bin_1] = 1
    bin_2 = np.logical_and(ndvi_da.values > 65, ndvi_da.values <= 96)
    grouped_da.values[bin_2] = 2
    bin_3 = np.logical_and(ndvi_da.values > 96, ndvi_da.values <= 118)
    grouped_da.values[bin_3] = 3
    bin_4 = np.logical_and(ndvi_da.values > 118, ndvi_da.values <= 200)
    grouped_da.values[bin_4] = 4
    # Conversion from raster to vector.
    ndvi_gdf = vectorize(grouped_da.astype("uint8"))
    # ndvi_gdf = ndvi_gdf.dissolve(by="NDVI", as_index = False)
    # Plot the new vector data.
    ndvi_gdf.rename(columns={"_data": "ndvi"}, inplace=True)
    ndvi_gdf.plot("ndvi", cmap="Set3", legend=True, categorical=True, legend_kwds={
                  "labels": ["no_value", "zoutwater", "geen_veg", "weinig_veg", "veel_veg"]})
    ndvi_da.close()
