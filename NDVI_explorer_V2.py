# -*- coding: utf-8 -*-
"""
Title: NDVI_explorer_V2.py

Script to display a smaller bounding box within a larger geotiff file

Created on Jul 24 2025
Development halted as of dec 15 2025

@author: siebr
"""

import math
import xarray as xr
import rioxarray as riox
from pathlib import Path

if __name__ == "__main__":
    # filepath of the raster data to be accesed
    filepath = Path("outdir/ndvi_swf_clipped.tiff")
    # bbox to display
    bbox_inner = (175068.6865, 558695.7532, 176801.4020, 560228.2752)
    # bbox_inner = (140032.13, 574120.007, 140034.13, 574122.007)
    x_inner_min, y_inner_min, x_inner_max, y_inner_max = bbox_inner
    ndvi_da = riox.open_rasterio(filepath, chunks="auto")
    ndvi_da = ndvi_da.squeeze("band", drop=True)
    x_bounds_min, y_bounds_min, x_bounds_max, y_bounds_max = ndvi_da.rio.bounds()
    y_length = y_bounds_max - y_bounds_min
    x_length = x_bounds_max - x_bounds_min
    y_pixel_length = y_length / ndvi_da.sizes["y"]
    x_pixel_length = x_length / ndvi_da.sizes["x"]
    row_start = math.floor((y_bounds_max - y_inner_max) / y_pixel_length)
    row_end = math.ceil((y_bounds_max - y_inner_min) / y_pixel_length)
    col_start = math.floor((x_inner_min - x_bounds_min) / x_pixel_length)
    col_end = math.ceil((x_inner_max - x_bounds_min) / x_pixel_length)
    ndvi_da_sub = ndvi_da[row_start:row_end + 1,
                          col_start:col_end + 1]
    ndvi_da_sub.load()
    ndvi_da_sub.plot()
    ndvi_da.close()
