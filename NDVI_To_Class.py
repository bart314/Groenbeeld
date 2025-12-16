# -*- coding: utf-8 -*-
"""
Titel:"NDVI_To_Class.py".

Development started on 18 sept 2025
Development postponed on 15 Dec 2025

@author: Siebrant Hendriks

emails: siebrant.business@gmail.com
        s.hendriks2@sudwestfryslan.nl
"""

import xarray as xr
from pathlib import Path
import numpy as np
import rasterio as rio
import gc


def ndvi_classify(ndvi_da):
    classed_arr = np.zeros(ndvi_da.shape, dtype=np.uint8)
    geen_veg = np.logical_and(ndvi_da.values >= 0, ndvi_da.values <= 65)
    classed_arr[geen_veg] = 1
    weinig_veg = np.logical_and(ndvi_da.values > 65, ndvi_da.values <= 96)
    classed_arr[weinig_veg] = 2
    middel_veg = np.logical_and(ndvi_da.values > 96, ndvi_da.values <= 118)
    classed_arr[middel_veg] = 3
    veel_veg = np.logical_and(ndvi_da.values > 118, ndvi_da.values <= 200)
    classed_arr[veel_veg] = 4
    return classed_arr


if __name__ == "__main__":
    cwd = Path.cwd()
    filepath = Path(cwd / "outdir/ndvi_swf_geotiff_old.tiff")
    ndvi_ds = xr.open_dataset(filepath, chunks=256)
    ndvi_ds = ndvi_ds.squeeze("band", drop=True)
    ndvi_ds = ndvi_ds.rename({"band_data": "ndvi"})
    bbox = ndvi_ds.rio.bounds()
    width = ndvi_ds.sizes["x"]
    height = ndvi_ds.sizes["y"]
    chunksize = 256
    crs = ndvi_ds.rio.crs
    tf = rio.transform.from_bounds(*bbox, width=width, height=height)

    profile = {"driver": "GTiff",
               "BIGTIFF": "yes",
               "height": height,
               "width": width,
               "count": 1,
               "dtype": np.uint8,
               "crs": crs,
               "transform": tf,
               "tiled": True,
               "compress": "lzw",
               "predictor": 2,
               "blockxsize": chunksize,
               "blockysize": chunksize}

    # Create the file the final array will be stored in.
    new_filepath = Path(cwd / "outdir/ndvi_swf_classes.tiff")
    ndvi_geo = rio.open(new_filepath, mode="w+", **profile)

    for row_off in range(0, height, chunksize):
        for col_off in range(0, width, chunksize):
            window_height = min(chunksize, height - row_off)
            window_width = min(chunksize, width - col_off)
            window = rio.windows.Window(col_off, row_off, window_width,
                                        window_height)
            ndvi_sub_da = ndvi_ds['ndvi'][row_off:row_off + window_height,
                                          col_off:col_off + window_width]
            ndvi_classed_arr = ndvi_classify(ndvi_sub_da)
            ndvi_geo.write(ndvi_classed_arr, window=window, indexes=1)
            del ndvi_sub_da
            del ndvi_classed_arr
            gc.collect()
