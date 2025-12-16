# -*- coding: utf-8 -*-
"""
Titel:"NDVI_Clip.py".
       Clip data on border of polygon

Development started at least on Sept 22 2025.
Script last inspected at least on dec 08 2025.

@authors: Siebrant Hendriks, Microsoft Copilot (AI Assistant)
emails: siebrant.business@gmail.com
        s.hendriks2@sudwestfryslan.nl
"""

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import numpy as np
import geopandas as gpd
import rasterio as rio
from rasterio.features import rasterize
import rasterio.windows as riow
import os
import psutil
from shapely.geometry import mapping
from shapely import wkb
import sys


def apply_mask(window_tup, filepath_ndvi, geom_wkb):
    """
    on windows apply the maks of the chosen geometry to chosen data.

    Parameters
    ----------
    window_tup : TYPE tup (int, int, int, int)
        the yielded window with the properties:
            column offset,
            row offset,
            window width,
            window height.
    filepath_ndvi : TYPE str
        The filepath of the chosen rasterdata (.tiff file)
    geom_wkb : TYPE well-known binary
        the geometry of the polygon that is uses to mask the data

    Returns
    -------
    col_off : int
        The column offset of the masked window/data.
    row_off : int
        The row offset of the masked window/data.
    width : int
        The width of the window.
    height: int
        The height of the window.
    masked_arr : np.ndarray.uint8
        The array containing the masked data in the area of the window.
    """
    geom = wkb.loads(geom_wkb)
    ndvi_dsr = rio.open(filepath_ndvi, "r")
    col_off, row_off, width, height = map(int, window_tup)
    window = riow.Window(*window_tup)
    w_transform = riow.transform(window, ndvi_dsr.transform)
    geom_mapping = mapping(geom)
    mask = rasterize(shapes=[geom_mapping],
                     out_shape=(int(window.height), int(window.width)),
                     fill=0,
                     transform=w_transform,
                     all_touched=True,
                     default_value=1,
                     dtype=np.uint8).astype(bool)
    masked_arr = ndvi_dsr.read(1, window=window)
    masked_arr = masked_arr.copy()
    masked_arr[mask == False] = 255
    ndvi_dsr.close()
    return (col_off, row_off, width, height), masked_arr


def generate_window_tups(col_len, row_len, width, height):
    """
    Generator that ad hoc yields windows from lager plane

    Parameters
    ----------
    col_len : int
        width (cols) of datapoints of to be generated windows 
    row_len : int
        height (rows) of datapoints of to be generated windows
    width : int
        Width of the original plane
    height : int
        height of the original plan

    Yields
    ------
    window_tup : tup (int, int, int, int)
        the yielded window with the properties:
            column offset,
            row offset,
            window width,
            window height.
    """
    for col_off in range(0, col_len, width):
        for row_off in range(0, row_len, height):
            window_width = min(width, col_len - col_off)
            window_height = min(height, row_len - row_off)
            window_tup = (col_off, row_off, window_width, window_height)
            yield window_tup


def get_optimal_chunksize(ndvi_dsr):
    """
    Calculates chunksize based off of RAM capacity

    Parameters
    ----------
    ndvi_dsr : rasterio.io.DatasetReader
        The rasterio object cotaining a (unread) link to the dataset

    Returns
    -------
    width : int
        The width of a chunk that is workable within system RAM.
    height : int
        The height of a chunk that is workable within system RAM.
    """
    # get thread count
    cores = os.cpu_count()
    worker_cnt = max(1, cores - 1)
    # get (minimal) chunksize as saved in dataset
    height, width = ndvi_dsr.block_shapes[0]
    height = int(height)
    width = int(width)
    # put ofset for testwindow in the middel of dataset so that test window
    # would contain varied data instead of null data (common on edges)
    row_off = ndvi_dsr.shape[0]//2
    col_off = ndvi_dsr.shape[1]//2
    row_off = int(row_off)
    col_off = int(col_off)
    # select data of initial chunk, and measure its RAM use
    test_window = riow.Window(col_off, row_off, width, height)
    test_arr = ndvi_dsr.read(1, window=test_window)
    chunk_mem_use = sys.getsizeof(test_arr)
    # determine available memory and divide over threads
    mem_avail = psutil.virtual_memory().available
    mem_per_thread = mem_avail / worker_cnt
    chunk_cnt = mem_per_thread / chunk_mem_use

    # Check if cunksize can be made larger
    if chunk_cnt > 1:
        chunk_mem_use = chunk_mem_use * 4
        chunk_cnt = mem_per_thread / chunk_mem_use
    while chunk_cnt > 16:
        # make chunks bigger and fewer
        width = width * 2
        height = height * 2
        chunk_mem_use = chunk_mem_use * 4
        chunk_cnt = mem_per_thread / chunk_mem_use
    # send 'optimal' chunksize
    return int(width), int(height)


if __name__ == "__main__":
    # Setup all filepaths
    cwd = Path.cwd()
    filepath_ndvi = Path(cwd / "outdir/ndvi_swf_new.tiff")
    filepath_gem = Path(cwd / "Gemeentegrens_zonder_IJsselmeer.gpkg")
    outpath = Path(cwd / "outdir/ndvi_swf_clipped_geen_IJsselmeer.tiff")

    # Read data
    layer = gpd.list_layers(filepath_gem)['name'][0]
    ndvi_dsr = rio.open(filepath_ndvi, "r")
    profile = ndvi_dsr.profile.copy()
    profile.update(nodata=255,
                   tiled=True,
                   compress="LZW",
                   predictor=1,
                   BIGTIFF="IF_SAFER")
    if outpath.exists():
        outpath.unlink()
    ndvi_clipped = rio.open(outpath, "w", **profile)
    gemeentegrens_gdf = gpd.read_file(filepath_gem, layer=layer)
    geom = gemeentegrens_gdf['geometry'][0]

    # Make sure crs of both inputs align
    assert ndvi_dsr.crs == gemeentegrens_gdf.crs
    # Extract relevant properties from data
    row_len, col_len = ndvi_dsr.shape
    width, height = get_optimal_chunksize(ndvi_dsr)
    workers = max(1, os.cpu_count() - 1)
    ndvi_dsr.close()

    # Setup parallelization of processes
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = []
        # Divide data into subsections, and generate a 'window' for each subsection
        for window in generate_window_tups(col_len, row_len, width, height):
            # Add windows to query to apply the selected mask geometry to each window
            futures.append(executor.submit(
                apply_mask, window, str(filepath_ndvi), geom.wkb))
        for future in as_completed(futures):
            window_tup, masked_arr = future.result()
            window = riow.Window(*map(int, window_tup))
            # Make sure datatype of input and output stay the same
            if masked_arr.dtype != ndvi_clipped.dtypes[0]:
                masked_arr = masked_arr.astype(
                    ndvi_clipped.dtypes[0], copy=False)
            # Write the maksed window to the output file
            ndvi_clipped.write(masked_arr, 1, window=window)
    ndvi_clipped.close()
