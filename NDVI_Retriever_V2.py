# -*- coding: utf-8 -*-
"""
Title:"NDVI_Retriever_V2.py".

Script that fetches a NDVI dataset from the WMTS of the CIR luchtfoto of PDOK

Development started on Jul 24 2025.
Script last inspected as of dec 08 2025.

@author: Siebrant Hendriks
emails: siebrant.business@gmail.com 
        s.hendriks2@sudwestfryslan.nl
        
Description
-----------
This script generates a geotiff of NDVI values rangeing from 0 to 200 in uint8 format. The area of
the geotiff can be any epsg:28992 bounding box that can be found within the dataset of the PDOK 
WMTS for the CIR luchtfoto. This WMTS is accessed at the following link: 
https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0?request=GetCapabilities&service=wmts
Any user defined parameters can bet set at the bottom of the script in the section:
__name_ == "__main__".

Notes
-----
The current script is written under the assumption of crs epsg:28992 and needs extra modification
for different crs standards. Specifically only cartesian systems using the meter as standerd
unit of measurement will have potential to succeed. Likewise, it is only made for and tested using
the WMTS of PDOK, and thus might fail with other WMTS.

Hardware Requirements
---------------------
TLDR: 32+GB RAM, 200+GB NVME SSD, 3.x GHz quad-core and no GPU recommended.

This is ofcourse depandent on the size of the requested dataset, and the accuracy, or zoom level 
at which the data is being requested. At the zoom level of "14" I would recommend at least 
16GB of RAM. However, even as much as 128GB of RAM might still find its use depending on the request.
Likewise, I would recommend at least 100GB of free SSD storage. Though preferable would be 200-400GB
of NVME SSD storage. An external SSD would also suffice as long as you make sure your USB standard
supports decent enough data speeds, both in connection and cable. I would recommend at least any 
USB 3.x standard. While CPU compute power is relevant it is not as much of a limiting factor as
storage. Any modern quad-core clocked at 3.x GHz with hyperthreading will do the trick, more cores
are still beneficial since the script is written with a fair amount of parallel processing in mind.
No GPU is needed, and probably won't be used by the script if it were present. To implement GPU
operations additional librariers would need to be adressed, which has not been a priority for
current development.
"""
import re
from pathlib import Path
import math
from io import BytesIO
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import os
import gc
from owslib.wmts import WebMapTileService as wmts
from PIL import Image
import numpy as np
import nest_asyncio
import aiohttp
import rasterio as rio


def get_tile_index_from_coords(bbox, x_coord, y_coord):
    """
    Determine the tile/index positions of the given coördinates.

    Parameters
    ----------
    bbox : tuple, float
        Bounding box to the data contained in the WMTS service
        tuple of (xmin, ymin, xmax, ymax).
    x_coord : float
        X coordinates within bbox of which to determine the index position.
    y_coord : float
        Y coordinates within bbox of which to determine the index position.

    Returns
    -------
    x_tile_index : int
        X index of the tile that contains the passed coördinates.
    y_tile_index : int
        Y index of the tile that contains the passed coördinates.

    Notes
    -----
    Coördninates and bbox are assumed to be epsg:28992.
    """
    x_min, y_min, x_max, y_max = bbox
    dist_to_x = x_coord - x_min
    dist_to_y = y_max - y_coord
    matrix_width = url_open.tilematrixsets[crs].tilematrix[zoom].matrixwidth
    matrix_height = url_open.tilematrixsets[crs].tilematrix[zoom].matrixheight
    tile_width_cm = (x_max - x_min) / matrix_width
    tile_height_cm = (y_max - y_min) / matrix_height
    x_tile_pos = math.ceil(dist_to_x / tile_width_cm)
    y_tile_pos = math.ceil(dist_to_y / tile_height_cm)
    x_tile_index = x_tile_pos - 1
    y_tile_index = y_tile_pos - 1
    return x_tile_index, y_tile_index


def get_tile_index_bounds_from_bbox_inner(bbox_outer, bbox_inner):
    """
    Determine the 4 outermost tile/index positions of a given bbox.

    Parameters
    ----------
    bbox_outer : tuple, float
        Bounding box to the data contained in the WMTS service,
        tuple of (xmin, ymin, xmax, ymax).
    bbox_inner : tuple, float
        Bounding box of the data to be retrieved from the WMTS service,
        tuple of (xmin, ymin, xmax, ymax).

    Returns
    -------
    x_index_range : range
        Range of intergers describing the x tile/index positions of the to be
        retrieved  data from the WMTS service.
    y_index_range : range
        Range of intergers describing the y tile/index positions of the to be
        retrieved  data from the WMTS sevice.

    Notes
    -----
    Coördninates and bbox are assumed to be epsg:28992.
    """
    x_lower, y_lower, x_upper, y_upper = bbox_inner
    x_index_lower, y_index_lower = get_tile_index_from_coords(bbox_outer,
                                                              x_lower, y_upper)
    x_index_upper, y_index_upper = get_tile_index_from_coords(bbox_outer,
                                                              x_upper, y_lower)
    # y_lower en upper swappen met y_index_lower en upper omdat de index van
    # boven naar beneden telt
    x_index_range = range(x_index_lower, x_index_upper + 1)
    y_index_range = range(y_index_lower, y_index_upper + 1)
    return x_index_range, y_index_range


def get_bbox_of_tile_index_bounds(bbox_outer, x_index_range,
                                  y_index_range):
    """
    Determine the bounding box of the to be retrieved data.

    Parameters
    ----------
    bbox_outer : tuple, float
        Bounding box to the data contained in the WMTS service,
        tuple of (xmin, ymin, xmax, ymax).
    x_index_range : range
        Range of intergers describing the x tile/index positions of the to be,
        retrieved  data from the WMTS service.
    y_index_range : range
        Range of intergers describing the y tile/index positions of the to be,
        retrieved  data from the WMTS sevice.

    Returns
    -------
    x_lower: int
    y_lower: int
    x_upper: int
    y_upper: int
    These four variables describe the bbox that contains the tiles described by
    x_index_range and y_index_range.

    Notes
    -----
    Coördninates and bbox are assumed to be epsg:28992.
    """
    x_min, y_min, x_max, y_max = bbox_outer
    matrix_width = url_open.tilematrixsets[crs].tilematrix[zoom].matrixwidth
    matrix_height = url_open.tilematrixsets[crs].tilematrix[zoom].matrixheight
    tile_width_cm = (x_max - x_min) / matrix_width
    tile_height_cm = (y_max - y_min) / matrix_height
    x_lower = x_min + (x_index_range[0] * tile_width_cm)
    x_upper = x_min + ((x_index_range[-1] + 1) * tile_width_cm)
    y_upper = y_max - (y_index_range[0] * tile_height_cm)
    y_lower = y_max - ((y_index_range[-1] + 1) * tile_height_cm)
    return x_lower, y_lower, x_upper, y_upper


def get_bbox_epsg28992():
    """
    Calculate the boundingbox of the entire WMTS dataset for the crs 28992.

    Parameters
    ----------
    source_url(global) : str
        The url accessing to base of the WMTS dataset/service.
    zoom(global) : str
        The number associated with the tilematrix or "zoom" of which the data
        is accessed. 

    Returns
    -------
    x_min : int
    y_min : int
    x_max : int
    y_max : int
    These are the four integers decribing the boundingbox of the data contained
    within the accesed WMTS service.

    Note
    ----
    Current usecase with zoom not hardcoded to be '00', this is yet to be tested.
    """
    opened_wmts = wmts(source_url)
    x_tile_amount = opened_wmts.tilematrixsets[crs].tilematrix[zoom].matrixheight
    y_tile_amount = opened_wmts.tilematrixsets[crs].tilematrix[zoom].matrixwidth
    x_min, y_max = opened_wmts.tilematrixsets[crs].tilematrix[zoom]\
        .topleftcorner
    dist_x = opened_wmts.tilematrixsets[crs].tilematrix[zoom]\
        .scaledenominator * tileheight * 0.00028 * x_tile_amount
    dist_y = opened_wmts.tilematrixsets[crs].tilematrix[zoom]\
        .scaledenominator * tilewidth * 0.00028 * y_tile_amount
    x_max = x_min + dist_x
    y_min = y_max - dist_y
    return x_min, y_min, x_max, y_max


def midprocess_url():
    """
    Determine the URL that wil be common to all server requests made.

    Parameters
    ----------
    source_url(global) : str
        The root URL of the WMTS webservice that will be passed to owslib's
        WMTS module.
    layer(global) : str
        The layer of the WMTS service from which the data will be retrieved.
    crs(global) : str
        The crs or TileMatrixSet used for requesting the data. This needs to
        be a TileMatrixSet present and described within the specific WMTS
        server.
    zoom(global) : str
        The zoom level or TileMaterix at which requests are made to the WMTS
        service.

    Returns
    -------
    url : str
        URL that is common to all future requests made to the WMTS service, and
        in doing so has all the passed parameters included.
    """
    layer = dataset_identifier
    source = wmts(source_url)
    url = source[f"{layer}"].resourceURLs[0]["template"]
    url = re.sub("{TileMatrixSet}", f"{crs}", url)
    url = re.sub("{TileMatrix}", f"{zoom}", url)
    url = re.sub("/{TileCol}/{TileRow}.jpeg", "", url)
    return url


def build_batch_query_list(thread_cnt, row_range, col_range):
    """
    Build a list of queries to be passed to the ThreadPoolExecutor.

    Parameters
    ----------
    thread_cnt : int
        The amount of threads among which all the server requests will be
        divided.
    row_range : range
        Range describing the amount of rows of the tiles to be requested.
    col_range : range
        Range describing the amount of columns of the tiles to be requested.

    Returns
    -------
    query_list : list, tuple, int, range, range
        List containing the queries to be passed through the ThreadPoolExecutor
        to the function of 'single_thread_operations' each list item is a tuple
        containing therewithin:

        thread : int
            The number or identifier assinged to the thread that will execute
            this specific query.
        subrange : range
            Range describing the specific amount of rows passed to this query.
            This will be a subset of the total rows.
        col_range : range
            Range of columns to be passed to this query. Since the queries are
            only divided by row this will pass the entire column range.
    """
    query_list = []
    start_row = 0
    end_row = 0
    row_len = len(row_range)
    thread_interval = round(row_len / thread_cnt)
    for thread in range(thread_cnt):
        # if we are at any thread beyond the first, then we can continue from
        # previous data
        if not thread == 0:
            start_row = end_row
            end_row += thread_interval
            # makes sure there won't be more rows queried than present
            end_row = min(end_row, row_range[-1] + 1)
            #
            subrange = range(start_row, end_row)
        # If this is the first thread to be assinged, then we need to initiate
        # some parameters.
        else:
            start_row = row_range[0]
            end_row = start_row + thread_interval
            subrange = range(start_row, end_row)
        query_list.append((thread + 1, subrange, col_range))
        rows_remaining = row_len - len(range(row_range[0], end_row))
        # ensures that queries will be divided as evenly as possible among
        # threads
        if rows_remaining:
            thread_interval = round(rows_remaining
                                    / (thread_cnt - (thread + 1)))
    return query_list


# noinspection PyTypeChecker
async def tile_bytes_to_ndvi_array(tile_bytes):
    """
    Take the bytestring of a request and calculates the ndvi array of it.

    Parameters
    ----------
    tile_bytes : str
        Bytestring retrieved from a sing WMTS webserver request. contains data
        of a single tile; default is: (3,256,256) CIR jpeg.

    Returns
    -------
    ndvi_array : np.array, np.uint8
        Array cotaining the calculated and transformed ndvi of the tile request as extracted 
        from the bytestring.

    Notes
    -----
    while this function is specialized to calculating the ndvi, it can serve as
    template for future functions processing the results of a tile request.
    """
    tile_jpeg = Image.open(BytesIO(tile_bytes))
    tile_array = np.array(tile_jpeg, dtype=np.float32)
    nir = tile_array[:, :, 0]
    red = tile_array[:, :, 1]
    numerator = nir - red
    denominator = nir + red
    # calculation of the ndvi adjusted for handleing divisions by zero
    # instead of yielding inf devision by zero will yield zero
    ndvi_array = np.divide(numerator, denominator,
                           out=np.zeros_like(numerator),
                           where=denominator != 0)
    # Convert values of range -1 to 1 into 0 to 200.
    # This is so that data can be stored into smaller uint8 format.
    ndvi_array = np.round((ndvi_array + 1) * 100).astype(np.uint8)
    return ndvi_array


# noinspection PyTypeChecker
async def get_tile(session, r_pos, c_pos, attempts=0):
    """
    Request a specified tile from the WMTS service.

    Parameters
    ----------
    session : aiohttp.ClientSession
        An active aiohttp session object used to  perform the HTTP request.
        This session should be reused across multiple requests to take
        advantage of connection pooling and efficient resource management.
    common_url(global) : str
        The URL sued to construct the request made to the WMTS service.
    r_pos : int
        The row position passed to the request.
    c_pos : int
        The column position passed to the request.
    attempts : int, optional
        The number of attempt made to request the WMTS service.
        The default is 0. The function will 'siently' timeout after 10 
        attempts.

    Returns
    -------
    ndvi_array : np.array, np.uint8
        2D array containing the calculated ndvi of the requested tile
        will be empty (filled with nodaavalue of 255) if the request timed out.
    fail : tuple, int
        Tuple containing the row and column position of a failed tile request.
        This will be empty if the request was succesfull.
    """
    fail = ()
    async with session.get(f'{common_url}/{c_pos}/{r_pos}.jpg') as resp:
        if 199 < resp.status < 300:
            tile_bytes = await resp.read()
            ndvi_array = await tile_bytes_to_ndvi_array(tile_bytes)
        else:
            if attempts > 9:
                ndvi_array = np.full(
                    (tileheight, tilewidth), 255, dtype=np.uint8)
                fail = (r_pos, c_pos)
                print(f"a request failed at coords: {r_pos},{c_pos}")
                return ndvi_array, fail
            await asyncio.sleep(10)
            ndvi_array, fail = await get_tile(session, r_pos, c_pos,
                                              attempts + 1)
    return ndvi_array, fail


async def build_set(output_array, thread, row_range, col_range):
    """
    Build a construct.npy file with all data of requests specified by ranges.

    Parameters
    ----------
    output_array : np.memmap(), np.array, np.uint8
        Memory mapped numpy array where the retrieved data will be stored.
    thread : int
        Number identity of current thread.
    row_range : range
        Range describing the amount of rows of the tiles to be requested.
    col_range : range
        Range describing the amount of columns of the tiles to be requested.

    Returns
    -------
    fail_list : list, tupple, int
        List of tupples containing the indexes of a failed request.

    """
    fail_list = []
    async with aiohttp.ClientSession() as session:
        for row_pos in row_range:
            for col_pos in col_range:
                ndvi_array, fail = await get_tile(session, r_pos=row_pos,
                                                  c_pos=col_pos)
                if fail:
                    fail_list.append(fail)
                output_row_start = (row_pos - row_range[0]) * tileheight
                output_col_start = (col_pos - col_range[0]) * tilewidth
                output_row_end = output_row_start + tileheight
                output_col_end = output_col_start + tilewidth
                output_array[output_row_start:output_row_end,
                             output_col_start:output_col_end] = ndvi_array
            print(f"Thread {thread}\thas constructed Row:"
                  + f" {row_pos - first_row}  \tat:{time.time() - start:,.2f}")
    return fail_list


def single_thread_operations(query):
    """
    Perform the operations of a single thread, described in a single query.

    Parameters
    ----------
    query : tuple, int, range, range
        thread : int
            The number or identifier assinged to the thread that will execute
            this specific query.
        subrange : range
            Range describing the specific amount of rows passed to this query.
            This will be a subset of the total rows.
        col_range : range
            Range of columns to be passed to this query. Since the queries are
            only divided by row this will pass the entire column range.

    Returns
    -------
    filepath : str
        The filepath of the resulting construct.npy file in which the resulting
        ndvi array is saved.
    shape : tuple, int
        Tupple describing the shape of the array contained in "construct.npy".
    fail_list : list, int
        List of the tile inedexes of failed requests to the WMTS service.
    """
    thread = query[0]
    print(f"thread {thread}\tstarted runing at:\t{time.time() - start:,.2f}")
    row_range = query[1]
    col_range = query[2]
    fail_list = []
    filepath = outdir / f"construct_{thread}.npy"
    shape = ((len(row_range) * tileheight), (len(col_range) * tilewidth))
    output_array = np.memmap(filepath, mode='w+',
                             dtype=np.uint8, shape=shape)
    fail_list = asyncio.run(build_set(output_array, thread,
                                      row_range, col_range))
    output_array.flush()
    del output_array
    print(f"Thread {thread}\tcompleted operations\tat:\t"
          + f"{time.time() - start:,.2f}")
    return filepath, shape, fail_list


async def amend_fails_per_construct(fail_list, con_array,
                                    row_start, col_start):
    """
    Retry the failed requests applicable to a specific construct.npy file.

    Parameters
    ----------
    fail_list : list, int
        List of the tile inedexes of failed requests to the WMTS service.
    con_array : np.memmap(), np.array, np.uint8
        Array retrieved from a specified construct.npy file.
    row_start : int
        The first row in the WMTS service of the retrieved data.
    col_start : int
        The first column in the WMTS service of the retrieved data.

    Returns
    -------
    None;
        con_array will be mended inplace.
    """
    async with aiohttp.ClientSession() as session:
        for fail in fail_list:
            r_pos = fail[0]
            c_pos = fail[1]
            ndvi_tile, new_fail = await get_tile(session, r_pos=r_pos,
                                                 c_pos=c_pos)
            if new_fail:
                print(f"A fail could not be mended at (row:{r_pos}, "
                      + f"col:{c_pos}). Proceeding with blank tile.")
            else:
                print("A fail can and will be mended at "
                      + f"(row:{r_pos}, col:{c_pos})")
            output_row_start = (r_pos - row_start) * tileheight
            output_col_start = (c_pos - col_start) * tilewidth
            output_row_end = output_row_start + tileheight
            output_col_end = output_col_start + tilewidth
            con_array[output_row_start:output_row_end,
                      output_col_start:output_col_end] = ndvi_tile
            con_array.flush()
    return


# noinspection PyTypeChecker
async def amend_fails_all_constructs(constructs_info_list, row_range,
                                     col_range):
    """
    Retries the failed requests applicable to all construct.npy files.

    Parameters
    ----------
    constructs_info_list : list, str, tuple, int
        List containing the filepaths, shapes, and fail list for each
        "construct.npy" file.
    row_range : range
        Range describing the amount of rows of the tiles requested.
    col_range : range
        Range describing the amount of columns of the tiles requested.

    Returns
    -------
    None;
        All construct.npy files will be updated with the repaired requests.

    """
    for entry in constructs_info_list:
        filepath = entry[0]
        construct_shape = entry[1]
        fail_list = entry[2]
        construct_array = np.memmap(filepath, mode= "r+", dtype= np.uint8, shape= construct_shape)
        if fail_list:
            row_range_start = row_range[0]
            col_range_start = col_range[0]
            await amend_fails_per_construct(fail_list, construct_array,
                                            row_start=row_range_start,
                                            col_start=col_range_start)
            construct_array.flush()
        del construct_array


# noinspection PyBroadException
async def remove_construct(filepath):
    """
    Delete a construct.npy file (after the script is done using it).

    Parameters
    ----------
    filepath : str
        The filepath of the to be deleted construct.npy files.

    Returns
    -------
    None.

    """
    attempts = 0
    while Path(filepath).is_file():
        try:
            os.remove(filepath)
        except:
            await asyncio.sleep(10)
            attempts += 1
        if attempts > 9:
            print(f"cold not remove construct at {filepath}")
            break


# noinspection PyGlobalUndefined
async def main():
    """
    Perform the main function of the script.

    Returns
    -------
    None.

    Notes
    -----
    The operations can be boiled down to:
        1: Determine request parameters;
           (these are partially moved to globals i.e. __name__ == "__main__"
            for sake of implementing future updates)
        2: Retrieve the requested data from the WMTS service.
        3: Format the collected data into a fully fledged geotiff file.
    In future versions it might be possible to trimm down the third step by
    already saving the retrieved intermediates into the final format.
    """

    # the epsg:28992 bounding box of the source dataset
    # future updates might implement different crs standards as wel
    bbox_source = get_bbox_epsg28992()

    # col_range and row_range indicate the indices of the requested data, which
    # will be retrieved from the WMTS service
    col_range, row_range = get_tile_index_bounds_from_bbox_inner(bbox_source,
                                                                 bbox_request)
    global first_row
    first_row = row_range[0]
    # Since the data is provided in tiles the to be retrieved data is
    # likely to cover a slightly larger area than the initial request.
    # The above function call calculates the area fo the to be retrieved data.
    bbox_tiles = get_bbox_of_tile_index_bounds(bbox_source, col_range,
                                               row_range)
    # An estimation of the amount of pythonic threads that the script can
    # comfortably utilise. (Note that pythonic threads do not equate to threads
    # as determined by the physical cpu core count.) This means that different
    # amounts of pythonic threads could be investigated for runtime
    # optimization
    thread_cnt = math.floor(os.cpu_count() * 1.5)
    # Calculate the shape of the final dataset, which is needed to initialise
    # the array it will be stored in.
    height = len(row_range) * tileheight
    width = len(col_range) * tilewidth
    # Initiate metadata needed for the construction of the geotiff file.
    tf = rio.transform.from_bounds(*bbox_tiles, width=width, height=height)

    profile = {"driver": "GTiff",
               "BIGTIFF": "yes",
               "height": height,
               "width": width,
               "count": 1,
               "dtype": np.uint8,
               "crs": crs,
               "transform": tf,
               "nodata": 255,
               "tiled": True,
               "compress": "lzw",
               "predictor": 2,
               "blockxsize": chunksize,
               "blockysize": chunksize}

    # Create the file the final array will be stored in.
    filepath = outdir / filename
    ndvi_geo = rio.open(filepath, mode="w+", **profile)
    # The list of queries passed to the ThreadPoolExecutor. Each thread will
    # execute one query, where one query will contain an equal amount of server
    # requests
    query_list = build_batch_query_list(thread_cnt=thread_cnt,
                                        row_range=row_range,
                                        col_range=col_range)
    print(f"initializing done in: {time.time() - start:,.2f}")
    # This will assing each query to a thread
    with ThreadPoolExecutor(max_workers=thread_cnt) as executor:
        results = executor.map(single_thread_operations, query_list)
        constructs_info_list = []
        # This will make sure the operations of each thread will be executed.
        # While the retrieved data is stored to file, some additional
        # information needed for further operations of the script is retrieved
        # in the construct_info_list
        for result in results:
            constructs_info_list.append(result)
    print("time eplased till end of first pass download: "
          + f"{time.time() - start:,.2f}")
    # If in the initial pass some server requests failed, they will be retried
    # here.
    await amend_fails_all_constructs(constructs_info_list, row_range,
                                     col_range)
    print("time eplased till end of  last pass download: "
          + f"{time.time() - start:,.2f}")
    print("proceeding to final stitching...")
    # Fills the file one 2048*2048 tile at a time, reading from the various
    # intermediate constructs as it goes.
    row_start = 0
    row_end = 0
    for entry in constructs_info_list:
        filepath = entry[0]
        construct_shape = entry[1]
        construct_array = np.memmap(filepath, mode="r", dtype=np.uint8,
                                    shape=construct_shape)
        height = construct_shape[0]
        width = construct_shape[1]
        if not row_end:
            row_end = height
        else:
            row_start = row_end
            row_end = row_start + height
        for row_off in range(row_start, row_end, chunksize):
            for col_off in range(0, width, chunksize):
                window_height = min(chunksize, row_end - row_off)
                window_width = min(chunksize, width - col_off)
                window = rio.windows.Window(col_off, row_off, window_width,
                                            window_height)
                # ca: current array
                row_off_ca = row_off - row_start
                tile = construct_array[row_off_ca:row_off_ca + window_height,
                                       col_off:col_off + window_width]
                ndvi_geo.write(tile, window=window, indexes=1)
                del tile
                gc.collect()
        print(f"{filepath.name} \thas been incorportated at: "
              + f"{time.time() - start:,.2f}")
        construct_array._mmap.close()
        del construct_array
        await remove_construct(filepath)
    print("Construction of geotiff done, performing final cleanup at: "
          + f"{time.time() - start:,.2f}")
    ndvi_geo.close()
    del ndvi_geo
    gc.collect()

if __name__ == "__main__":
    # Set the start time of the script as a (global) variable.
    start = time.time()
    # Nest asyncio allows multiple event loops to be initiated, which removes
    # headaches when IDEs such a spyder and jupyter lab initiate their own
    # eventloop in debugging and the likes.
    nest_asyncio.apply()
    # Set request specific (global) variables

    # This makes sure an outdir is present for output files
    current_dir = Path.cwd()
    outdir = current_dir / "outdir"
    if not Path.exists(outdir):
        outdir.mkdir()
    # Next is the identifier or name that is associated with the dataset
    # present in the used WMTS, from which we will retrieve our data.
    # the script "WMTS_Explorere_V*.py" can be used to retrieve usefull
    # alternatives.
    dataset_identifier = "2024_ortho25IR"
    # This is the CRS that will be used in making the request.
    # Note that currently only the CRS of "EPSG:28992" is implemented.
    # The script will need further modifications to support other CRS.
    crs = "EPSG:28992"
    # This is de URL  being used for the WMTS, currently set to:
    # PDOK CIR arial photo WMTS archive server
    source_url = "https://service.pdok.nl/hwh/luchtfotocir/wmts/"
    source_url = source_url + "v1_0?request=GetCapabilities&service=wmts"
    # Next is the zoom level the script will use.
    # It amounts to an accuracy or resolution of
    # 0.21m per pixel at the default settings.
    # Since the analog measurements have been made at 0.25m accuracy,
    # this is the most accurate usefull zoom level.
    # Ofcourse, this only applies to the default service (PDOK CIR arial).

    # The accuracy of a zoom level can be determined by retrieving
    # the scale denominator of that level, which is the 'tile matrix' from the
    # WMTS service. This scale denominator needs to be multiplied by 0.00028,
    # which is the number denoting the default real world size representation
    # of a pixel. In the default case the scale denominator is 750.
    # With the calculation of 750 * 0.00028 = 0.21 we reach the size in meters
    # one pixel represents.
    zoom = "14"
    # Next the part of the url common to all WMTS request will be determined.
    common_url = midprocess_url()
    # Next set the pixel width and height that a single response from the WMTS
    # This data is derived from the WMTS at the given zoom lvl
    url_open = wmts(source_url)
    tileheight = url_open.tilematrixsets[crs].tilematrix[zoom].tileheight
    tilewidth = url_open.tilematrixsets[crs].tilematrix[zoom].tilewidth
    # Next is an identifier for this specific request, used for filenames.
    name = "swf"
    # The name will be used for the final filename
    filename = f"ndvi_{name}_new.tiff"
    # Next is the bounding box of the request
    bbox_request = (140032.13, 534299.47, 181862.694, 574122.007)
    # Next is the chunksize that will be encoded in the resulting geotiff.
    # The set value of this may be user preference.
    chunksize = 256

    asyncio.run(main())
    print("execution sucessfull; time eplased till end of script\t : "
          + f"{time.time() - start:,.2f}")
    print(f"resulting file: {filename} can be found at: {outdir}")

    # some bbox (test) cases for the default service can be found below:
    # bbox_bolsward = (162995.2992, 563126.3802, 165494.1228, 565237.1188)
    # bbox_sneekenmeer = (170233.1786, 554948.3689, 184250.9775, 562942.3701)
    # bbox_julianapark = (164477.7017,563458.7532,164896.2459,563859.1646)
    # bbox_swf = (140032.13 , 534299.47 , 181862.694, 574122.007)
