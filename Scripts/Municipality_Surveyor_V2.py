# -*- coding: utf-8 -*-
"""
## BESTE SCRIPT ##
Title: "Municipality_Surveyor.py"

Script that fetches CBS buurten for Súdwest-Fryslân and sequentially 
retrieves infrared (CIR) aerial imagery for each neighborhood.
Designed for 16GB RAM / MX450 environments.

Built upon the logic of "Buurt_Sampler.py" and "NDVI_Retriever_V2.py".
"""

import os
import re
import math
import time
import asyncio
import gc
import argparse
from pathlib import Path
from io import BytesIO
import urllib.parse

import numpy as np
import geopandas as gpd
import aiohttp
import rasterio as rio
from owslib.wmts import WebMapTileService as wmts
from owslib.wcs import WebCoverageService as wcs_client
from owslib.wms import WebMapService
from PIL import Image

# Configuration
MUNICIPALITY_NAME = "Súdwest-Fryslân"
MUNICIPALITY_CODE = "GM1900"
WFS_URL = "https://service.pdok.nl/cbs/wijkenbuurten/2023/wfs/v1_0"
CIR_WMTS_URL = "https://service.pdok.nl/hwh/luchtfotocir/wmts/v1_0?request=GetCapabilities&service=wmts"
RGB_WMTS_URL = "https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0?request=GetCapabilities&service=wmts"
AHN_WCS_URL = "https://service.pdok.nl/rws/ahn/wcs/v1_0"
BRT_WMTS_URL = "https://service.pdok.nl/brt/top10nl/wmts/v1_0?request=GetCapabilities&service=wmts"
BRT_A_WMTS_URL = "https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0?request=getcapabilities&service=wmts"
BGT_WMTS_URL = "https://service.pdok.nl/lv/bgt/wmts/v1_0?request=GetCapabilities&service=WMTS"
KEA_WMS_URL = "https://cas.cloud.sogelink.com/public/data/org/gws/YWFMLMWERURF/kea_public/wms?service=wms&request=getcapabilities"
CIR_DATASET_ID = "2024_ortho25IR"
RGB_DATASET_ID = "2024_ortho25"
BRT_A_DATASET_ID = "standaard"
BGT_DATASET_ID = "standaardvisualisatie"
KEA_DATASET_IDS = ["gevoelstemperatuur_2022", "RMK_BKB_AHN4", "RMK_SchaduwGrijs_AHN4", "RMK_SchaduwGroen_AHN4", "BGV_ONO_2024", "BGV_landbedekking_2024", "RMK_30percBKB_AHN4"] 
ZOOM = "14"
CRS_EPSG = "EPSG:28992"

class MunicipalitySurveyor:
    def __init__(self, outdir, overwrite=False):
        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.overwrite = overwrite
        self.cir_url = None
        self.rgb_url = None
        self.brt_url = None
        self.tileheight = None
        self.tilewidth = None
        self.bbox_source = None
        self.wmts_client = None
        self.kea_client = None
        self.extension = ".jpg"
        self.brt_dataset_id = "top10nl"

    def fetch_buurten(self):
        """Fetch all buurten for the municipality using a server-side filter."""
        print(f"Fetching all buurten for {MUNICIPALITY_NAME} ({MUNICIPALITY_CODE})...")
        import requests
        
        # OGC Filter for GM1900 and land-only (water=NEE)
        ogc_filter = f"""
        <ogc:Filter xmlns:ogc='http://www.opengis.net/ogc'>
            <ogc:And>
                <ogc:PropertyIsEqualTo>
                    <ogc:PropertyName>gemeentecode</ogc:PropertyName>
                    <ogc:Literal>{MUNICIPALITY_CODE}</ogc:Literal>
                </ogc:PropertyIsEqualTo>
                <ogc:PropertyIsEqualTo>
                    <ogc:PropertyName>water</ogc:PropertyName>
                    <ogc:Literal>NEE</ogc:Literal>
                </ogc:PropertyIsEqualTo>
            </ogc:And>
        </ogc:Filter>
        """
        encoded_filter = urllib.parse.quote(ogc_filter.strip())
        url = f"{WFS_URL}?request=GetFeature&service=WFS&version=2.0.0&typeName=wijkenbuurten:buurten&outputFormat=application/json&filter={encoded_filter}"
        
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch buurten: {response.status_code}")
            
        df = gpd.read_file(BytesIO(response.content))
        
        if df.empty:
            print(f"Error: No land-based neighborhoods found for {MUNICIPALITY_CODE}.")
            return df
            
        print(f"Found {len(df)} land-based buurten for {MUNICIPALITY_NAME}.")
        return df

    def initialize_wmts(self):
        """Initialize WMTS client and parameters for both CIR and RGB."""
        print("Initializing CIR WMTS service...")
        client = wmts(CIR_WMTS_URL)
        layer = client[CIR_DATASET_ID]
        template = layer.resourceURLs[0]["template"]
        self.extension = ".jpeg" if ".jpeg" in template else ".jpg"
        
        url_cir = re.sub("{TileMatrixSet}", CRS_EPSG, template)
        url_cir = re.sub("{TileMatrix}", ZOOM, url_cir)
        self.cir_url = re.sub(r"/{TileCol}/{TileRow}\.(jpeg|jpg)", "", url_cir)
        
        print("Initializing RGB WMTS service...")
        rgb_client = wmts(RGB_WMTS_URL)
        rgb_template = rgb_client[RGB_DATASET_ID].resourceURLs[0]["template"]
        url_rgb = re.sub("{TileMatrixSet}", CRS_EPSG, rgb_template)
        url_rgb = re.sub("{TileMatrix}", ZOOM, url_rgb)
        self.rgb_url = re.sub(r"/{TileCol}/{TileRow}\.(jpeg|jpg)", "", url_rgb)

        matrix = client.tilematrixsets[CRS_EPSG].tilematrix[ZOOM]
        self.tileheight = matrix.tileheight
        self.tilewidth = matrix.tilewidth

        print("Initiatlizing BRT WMTS service...")
        brt_client = wmts(BRT_WMTS_URL)
        # Usually, the layer ID is 'top10nl' or similar depening on the capabilities
        brt_template = brt_client[self.brt_dataset_id].resourceURLs[0]["template"]
        url_brt = re.sub("{TileMatrixSet}", CRS_EPSG, brt_template)
        url_brt = re.sub("{TileMatrix}", ZOOM, url_brt)
        self.brt_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg)", "", url_brt)

        # Note BRT is often PNG, so we'll handle the extension dynamically in download_area
        self.brt_extension = ".png" if ".png" in brt_template else ".jpg"

        print("Initializing BRT-A Background service...")
        brta_client = wmts(BRT_A_WMTS_URL)
        brta_template = brta_client[BRT_A_DATASET_ID].resourceURLs[0]["template"]
        url_brta = re.sub("{TileMatrixSet}", CRS_EPSG, brta_template)
        url_brta = re.sub("{TileMatrix}", ZOOM, url_brta)
        self.brta_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg)", "", url_brta)
        self.brta_extension = ".png" if ".png" in brta_template else ".jpg"

        print("Initializing BGT WMTS service...")
        bgt_client = wmts(BGT_WMTS_URL)
        bgt_layer = bgt_client[BGT_DATASET_ID]
        bgt_template = bgt_layer.resourceURLs[0]["template"]
        url_bgt = re.sub("{TileMatrixSet}", CRS_EPSG, bgt_template)
        url_bgt = re.sub("{TileMatrix}", ZOOM, url_bgt)
        self.bgt_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg)", "", url_bgt)
        self.bgt_extension = ".png" if ".png" in bgt_template else ".jpg"

        # Calculate source bbox for EPSG:28992
        x_tile_amount = matrix.matrixheight
        y_tile_amount = matrix.matrixwidth
        x_min, y_max = matrix.topleftcorner
        # Resolution calculation (0.00028 is the standard pixel size in meters for WMTS)
        dist_x = matrix.scaledenominator * self.tileheight * 0.00028 * x_tile_amount
        dist_y = matrix.scaledenominator * self.tilewidth * 0.00028 * y_tile_amount
        x_max = x_min + dist_x
        y_min = y_max - dist_y
        self.bbox_source = (x_min, y_min, x_max, y_max)
        self.wmts_client = client
        print(f"WMTS initialized. Resolution: {matrix.scaledenominator * 0.00028:.2f}m/px")
    
    def initialize_wms(self):
        print("Initializing KEA WMS service...")
        print(f"Connecting to Sogelink Server for {KEA_DATASET_IDS}...")
        try:
            # Sogelink works well with version 1.3.0
            self.kea_client = WebMapService(KEA_WMS_URL, version='1.3.0', timeout=60)
            
            if self.kea_client and hasattr(self.kea_client, 'contents'):
                for layer_id in KEA_DATASET_IDS:
                    if layer_id in self.kea_client.contents:
                        print(f" SUCCESS: Layer '{layer_id}' found.")
                    else:
                        print(f" Warning: Layer '{layer_id}' not found.")
                else:
                    print(" ERROR: Could not retrieve capabilities.")
        except Exception as e:
            print(f" CONNECTION FAILED: {e}")
            self.kea_client = None

    def get_tile_index_from_coords(self, x_coord, y_coord):
        x_min, y_min, x_max, y_max = self.bbox_source
        dist_to_x = x_coord - x_min
        dist_to_y = y_max - y_coord
        
        matrix = self.wmts_client.tilematrixsets[CRS_EPSG].tilematrix[ZOOM]
        tile_width_coord = (x_max - x_min) / matrix.matrixwidth
        tile_height_coord = (y_max - y_min) / matrix.matrixheight
        
        x_tile_pos = math.ceil(dist_to_x / tile_width_coord)
        y_tile_pos = math.ceil(dist_to_y / tile_height_coord)
        return x_tile_pos - 1, y_tile_pos - 1

    def get_index_bounds(self, bbox_inner):
        x_lower, y_lower, x_upper, y_upper = bbox_inner
        x_idx_l, y_idx_l = self.get_tile_index_from_coords(x_lower, y_upper)
        x_idx_u, y_idx_u = self.get_tile_index_from_coords(x_upper, y_lower)
        return range(x_idx_l, x_idx_u + 1), range(y_idx_l, y_idx_u + 1)

    def get_bbox_of_tile_indices(self, x_range, y_range):
        """Calculate the world-coordinate bounding box of the specified tile ranges."""
        x_min_s, y_min_s, x_max_s, y_max_s = self.bbox_source
        
        matrix = self.wmts_client.tilematrixsets[CRS_EPSG].tilematrix[ZOOM]
        tile_width_coord = (x_max_s - x_min_s) / matrix.matrixwidth
        tile_height_coord = (y_max_s - y_min_s) / matrix.matrixheight
        
        x_lower = x_min_s + (x_range[0] * tile_width_coord)
        x_upper = x_min_s + ((x_range[-1] + 1) * tile_width_coord)
        y_upper = y_max_s - (y_range[0] * tile_height_coord)
        y_lower = y_max_s - ((y_range[-1] + 1) * tile_height_coord)
        
        return x_lower, y_lower, x_upper, y_upper

    async def fetch_tile(self, session, base_url, r_pos, c_pos, semaphore, ext):
        async with semaphore:
            url = f"{base_url}/{c_pos}/{r_pos}{ext}"
            try:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    return None
            except Exception:
                return None

    async def download_area(self, x_range, y_range):
        height = len(y_range) * self.tileheight
        width = len(x_range) * self.tilewidth
        
        cir_array = np.zeros((height, width, 3), dtype=np.uint8)
        rgb_array = np.zeros((height, width, 3), dtype=np.uint8)
        brt_array = np.zeros((height, width, 3), dtype=np.uint8)
        brta_array = np.zeros((height, width, 3), dtype=np.uint8)
        bgt_array = np.zeros((height, width, 3), dtype=np.uint8)
        
        semaphore = asyncio.Semaphore(15) 
        
        async with aiohttp.ClientSession() as session:
            print(f"  Downloading tiles for area ({width}x{height} pixels)...")
            cir_tasks = [self.fetch_tile(session, self.cir_url, r, c, semaphore, self.extension) for r in y_range for c in x_range]
            rgb_tasks = [self.fetch_tile(session, self.rgb_url, r, c, semaphore, self.extension) for r in y_range for c in x_range]
            brt_tasks = [self.fetch_tile(session, self.brt_url, r, c, semaphore, self.brt_extension) for r in y_range for c in x_range]
            brta_tasks = [self.fetch_tile(session, self.brta_url, r, c, semaphore, self.brta_extension) for r in y_range for c in x_range]
            bgt_tasks = [self.fetch_tile(session, self.bgt_url, r, c, semaphore, self.bgt_extension) for r in y_range for c in x_range]
            
            cir_results = await asyncio.gather(*cir_tasks)
            rgb_results = await asyncio.gather(*rgb_tasks)
            brt_results = await asyncio.gather(*brt_tasks)
            brta_results = await asyncio.gather(*brta_tasks)
            bgt_results = await asyncio.gather(*bgt_tasks)
            
            for idx, (y_idx, x_idx) in enumerate([(y, x) for y in range(len(y_range)) for x in range(len(x_range))]):
                r_start, c_start = y_idx * self.tileheight, x_idx * self.tilewidth
                if cir_results[idx]:
                    cir_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = np.array(Image.open(BytesIO(cir_results[idx])))
                if rgb_results[idx]:
                    rgb_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = np.array(Image.open(BytesIO(rgb_results[idx])))
                if brt_results[idx]:
                    img = Image.open(BytesIO(brt_results[idx])).convert('RGB')
                    brt_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = np.array(img)
                if brta_results[idx]:
                    img = Image.open(BytesIO(brta_results[idx])).convert('RGB')
                    brta_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = np.array(img)
                if bgt_results[idx]:
                    img_bgt = Image.open(BytesIO(bgt_results[idx])).convert('RGB')
                    bgt_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = np.array(img_bgt)
                
        return cir_array, rgb_array, brt_array, brta_array, bgt_array

    def download_height_data(self, bbox, width, height, layer='dsm_05m'):
        """Fetch height data from PDOK WCS and upsample to match imagery dimensions.
        
        Returns a masked numpy array where nodata pixels are masked out.
        """
        print(f"  Downloading AHN {layer} ({width}x{height} pixels after upsampling)...")
        wcs = wcs_client(AHN_WCS_URL, version='1.0.0')
        
        try:
            response = wcs.getCoverage(
                identifier=layer,
                bbox=bbox,
                format='GEOTIFF',
                crs=CRS_EPSG,
                resx=0.5, resy=0.5
            )
            
            raw_content = response.read()
            if not raw_content.startswith(b'II\x2a') and not raw_content.startswith(b'MM\x00\x2a'):
                print(f"  Warning: WCS response for {layer} may not be a GeoTIFF.")
            
            with rio.MemoryFile(raw_content) as memfile:
                with memfile.open() as src:
                    src_nodata = src.nodata
                    data = src.read(
                        1,
                        out_shape=(height, width),
                        resampling=rio.enums.Resampling.bilinear
                    ).astype(np.float32)
            
            # Build a proper mask from the source nodata value
            # float32 max (~3.4e38) is used by AHN as nodata when nodata tag is None
            if src_nodata is not None:
                mask = (data == src_nodata)
            else:
                # Detect suspiciously large floats as nodata
                mask = (data > 1e30) | (data < -1e30)
            
            nodata_count = np.sum(mask)
            if nodata_count > 0:
                print(f"  Found {nodata_count} nodata pixels in {layer}, masking them.")
            
            # Return a masked array so arithmetic (CHM = DSM - DTM) propagates the mask
            return np.ma.array(data, mask=mask)
            
        except Exception as e:
            print(f"  Error downloading {layer}: {e}")
            return np.ma.array(np.zeros((height, width), dtype=np.float32), mask=True)
    
    def download_wms_data(self, bbox, width, height, layer):
        if not self.kea_client:
            return np.zeros((height, width, 4), dtype=np.uint8)

        # Create an empty canvas for the full neighborhood
        full_map = np.zeros((height, width, 4), dtype=np.uint8)
        
        # Split into a 2x2 grid (4 chunks) to bypass server memory limits
        rows, cols = 2, 2
        chunk_w = width // cols
        chunk_h = height // rows
        
        x_min, y_min, x_max, y_max = bbox
        dx = (x_max - x_min) / cols
        dy = (y_max - y_min) / rows

        print(f"  Area too large for server. Splitting into {rows*cols} chunks...")

        for i in range(rows):
            for j in range(cols):
                # Calculate BBOX for this chunk
                c_xmin = x_min + (j * dx)
                c_xmax = x_min + ((j + 1) * dx)
                c_ymin = y_max - ((i + 1) * dy) # WMS Y is usually top-down in calculation
                c_ymax = y_max - (i * dy)
                
                chunk_bbox = (c_xmin, c_ymin, c_xmax, c_ymax)
                
                try:
                    response = self.kea_client.getmap(
                        layers=[layer],
                        srs=CRS_EPSG,
                        bbox=chunk_bbox,
                        size=(chunk_w, chunk_h),
                        format='image/png',
                        transparent=True
                    )
                    
                    img = Image.open(BytesIO(response.read())).convert('RGBA')
                    img_arr = np.array(img.resize((chunk_w, chunk_h)))
                    
                    # Place chunk into the full canvas
                    y_start, x_start = i * chunk_h, j * chunk_w
                    full_map[y_start:y_start+chunk_h, x_start:x_start+chunk_w] = img_arr
                    
                except Exception as e:
                    print(f"    Chunk {i},{j} failed: {e}")

        return full_map

    def run(self, limit=None, targets=None, include_height=True):
        self.initialize_wmts()
        self.initialize_wms() 
        df_buurten = self.fetch_buurten()
        
        if df_buurten.empty:
            return

        if targets:
            print(f"Filtering for specified neighborhoods: {', '.join(targets)}")
            targets_lower = [t.lower().strip() for t in targets]
            name_col = 'buurtnaam' if 'buurtnaam' in df_buurten.columns else 'buurtname'
            
            mask = df_buurten['buurtcode'].str.lower().isin(targets_lower) | \
                   df_buurten[name_col].str.lower().isin(targets_lower)
            
            found_df = df_buurten[mask]
            found_codes = found_df['buurtcode'].str.lower().tolist()
            found_names = found_df[name_col].str.lower().tolist()
            
            not_found = [t for t in targets if t.lower() not in found_codes and t.lower() not in found_names]
            
            if not_found:
                print(f"Warning: The following targets were not found: {', '.join(not_found)}")
            
            df_buurten = found_df
            if df_buurten.empty:
                print("No matching neighborhoods found. Exiting.")
                return

        total = len(df_buurten)
        if limit and not targets: # Limit only applies if no targets are specified
            print(f"Limiting to first {limit} neighborhoods for testing.")
            df_buurten = df_buurten.iloc[:limit]
            total = limit

        name_col = 'buurtnaam' if 'buurtnaam' in df_buurten.columns else 'buurtname'
        
        for i, (idx, row) in enumerate(df_buurten.iterrows()):
            buurt_name = row[name_col]
            buurt_code = row['buurtcode']
            base_filename = f"{buurt_code}_{buurt_name.replace(' ', '_')}"
            
            output_cir = self.outdir / f"{base_filename}_CIR.tif"
            output_rgb = self.outdir / f"{base_filename}_RGB.tif"
            output_ndvi = self.outdir / f"{base_filename}_NDVI.tif"
            output_dsm = self.outdir / f"{base_filename}_DSM.tif"
            output_dtm = self.outdir / f"{base_filename}_DTM.tif"
            output_chm = self.outdir / f"{base_filename}_CHM.tif"
            output_brt = self.outdir / f"{base_filename}_BRT.tif"
            output_brta = self.outdir / f"{base_filename}_BRTA.tif"
            output_bgt = self.outdir / f"{base_filename}_BGT.tif"
            #output_kea = self.outdir / f"{base_filename}_KEA.tif"
            
            # Check if all targeted files exist
            targets_exist = output_cir.exists() and output_rgb.exists() and output_ndvi.exists()
            if include_height:
                targets_exist = targets_exist and output_dsm.exists() and output_dtm.exists() and output_chm.exists()

            if targets_exist and not self.overwrite:
                print(f"[{i+1}/{total}] Skipping {buurt_name} (All files exist).")
                continue

            print(f"[{i+1}/{total}] Processing {buurt_name} ({buurt_code})...")
            bbox = row.geometry.bounds 
            x_range, y_range = self.get_index_bounds(bbox)
            bbox_tiles = self.get_bbox_of_tile_indices(x_range, y_range)
            
            # Simple RAM safety check (now with RGB and CIR)
            total_pixels = (len(x_range) * self.tilewidth) * (len(y_range) * self.tileheight)
            if total_pixels > 200_000_000: # ~1.2GB for 6-band uint8 + processing
                print(f"WARNING: {buurt_name} is very large ({total_pixels} pixels).")

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                cir_data, rgb_data, brt_data, brta_data, bgt_data = loop.run_until_complete(self.download_area(x_range, y_range))
                loop.close()

                # NDVI Calc
                print("  Calculating NDVI...")
                nir = cir_data[:, :, 0].astype(np.float32)
                red = cir_data[:, :, 1].astype(np.float32)
                # Formula: (NIR - Red) / (NIR + Red)
                ndvi = np.divide(nir - red, nir + red, out=np.zeros_like(nir), where=(nir + red) != 0)
                # Rescale to 0-200 uint8
                ndvi_uint8 = np.round((ndvi + 1) * 100).astype(np.uint8)
                
                # Metadata
                common_meta = {
                    'driver': 'GTiff', 'height': cir_data.shape[0], 'width': cir_data.shape[1],
                    'crs': CRS_EPSG, 'transform': rio.transform.from_bounds(*bbox_tiles, cir_data.shape[1], cir_data.shape[0]),
                    'compress': 'lzw', 'tiled': True, 'blockxsize': 256, 'blockysize': 256, 'nodata': 255
                }
                
                # Save CIR
                with rio.open(output_cir, 'w', count=3, dtype=np.uint8, **common_meta) as dst:
                    for b in range(3): dst.write(cir_data[:, :, b], b + 1)
                
                # Save RGB
                with rio.open(output_rgb, 'w', count=3, dtype=np.uint8, **common_meta) as dst:
                    for b in range(3): dst.write(rgb_data[:, :, b], b + 1)
                
                # Save BRT
                brt_meta = common_meta.copy()
                brt_meta.update({
                   'nodata': None,
                   'photometric': 'rgb',
                   'interleave': 'pixel'
                })
                
                with rio.open(output_brt, 'w', count=3, dtype=np.uint8, **common_meta) as dst:
                    for b in range(3): dst.write(brt_data[:, :, b], b + 1)

                brta_meta = common_meta.copy()
                brta_meta.update({
                    'nodata': None,
                    'photometric': 'rgb',
                    'interleave': 'pixel'
                    })

                with rio.open(output_brta, 'w', count=3, dtype=np.uint8, **brta_meta) as dst:
                    for b in range(3): dst.write(brta_data[:, :, b], b + 1)

                # Save BGT
                map_meta = common_meta.copy()
                map_meta.update({
                    'nodata': None,           # Windows often renders 255 as transparent/black
                    'photometric': 'rgb',     # Essential for Windows compatibility
                    'interleave': 'pixel'     # Better for standard image viewers
                    })

                with rio.open(output_bgt, 'w', count=3, dtype=np.uint8, **map_meta) as dst:
                    for b in range(3): dst.write(bgt_data[:, :, b], b + 1)
                """
                # Save KEA metadata
                kea_meta = common_meta.copy()
                kea_meta.update({'nodata': None, 'photometric': 'rgb', 'interleave': 'pixel'})
                
                with rio.open(output_kea, 'w', count=3, dtype=np.uint8, **kea_meta) as dst:
                    for b in range(3): dst.write(kea_data[:, :, b], b + 1)
                """
                
                # KEA data
                # KEA data - Download and save each layer separately
                print(f"  Downloading {len(KEA_DATASET_IDS)} KEA WMS layers...")
                h, w = cir_data.shape[0], cir_data.shape[1]
                
                kea_meta = common_meta.copy()
                kea_meta.update({'count':4, 'nodata': None, 'interleave': 'pixel'})

                for layer_id in KEA_DATASET_IDS:
                    # 1. Download the specific layer
                    kea_data = self.download_wms_data(bbox_tiles, w, h, layer_id)
                    
                    # 2. Define a unique filename for this layer
                    output_kea_layer = self.outdir / f"{base_filename}_KEA_{layer_id}.tif"
                    
                    # 3. Save it
                    with rio.open(output_kea_layer, 'w', dtype=np.uint8, **kea_meta) as dst:
                        for b in range(4): 
                            dst.write(kea_data[:, :, b], b + 1)
                    
                    print(f"    Saved KEA layer: {layer_id}")
                    del kea_data # Clean up memory for each layer

                
                # Save NDVI
                with rio.open(output_ndvi, 'w', count=1, dtype=np.uint8, **common_meta) as dst:
                    dst.write(ndvi_uint8, 1)

                # Height Data
                if include_height:
                    dsm = self.download_height_data(bbox_tiles, cir_data.shape[1], cir_data.shape[0], layer='dsm_05m')
                    dtm = self.download_height_data(bbox_tiles, cir_data.shape[1], cir_data.shape[0], layer='dtm_05m')
                    
                    # CHM = DSM - DTM. Masked arrays propagate the mask automatically.
                    # Clip to 0 minimum: DSM can be slightly below DTM due to interpolation artifacts.
                    chm = np.ma.maximum(dsm - dtm, 0)
                    
                    height_meta = common_meta.copy()
                    height_meta.update({'dtype': np.float32, 'nodata': -9999.0, 'count': 1})
                    
                    # Fill masked (nodata) regions with -9999 before saving
                    dsm_filled = dsm.filled(-9999.0)
                    dtm_filled = dtm.filled(-9999.0)
                    chm_filled = chm.filled(-9999.0)
                    
                    with rio.open(output_dsm, 'w', **height_meta) as dst: dst.write(dsm_filled, 1)
                    with rio.open(output_dtm, 'w', **height_meta) as dst: dst.write(dtm_filled, 1)
                    with rio.open(output_chm, 'w', **height_meta) as dst: dst.write(chm_filled, 1)
                    
                    valid_chm = chm.compressed()
                    if len(valid_chm) > 0:
                        print(f"  CHM stats: min={valid_chm.min():.2f}m, max={valid_chm.max():.2f}m, mean={valid_chm.mean():.2f}m")
                    print(f"  Successfully saved height datasets for {buurt_name}")
                    del dsm, dtm, chm, dsm_filled, dtm_filled, chm_filled

                print(f"  Successfully saved triple datasets for {buurt_name}")
                
                # Explicit cleanup
                del cir_data, rgb_data, nir, red, ndvi, ndvi_uint8
                gc.collect()
                
            except Exception as e:
                print(f"Error processing {buurt_name}: {e}")

        print(f"\nFinished processing {total} neighborhoods.")
        print(f"Outputs are located in: {self.outdir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Surveyor for Súdwest-Fryslân Infrared Imagery")
    parser.add_argument("--limit", type=int, default=2, help="Limit the number of neighborhoods (default: 2 for testing)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--outdir", type=str, default="outdir/municipality_survey_V2", help="Output directory")
    parser.add_argument("--buurten", nargs="+", help="Specific neighborhood codes or names to process")
    parser.add_argument("--file", type=str, default="targets.txt", help="Path to a text file containing neighborhood codes or names (one per line)")
    parser.add_argument("--no-height", action="store_false", dest="height", help="Disable height data retrieval")
    parser.set_defaults(height=True)
    
    args = parser.parse_args()
    
    targets = []
    if args.buurten:
        targets.extend(args.buurten)
    if args.file:
        file_path = Path(args.file)
        if file_path.exists():
            with open(file_path, 'r') as f:
                targets.extend([line.strip() for line in f if line.strip()])
            print(f"Loaded {len(targets)} targets from {args.file}")
        elif args.file != "targets.txt":
            print(f"Error: File {args.file} not found.")
            exit(1)

    start_time = time.time()
    surveyor = MunicipalitySurveyor(args.outdir, overwrite=args.overwrite)
    surveyor.run(limit=args.limit if args.limit > 0 else None, targets=targets if targets else None, include_height=args.height)
    
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")
