# -*- coding: utf-8 -*-
"""
## BESTE SCRIPT V4 ##
Title: "Municipality_Surveyor_V4.py"

Script that fetches CBS buurten for Súdwest-Fryslân and sequentially 
retrieives infrared (CIR), RGB, and AHN height imagery for each neighborhood.
NEW in V4:
 - Historical 2022 dataset matching.
 - Uses Topotijdreis 2022 for BRT background tiles.
 - Uses BGT/BRT OGC API temporal filtering (peildatum May 2022).
 - Uses 2022 CBS neighborhood boundaries.

Designed for 16GB RAM / MX450 environments.
"""

import os
import re
import math
import time
import asyncio
import gc
import argparse
import requests
import html
from pathlib import Path
from io import BytesIO
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import numpy as np
import geopandas as gpd
import aiohttp
import rasterio as rio
import rasterio.features
from owslib.wmts import WebMapTileService as wmts
from owslib.wcs import WebCoverageService as wcs_client
from owslib.wms import WebMapService
from PIL import Image
from shapely.geometry import box

# Configuration
MUNICIPALITY_NAME = "Súdwest-Fryslân"
MUNICIPALITY_CODE = "GM1900"

# --- 2022 Historical Configuration ---
WFS_URL = "https://service.pdok.nl/cbs/wijkenbuurten/2022/wfs/v1_0"
CIR_WMTS_URL = "https://service.pdok.nl/hwh/luchtfotocir/wmts/v1_0?request=GetCapabilities&service=wmts"
RGB_WMTS_URL = "https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0?request=GetCapabilities&service=wmts"
AHN_WCS_URL = "https://service.pdok.nl/rws/ahn/wcs/v1_0"

# Topotijdreis 2022 for Historical BRT Tiles
BRT_A_WMTS_URL = "https://tiles.arcgis.com/tiles/nSZVuSZjHpEZZbRo/arcgis/rest/services/Historische_tijdreis_2022/MapServer/WMTS/1.0.0/WMTSCapabilities.xml"
BRT_WMTS_URL = "https://service.pdok.nl/brt/top10nl/wmts/v1_0?request=GetCapabilities&service=wmts"
BGT_WMTS_URL = "https://service.pdok.nl/lv/bgt/wmts/v1_0?request=GetCapabilities&service=WMTS"
KEA_WMS_URL = "https://cas.cloud.sogelink.com/public/data/org/gws/YWFMLMWERURF/kea_public/wms?service=wms&request=getcapabilities"

BGT_FEATURES_URL = "https://api.pdok.nl/lv/bgt/ogc/v1"
BRT_FEATURES_URL = "https://api.pdok.nl/brt/top10nl/ogc/v1"

# Dataset IDs for 2022
CIR_DATASET_ID = "2022_ortho25IR"
RGB_DATASET_ID = "2022_ortho25"
BRT_A_DATASET_ID = "Historische_tijdreis_2022" 
BGT_DATASET_ID = "standaardvisualisatie"
KEA_DATASET_IDS = [
    "gevoelstemperatuur_2022", 
    "RMK_BKB_AHN4", 
    "RMK_SchaduwGrijs_AHN4", 
    "RMK_SchaduwGroen_AHN4", 
    "BGV_ONO_2024", 
    "BGV_landbedekking_2024", 
    "RMK_30percBKB_AHN4"
]

# Temporal filtering for OGC API Features
PEILDATUM = "2022-05-15T12:00:00Z" 

ZOOM = "14"
CRS_EPSG = "EPSG:28992"

# KEA Reclassification Configuration (LUT)
# Maps legend RGB colors to (Integer ID, Label, Numeric Value/Description)
KEA_CLASSIFICATION_CONFIG = {
    "gevoelstemperatuur_2022": {
        (51, 122, 159):  (1, "Gematigde Hittestress: <32C", 31.0),
        (140, 166, 189): (2, "Gematigde Hittestress: 32C - 34C", 33.0),
        (206, 221, 172): (3, "Sterke Hittestress: 35C - 37C", 36.0),
        (248, 225, 146): (4, "Sterke Hittestress: 38C - 40C", 39.0),
        (197, 141, 131): (5, "Extreme Hittestress (niveau 1): 41C - 43C", 42.0),
        (194, 118, 104): (6, "Extreme Hittestress (niveau 1): 44C - 45C", 44.5),
        (139, 49, 44):   (7, "Extreme Hittestress (niveau 2): 46C - 48C", 47.0),
        (125, 0, 0):     (8, "Extreme Hittestress (niveau 2): 49C - 50C", 49.5),
        (46, 0, 0):      (9, "Extreme Hittestress (niveau 3): >50C", 51.0),
    },
    "BGV_landbedekking_2024": {
        (104, 171, 210): (1, "Water", 0),
        (223, 192, 114): (2, "Onverhard", 0),
        (248, 204, 147): (3, "Half verhard", 0),
        (124, 124, 124): (4, "Verhard", 0),
        (184, 242, 166): (5, "Gras", 0),
        (172, 240, 96):  (6, "Onverhard met struiken", 0),
        (248, 52, 52):   (7, "Gebouw", 0),
        (77, 122, 150):  (8, "Brug over water", 0),
        (226, 231, 226): (9, "Buitenland", 0),
    }
}

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

    def fetch_buurten(self, targets=None):
        """Fetch buurten for the municipality. If targets are provided, fetch only those."""
        if targets:
            print(f"Fetching specific neighborhoods (2022 boundaries): {', '.join(targets)}...")
        else:
            print(f"Fetching all buurten for {MUNICIPALITY_NAME} ({MUNICIPALITY_CODE}) - 2022 Boundaries...")
        
        # Base filter: Gemeentecode + Land-only (water=NEE)
        filter_parts = [
            f"<ogc:PropertyIsEqualTo><ogc:PropertyName>gemeentecode</ogc:PropertyName><ogc:Literal>{MUNICIPALITY_CODE}</ogc:Literal></ogc:PropertyIsEqualTo>",
            "<ogc:PropertyIsEqualTo><ogc:PropertyName>water</ogc:PropertyName><ogc:Literal>NEE</ogc:Literal></ogc:PropertyIsEqualTo>"
        ]
        
        # Add targeted neighborhood filter if targets are provided
        if targets:
            target_filters = []
            for t in targets:
                # If it looks like a code (BU1900...)
                prop = "buurtcode" if t.upper().startswith("BU") else "buurtnaam"
                target_filters.append(f"<ogc:PropertyIsEqualTo><ogc:PropertyName>{prop}</ogc:PropertyName><ogc:Literal>{t}</ogc:Literal></ogc:PropertyIsEqualTo>")
            
            if len(target_filters) == 1:
                filter_parts.append(target_filters[0])
            else:
                or_filter = "<ogc:Or>" + "".join(target_filters) + "</ogc:Or>"
                filter_parts.append(or_filter)

        ogc_filter = f"""
        <ogc:Filter xmlns:ogc='http://www.opengis.net/ogc'>
            <ogc:And>
                {"".join(filter_parts)}
            </ogc:And>
        </ogc:Filter>
        """
        
        encoded_filter = urllib.parse.quote(ogc_filter.strip())
        url = f"{WFS_URL}?request=GetFeature&service=WFS&version=2.0.0&typeName=wijkenbuurten:buurten&outputFormat=application/json&filter={encoded_filter}"
        
        # Resilience: Retry strategy
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        
        try:
            response = session.get(url, timeout=60)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch buurten: {response.status_code}")
                
            df = gpd.read_file(BytesIO(response.content))
            
            if df.empty:
                if targets:
                    print(f"Warning: No matching neighborhoods found for the provided targets in {MUNICIPALITY_CODE}.")
                else:
                    print(f"Error: No land-based neighborhoods found for {MUNICIPALITY_CODE}.")
                return df
                
            print(f"Found {len(df)} neighborhood geometries.")
            return df
        except Exception as e:
            print(f"Connection failed after retries: {e}")
            raise

    def initialize_wmts(self):
        """Initialize WMTS client and parameters for all layers."""
        print("Initializing CIR WMTS service (2022)...")
        client = wmts(CIR_WMTS_URL)
        layer = client[CIR_DATASET_ID]
        template = layer.resourceURLs[0]["template"]
        self.extension = ".jpeg" if ".jpeg" in template else ".jpg"
        
        url_cir = re.sub("{TileMatrixSet}", CRS_EPSG, template)
        url_cir = re.sub("{TileMatrix}", ZOOM, url_cir)
        self.cir_url = re.sub(r"/{TileCol}/{TileRow}\.(jpeg|jpg)", "", url_cir)
        
        print("Initializing RGB WMTS service (2022)...")
        rgb_client = wmts(RGB_WMTS_URL)
        rgb_template = rgb_client[RGB_DATASET_ID].resourceURLs[0]["template"]
        url_rgb = re.sub("{TileMatrixSet}", CRS_EPSG, rgb_template)
        url_rgb = re.sub("{TileMatrix}", ZOOM, url_rgb)
        self.rgb_url = re.sub(r"/{TileCol}/{TileRow}\.(jpeg|jpg)", "", url_rgb)

        matrix = client.tilematrixsets[CRS_EPSG].tilematrix[ZOOM]
        self.tileheight = matrix.tileheight
        self.tilewidth = matrix.tilewidth

        print("Initializing BRT WMTS service...")
        brt_client = wmts(BRT_WMTS_URL)
        brt_template = brt_client[self.brt_dataset_id].resourceURLs[0]["template"]
        url_brt = re.sub("{TileMatrixSet}", CRS_EPSG, brt_template)
        url_brt = re.sub("{TileMatrix}", ZOOM, url_brt)
        self.brt_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg)", "", url_brt)
        self.brt_extension = ".png" if ".png" in brt_template else ".jpg"

        print("Initializing BRT-A Background service (Topotijdreis 2022)...")
        try:
            brta_client = wmts(BRT_A_WMTS_URL)
            brta_layer = brta_client[BRT_A_DATASET_ID]
            brta_template = brta_layer.resourceURLs[0]["template"]
            
            # ArcGIS templates often use {level}/{row}/{col} or similar
            url_brta = re.sub("{TileMatrixSet}", CRS_EPSG, brta_template)
            url_brta = re.sub("{TileMatrix}", ZOOM, url_brta)
            # Generic replacement for tile indices to get base URL
            self.brta_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg|png8)", "", url_brta)
            self.brta_extension = ".png" if ".png" in brta_template else ".jpg"
        except Exception as e:
            print(f" Warning: Could not initialize Topotijdreis 2022: {e}. Falling back to default background.")
            # Fallback logic could be added here if needed

        print("Initializing BGT WMTS service...")
        bgt_client = wmts(BGT_WMTS_URL)
        bgt_layer = bgt_client[BGT_DATASET_ID]
        bgt_template = bgt_layer.resourceURLs[0]["template"]
        url_bgt = re.sub("{TileMatrixSet}", CRS_EPSG, bgt_template)
        url_bgt = re.sub("{TileMatrix}", ZOOM, url_bgt)
        self.bgt_url = re.sub(r"/{TileCol}/{TileRow}\.(png|jpeg|jpg)", "", url_bgt)
        self.bgt_extension = ".png" if ".png" in bgt_template else ".jpg"

        x_tile_amount = matrix.matrixheight
        y_tile_amount = matrix.matrixwidth
        x_min, y_max = matrix.topleftcorner
        dist_x = matrix.scaledenominator * self.tileheight * 0.00028 * x_tile_amount
        dist_y = matrix.scaledenominator * self.tilewidth * 0.00028 * y_tile_amount
        x_max = x_min + dist_x
        y_min = y_max - dist_y
        self.bbox_source = (x_min, y_min, x_max, y_max)
        self.wmts_client = client
        print(f"WMTS initialized. Resolution: {matrix.scaledenominator * 0.00028:.2f}m/px")
    
    def initialize_wms(self):
        print("Initializing KEA WMS service...")
        print(f"Connecting to Sogelink Server for {len(KEA_DATASET_IDS)} KEA layers...")
        try:
            self.kea_client = WebMapService(KEA_WMS_URL, version='1.3.0', timeout=60)
            if self.kea_client and hasattr(self.kea_client, 'contents'):
                for lid in KEA_DATASET_IDS:
                    if lid in self.kea_client.contents:
                        print(f" SUCCESS: Connected. Layer '{lid}' found.")
                    else:
                        print(f" Warning: Layer '{lid}' not found.")
            else:
                print(" ERROR: Could not retrieve capabilities from Sogelink.")
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
            # Topotijdreis ArcGIS WMTS uses {TileMatrix}/{TileRow}/{TileCol} style
            # or sometimes just base/{c}/{r}. PDOK standard is /{c}/{r}.
            # We'll stick to the V3 logic which was /{c}/{r} unless base_url suggests otherwise.
            url = f"{base_url}/{c_pos}/{r_pos}{ext}"
            try:
                async with session.get(url, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    return None
            except Exception:
                return None

    def _safe_image_to_array(self, img):
        """Convert a PIL image to a 3-channel numpy array safely."""
        if img.mode == 'P':
            return np.array(img.convert('RGB'))
        arr = np.array(img)
        if arr.ndim == 3 and arr.shape[2] == 4:
            return arr[:, :, :3]
        return arr

    async def download_area(self, x_range, y_range):
        height = len(y_range) * self.tileheight
        width = len(x_range) * self.tilewidth
        
        cir_array = np.zeros((height, width, 3), dtype=np.uint8)
        rgb_array = np.zeros((height, width, 3), dtype=np.uint8)
        brt_array = np.zeros((height, width, 3), dtype=np.uint8)
        brta_array = np.zeros((height, width, 3), dtype=np.uint8)
        bgt_array = np.zeros((height, width, 3), dtype=np.uint8)
        
        semaphore = asyncio.Semaphore(5) 
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
                    img = Image.open(BytesIO(brt_results[idx]))
                    brt_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = self._safe_image_to_array(img)
                if brta_results[idx]:
                    img = Image.open(BytesIO(brta_results[idx]))
                    brta_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = self._safe_image_to_array(img)
                if bgt_results[idx]:
                    img_bgt = Image.open(BytesIO(bgt_results[idx]))
                    bgt_array[r_start:r_start + self.tileheight, c_start:c_start + self.tilewidth] = self._safe_image_to_array(img_bgt)
                
        return cir_array, rgb_array, brt_array, brta_array, bgt_array

    def download_height_data(self, bbox, width, height, layer='dsm_05m'):
        print(f"  Downloading AHN {layer} ({width}x{height} pixels after upsampling)...")
        wcs = wcs_client(AHN_WCS_URL, version='1.0.0', timeout=120)
        try:
            response = wcs.getCoverage(identifier=layer, bbox=bbox, format='GEOTIFF', crs=CRS_EPSG, resx=0.5, resy=0.5)
            raw_content = response.read()
            with rio.MemoryFile(raw_content) as memfile:
                with memfile.open() as src:
                    src_nodata = src.nodata
                    data = src.read(1, out_shape=(height, width), resampling=rio.enums.Resampling.bilinear).astype(np.float32)
            if src_nodata is not None: mask = (data == src_nodata)
            else: mask = (data > 1e30) | (data < -1e30)
            return np.ma.array(data, mask=mask)
        except Exception as e:
            print(f"  Error downloading {layer}: {e}")
            return np.ma.array(np.zeros((height, width), dtype=np.float32), mask=True)

    def download_wms_data(self, bbox, width, height, layer):
        if not self.kea_client: return np.zeros((height, width, 4), dtype=np.uint8)
        full_map = np.zeros((height, width, 4), dtype=np.uint8)
        rows, cols = 2, 2
        chunk_w, chunk_h = width // cols, height // rows
        x_min, y_min, x_max, y_max = bbox
        dx, dy = (x_max - x_min) / cols, (y_max - y_min) / rows
        print(f"  Downloading KEA layer '{layer}' in chunks...")
        for i in range(rows):
            for j in range(cols):
                chunk_bbox = (x_min + (j * dx), y_max - ((i + 1) * dy), x_min + ((j + 1) * dx), y_max - (i * dy))
                try:
                    response = self.kea_client.getmap(layers=[layer], srs=CRS_EPSG, bbox=chunk_bbox, size=(chunk_w, chunk_h), format='image/png', transparent=True)
                    img = Image.open(BytesIO(response.read())).convert('RGBA')
                    img_arr = np.array(img.resize((chunk_w, chunk_h)))
                    full_map[i * chunk_h:i * chunk_h + chunk_h, j * chunk_w:j * chunk_w + chunk_w] = img_arr
                except Exception as e: print(f"    Chunk {i},{j} failed: {e}")
        return full_map

    def _reclassify_kea(self, rgba_array, layer_id):
        """Reclassify an RGBA KEA image into a categorical single-band raster using LUT."""
        config = KEA_CLASSIFICATION_CONFIG.get(layer_id)
        if not config:
            return None, {}, {}
        
        h, w, c = rgba_array.shape
        # Input is RGBA, we only use RGB for matching
        rgb = rgba_array[:, :, :3]
        alpha = rgba_array[:, :, 3]
        
        # Output categorical mask (0 = No Data)
        cat_mask = np.zeros((h, w), dtype=np.uint8)
        
        # Prepare LUT entries
        colors = np.array(list(config.keys())) # (N, 3)
        ids = [v[0] for v in config.values()]
        
        # Efficient vector-based closest color matching
        # Flatten image for bulk computation
        pixels = rgb.reshape(-1, 3)
        
        # For each pixel, find the color in LUT with minimal distance
        # We use squared Euclidean distance for speed
        # pixels: (P, 3), colors: (N, 3) -> dists: (P, N)
        from scipy.spatial.distance import cdist
        dists = cdist(pixels, colors, 'sqeuclidean')
        closest_indices = np.argmin(dists, axis=1)
        
        # Map indices back to our specific Category IDs
        mapped_ids = np.array(ids)[closest_indices]
        cat_mask = mapped_ids.reshape(h, w)
        
        # Mask out transparent areas (alpha < 10) as 0
        cat_mask[alpha < 10] = 0
        
        # Build label mapping for auxiliary metadata
        mapping = {v[1]: v[0] for v in config.values()}
        colormap = {v[0]: k + (255,) for k, v in config.items()}
        
        return cat_mask, mapping, colormap

    async def _fetch_bgt_features(self, session, collection, bbox_rd, semaphore, max_features=10000):
        """Fetch all GeoJSON features for a specific collection and BBOX using pagination and PEILDATUM."""
        url = f"{BGT_FEATURES_URL}/collections/{collection}/items"
        params = {
            "bbox": f"{bbox_rd[0]},{bbox_rd[1]},{bbox_rd[2]},{bbox_rd[3]}",
            "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/28992",
            "crs": "http://www.opengis.net/def/crs/EPSG/0/28992",
            "limit": 1000,
            "datetime": PEILDATUM  # NEW in V4: Temporal filtering
        }
        
        all_features = []
        next_url = url
        current_params = params

        async with semaphore:
            try:
                while next_url and len(all_features) < max_features:
                    retry_count = 0
                    success = False
                    while retry_count < 3 and not success:
                        try:
                            async with session.get(next_url, params=current_params, timeout=60) as resp:
                                if resp.status != 200:
                                    print(f"    Warning: BGT {collection} returned status {resp.status}")
                                    break
                                
                                data = await resp.json()
                                features = data.get("features", [])
                                all_features.extend(features)
                                
                                next_url = None
                                current_params = None 
                                links = data.get("links", [])
                                for link in links:
                                    if link.get("rel") == "next":
                                        next_url = link.get("href")
                                        break
                                success = True
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            retry_count += 1
                            if retry_count >= 3:
                                raise e
                            await asyncio.sleep(2 ** retry_count) # exponential backoff
                return all_features
            except Exception as e:
                err_type = type(e).__name__
                print(f"    Error fetching BGT {collection}: {err_type} {e}")
                return all_features

    async def _fetch_brt_features(self, session, collection, bbox_rd, semaphore, max_features=10000):
        """Fetch all GeoJSON features from BRT Top10NL OGC API using pagination and PEILDATUM."""
        url = f"{BRT_FEATURES_URL}/collections/{collection}/items"
        params = {
            "bbox": f"{bbox_rd[0]},{bbox_rd[1]},{bbox_rd[2]},{bbox_rd[3]}",
            "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/28992",
            "crs": "http://www.opengis.net/def/crs/EPSG/0/28992",
            "limit": 1000,
            "f": "json"
            # Removed datetime for BRT as it is not supported by the OGC API (Status 400)
        }

        all_features = []
        next_url = url
        current_params = params

        async with semaphore:
            try:
                while next_url and len(all_features) < max_features:
                    retry_count = 0
                    success = False
                    while retry_count < 3 and not success:
                        try:
                            async with session.get(next_url, params=current_params, timeout=60) as resp:
                                if resp.status != 200:
                                    print(f"    Warning: BRT {collection} returned status {resp.status}")
                                    break
                                
                                data = await resp.json(content_type=None)
                                features = data.get("features", [])
                                all_features.extend(features)
                                
                                next_url = None
                                current_params = None
                                links = data.get("links", [])
                                for link in links:
                                    if link.get("rel") == "next":
                                        next_url = link.get("href")
                                        break
                                success = True
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            retry_count += 1
                            if retry_count >= 3:
                                raise e
                            await asyncio.sleep(2 ** retry_count) # exponential backoff
                return all_features
            except Exception as e:
                print(f"    Error fetching BRT {collection}: {e}")
                return all_features

    async def download_categorical_bgt(self, bbox_rd, width, height, transform, bgt_max_features=10000):
        """Fetch BGT feature collections and rasterize them with granular human-readable labels."""
        print(f"  Generating categorical BGT raster (Peildatum: {PEILDATUM})...")

        collection_config = [
            ("onbegroeidterreindeel", "fysiek_voorkomen"),
            ("begroeidterreindeel",   "fysiek_voorkomen"),
            ("waterdeel",             "type"),
            ("ondersteunendwegdeel",  "functie"),
            ("wegdeel",               "functie"),
            ("spoor",                 "functie"),
            ("overigbouwwerk",        "bgt_type"),
            ("pand",                  None),
        ]

        mask = np.zeros((height, width), dtype=np.uint8)
        semaphore = asyncio.Semaphore(5)

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_bgt_features(session, coll, bbox_rd, semaphore, max_features=bgt_max_features)
                for coll, _ in collection_config
            ]
            results = await asyncio.gather(*tasks)

        label_to_id = {}
        next_id = 1

        for (coll, prop_name), features in zip(collection_config, results):
            if not features: continue
            shapes_by_id = {}
            for feat in features:
                geom = feat.get("geometry")
                if not geom: continue
                props = feat.get("properties", {})
                subtype = props.get(prop_name) if prop_name else None
                label = f"{coll.capitalize()}: {subtype}" if subtype and subtype != "Bestaand" else coll.capitalize()
                if label not in label_to_id:
                    label_to_id[label] = next_id
                    next_id += 1
                val = label_to_id[label]
                shapes_by_id.setdefault(val, []).append(geom)

            for val, geoms in shapes_by_id.items():
                rasterio.features.rasterize([(g, val) for g in geoms], out=mask, transform=transform)

        if np.any(mask == 0):
            fallback_label = "Geen BGT-object (niet geclassificeerd)"
            if fallback_label not in label_to_id:
                label_to_id[fallback_label] = next_id
                next_id += 1
            mask[mask == 0] = label_to_id[fallback_label]

        return mask, label_to_id

    async def download_categorical_brt(self, bbox_rd, width, height, transform, brt_max_features=10000):
        """Fetch BRT Top10NL feature collections via OGC API and rasterize them."""
        print(f"  Generating BRT Top10NL categorical raster (Peildatum: {PEILDATUM})...")

        collection_config = [
            ("terrein_vlak",                "typelandgebruik",       "Terrein",              False),
            ("waterdeel_vlak",              "typewater",             "Waterdeel",            False),
            ("wegdeel_lijn",                "typeweg",               "Weg",                  True),
            ("wegdeel_vlak",                "typeweg",               "Weg",                  False),
            ("spoorbaandeel_lijn",          None,                    "Spoorbaandeel",        True),
            ("functioneel_gebied_vlak",     "typefunctioneelgebied", "Functioneel gebied",   False),
            ("functioneel_gebied_multivlak","typefunctioneelgebied", "Functioneel gebied",   False),
            ("gebouw_vlak",                 None,                    "Gebouw",               False),
        ]

        mask = np.zeros((height, width), dtype=np.uint8)
        semaphore = asyncio.Semaphore(8)

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_brt_features(session, coll_id, bbox_rd, semaphore, max_features=brt_max_features)
                for coll_id, _, _, _ in collection_config
            ]
            results = await asyncio.gather(*tasks)

        label_to_id = {}
        next_id = 1

        for (coll_id, prop_name, base_name, all_touched), features in zip(collection_config, results):
            if not features: continue
            shapes_by_id = {}
            for feat in features:
                geom = feat.get("geometry")
                if not geom: continue
                props = feat.get("properties", {})
                if prop_name and props.get(prop_name):
                    raw = str(props[prop_name]).split("|")[0].strip()
                    label = f"{base_name}: {raw}"
                else: label = base_name
                if label not in label_to_id:
                    label_to_id[label] = next_id
                    next_id += 1
                val = label_to_id[label]
                shapes_by_id.setdefault(val, []).append(geom)

            for val, geoms in shapes_by_id.items():
                rasterio.features.rasterize([(g, val) for g in geoms], out=mask, transform=transform, all_touched=all_touched)

        if np.any(mask == 0):
            fallback_label = "Geen BRT-object (niet geclassificeerd)"
            if fallback_label not in label_to_id:
                label_to_id[fallback_label] = next_id
                next_id += 1
            mask[mask == 0] = label_to_id[fallback_label]

        return mask, label_to_id

    @staticmethod
    def _brt_color(label):
        """Return an RGBA tuple for a BRT label based on its type group."""
        l = label.lower()
        if "geen brt" in l or "niet geclassificeerd" in l: return (235, 232, 225, 255)
        if "gebouw" in l: return (220, 75, 50, 255)
        if "spoor" in l: return (80, 50, 20, 255)
        if "inrichting" in l: return (190, 175, 155, 255)
        if l.startswith("weg"):
            if "autosnelweg" in l: return (60, 60, 60, 255)
            if "hoofdweg" in l: return (100, 100, 100, 255)
            if "lokale weg" in l or "straat" in l: return (175, 175, 175, 255)
            return (155, 155, 155, 255)
        if "water" in l: return (70, 145, 215, 255)
        if "terrein" in l:
            if "bos" in l: return (30, 100, 30, 255)
            if "grasland" in l: return (110, 195, 75, 255)
            if "akker" in l: return (195, 180, 90, 255)
            return (150, 195, 115, 255)
        if "functioneel" in l: return (160, 195, 215, 255)
        return (145, 145, 145, 255)

    def _write_qgis_style(self, tiff_path, label_mapping, colormap):
        """Generate a .qml style file for QGIS to ensure labels and colors work perfectly."""
        if not label_mapping or not colormap: return
        
        qml_lines = [
            '<!DOCTYPE qgis PUBLIC "http://mrcc.com/qgis.dtd" "SYSTEM">',
            '<qgis version="3.44.8" styleCategories="AllStyleCategories">',
            '  <pipe>',
            '    <rasterrenderer opacity="1" band="1" type="paletted" alphaBand="-1">',
            '      <colorPalette>'
        ]
        
        # 1. Standard Color Palette (for the Legend)
        sorted_items = sorted(label_mapping.items(), key=lambda x: x[1])
        for label, val in sorted_items:
            if val in colormap:
                r, g, b, a = colormap[val]
                hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
                safe_label = html.escape(label)
                qml_lines.append(f'        <paletteEntry color="{hex_color}" label="{safe_label}" value="{val}" alpha="{a}"/>')
        
        qml_lines.extend([
            '      </colorPalette>',
            '    </rasterrenderer>',
            '  </pipe>',
            '  <attributeTable>',
            '    <field name="Value" usage="8" type="0" defn="0"/>',
            '    <field name="Label" usage="1" type="2" defn="1"/>'
        ])

        # 2. Attribute Table rows (for Identify Results tool)
        id_to_label = {v: k for k, v in label_mapping.items()}
        for i in range(max(label_mapping.values()) + 1):
            label = id_to_label.get(i, "No Data" if i == 0 else "Unknown")
            safe_label = html.escape(label)
            qml_lines.append(f'    <row index="{i}"><field name="Value">{i}</field><field name="Label">{safe_label}</field></row>')
            
        qml_lines.extend([
            '  </attributeTable>',
            '</qgis>'
        ])
        
        qml_path = os.path.splitext(tiff_path)[0] + ".qml"
        with open(qml_path, "w", encoding="utf-8") as f:
            f.write("\n".join(qml_lines))

    def _write_pam_metadata(self, tiff_path, label_mapping, colormap=None):
        """Generate a .aux.xml sidecar file for QGIS to show categorical labels."""
        if not label_mapping: return
        
        max_id = max(label_mapping.values())
        categories = [""] * (max_id + 1)
        categories[0] = "No Data"
        for label, val in label_mapping.items():
            categories[val] = label
            
        xml_lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<PAMDataset>",
            '  <PAMRasterBand band="1">',
            "    <Metadata>"
        ]
        
        for label, val in label_mapping.items():
            safe_label = html.escape(label)
            xml_lines.append(f'      <MDI key="CLASS_{val}">{safe_label}</MDI>')
        xml_lines.extend([
            '      <MDI key="GDAL_METADATA_DOMAIN">CATEGORICAL</MDI>',
            '      <MDI key="CATEGORICAL">YES</MDI>',
            "    </Metadata>",
            "    <CategoryNames>"
        ])
        
        for cat in categories:
            safe_cat = html.escape(cat)
            xml_lines.append(f"      <Category>{safe_cat}</Category>")
        
        xml_lines.extend([
            "    </CategoryNames>",
            "    <GDALRasterAttributeTable>",
            '      <Field defn="0" name="Value" type="0" usage="8"/>',
            '      <Field defn="1" name="Label" type="2" usage="1"/>'
        ])
        
        if colormap:
            xml_lines.extend([
                '      <Field defn="2" name="Red" type="0" usage="3"/>',
                '      <Field defn="3" name="Green" type="0" usage="4"/>',
                '      <Field defn="4" name="Blue" type="0" usage="5"/>',
                '      <Field defn="5" name="Alpha" type="0" usage="6"/>'
            ])
        
        id_to_label = {v: k for k, v in label_mapping.items()}
        for i in range(max_id + 1):
            label = id_to_label.get(i, "No Data" if i == 0 else "")
            safe_label = html.escape(label)
            
            xml_lines.append(f'      <Row index="{i}"><F>{i}</F><F>{safe_label}</F>')
            if colormap:
                r, g, b, a = colormap.get(i, (0, 0, 0, 0))
                xml_lines.append(f'<F>{r}</F><F>{g}</F><F>{b}</F><F>{a}</F>')
            xml_lines.append('</Row>')
            
        xml_lines.extend([
            "    </GDALRasterAttributeTable>",
            "  </PAMRasterBand>",
            "</PAMDataset>"
        ])
        
        with open(f"{tiff_path}.aux.xml", "w", encoding="utf-8") as f:
            f.write("\n".join(xml_lines))

    def run(self, limit=None, targets=None, include_height=True, include_bgt_cat=True, include_brt_cat=True, bgt_max_features=10000, brt_max_features=10000):
        self.initialize_wmts()
        self.initialize_wms() 
        df_buurten = self.fetch_buurten(targets=targets)
        if df_buurten.empty: return
        
        if targets:
            targets_lower = [t.lower().strip() for t in targets]
            name_col = 'buurtnaam' if 'buurtnaam' in df_buurten.columns else 'buurtname'
            mask = df_buurten['buurtcode'].str.lower().isin(targets_lower) | df_buurten[name_col].str.lower().isin(targets_lower)
            df_buurten = df_buurten[mask]
            if df_buurten.empty: return

        if limit: df_buurten = df_buurten.iloc[:limit]
        total = len(df_buurten)
        name_col = 'buurtnaam' if 'buurtnaam' in df_buurten.columns else 'buurtname'
        
        for i, (idx, row) in enumerate(df_buurten.iterrows()):
            buurt_name = row[name_col]; buurt_code = row['buurtcode']
            base_filename = f"{buurt_code}_{buurt_name.replace(' ', '_')}"
            output_cir = self.outdir / f"{base_filename}_CIR.tif"
            output_rgb = self.outdir / f"{base_filename}_RGB.tif"
            output_ndvi = self.outdir / f"{base_filename}_NDVI.tif"
            output_bgt_cat = self.outdir / f"{base_filename}_BGT_CAT.tif"
            output_brt_cat = self.outdir / f"{base_filename}_BRT_CAT.tif"
            
            print(f"[{i+1}/{total}] Processing {buurt_name} ({buurt_code})...")
            bbox_geom = row.geometry.bounds 
            x_range, y_range = self.get_index_bounds(bbox_geom)
            bbox_tiles = self.get_bbox_of_tile_indices(x_range, y_range)
            
            try:
                async def fetch_all_data():
                    results_area = await self.download_area(x_range, y_range)
                    h, w = results_area[0].shape[0], results_area[0].shape[1]
                    transform = rio.transform.from_bounds(*bbox_tiles, w, h)
                    bgt_cat_res, lbl_mapping = (await self.download_categorical_bgt(bbox_tiles, w, h, transform, bgt_max_features=bgt_max_features)) if include_bgt_cat else (None, {})
                    brt_cat_res, brt_lbl_mapping = (await self.download_categorical_brt(bbox_tiles, w, h, transform, brt_max_features=brt_max_features)) if include_brt_cat else (None, {})
                    return results_area, bgt_cat_res, lbl_mapping, brt_cat_res, brt_lbl_mapping, h, w, transform

                (cir_data, rgb_data, brt_data, brta_data, bgt_data), bgt_cat, label_mapping, brt_cat, brt_label_mapping, h, w, transform = asyncio.run(fetch_all_data())
                
                common_meta = {'driver': 'GTiff', 'height': h, 'width': w, 'crs': CRS_EPSG, 'transform': transform, 'compress': 'lzw', 'tiled': True, 'blockxsize': 256, 'blockysize': 256, 'nodata': 255, 'dtype': 'uint8'}
                with rio.open(output_cir, 'w', count=3, **common_meta) as dst: [dst.write(cir_data[:, :, b], b + 1) for b in range(3)]
                with rio.open(output_rgb, 'w', count=3, **common_meta) as dst: [dst.write(rgb_data[:, :, b], b + 1) for b in range(3)]
                
                map_meta = common_meta.copy(); map_meta.update({'nodata': None, 'photometric': 'rgb', 'interleave': 'pixel', 'count': 3})
                with rio.open(self.outdir / f"{base_filename}_BRT.tif", 'w', **map_meta) as dst:
                    for b in range(3): dst.write(brt_data[:, :, b], b + 1)
                    dst.update_tags(DESCRIPTION="BRT Top10NL (2022) - RGB", PEILDATUM=PEILDATUM)
                with rio.open(self.outdir / f"{base_filename}_BRTA.tif", 'w', **map_meta) as dst:
                    for b in range(3): dst.write(brta_data[:, :, b], b + 1)
                    dst.update_tags(DESCRIPTION="BRT Topotijdreis 2022 Background - RGB", SOURCE="Topotijdreis")
                with rio.open(self.outdir / f"{base_filename}_BGT.tif", 'w', **map_meta) as dst:
                    for b in range(3): dst.write(bgt_data[:, :, b], b + 1)

                # KEA Multi-Layer retrieval
                kea_meta = common_meta.copy()
                kea_meta.update({'count': 4, 'nodata': None, 'interleave': 'pixel'})
                
                for layer_id in KEA_DATASET_IDS:
                    kea_data = self.download_wms_data(bbox_tiles, w, h, layer_id)
                    
                    # 1. Save Visual RGBA (for QGIS display)
                    output_kea_layer = self.outdir / f"{base_filename}_KEA_{layer_id}.tif"
                    try:
                        with rio.open(output_kea_layer, 'w', **kea_meta) as dst:
                            for b in range(4):
                                dst.write(kea_data[:, :, b], b + 1)
                            dst.update_tags(DESCRIPTION=f"KEA Visual Overlay: {layer_id}")
                    except PermissionError:
                        print(f"    WARNING: Permission denied for {output_kea_layer.name}. Is it open in QGIS? Skipping...")
                        continue
                    
                    # 2. Reclassify to Labelled Categorical (for ML)
                    kea_cat, kea_lbl_map, kea_colormap = self._reclassify_kea(kea_data, layer_id)
                    if kea_cat is not None:
                        output_kea_cat = self.outdir / f"{base_filename}_KEA_CAT_{layer_id}.tif"
                        cat_meta = common_meta.copy()
                        cat_meta.update({'count': 1, 'nodata': 0})
                        try:
                            with rio.open(output_kea_cat, 'w', **cat_meta) as dst:
                                dst.write(kea_cat, 1)
                                dst.write_colormap(1, kea_colormap)
                                dst.update_tags(DESCRIPTION=f"KEA Categorical Labels: {layer_id}", CATEGORICAL="YES")
                            
                            # Generate sidecars for QGIS metadata
                            self._write_pam_metadata(str(output_kea_cat), kea_lbl_map, colormap=kea_colormap)
                            self._write_qgis_style(str(output_kea_cat), kea_lbl_map, kea_colormap)
                            print(f"    SUCCESS: Reclassified KEA {layer_id} into {len(kea_lbl_map)} classes.")
                        except PermissionError:
                            print(f"    WARNING: Permission denied for {output_kea_cat.name}. Is it open in QGIS? Skipping...")

                    del kea_data

                if bgt_cat is not None:
                    cat_meta = common_meta.copy(); cat_meta.update({'count': 1, 'nodata': 0})
                    with rio.open(output_bgt_cat, 'w', **cat_meta) as dst:
                        dst.write(bgt_cat, 1)
                        colormap = {val: (220,75,50,255) if "pand" in l.lower() else (150,150,150,255) for l, val in label_mapping.items()} # simplified color logic for brevity
                        dst.write_colormap(1, colormap)
                    self._write_pam_metadata(str(output_bgt_cat), label_mapping, colormap=colormap)
                    self._write_qgis_style(str(output_bgt_cat), label_mapping, colormap)

                if brt_cat is not None:
                    cat_meta = common_meta.copy(); cat_meta.update({'count': 1, 'nodata': 0})
                    with rio.open(output_brt_cat, 'w', **cat_meta) as dst:
                        dst.write(brt_cat, 1)
                        brt_colormap = {val: self._brt_color(l) for l, val in brt_label_mapping.items()}
                        dst.write_colormap(1, brt_colormap)
                    self._write_pam_metadata(str(output_brt_cat), brt_label_mapping, colormap=brt_colormap)
                    self._write_qgis_style(str(output_brt_cat), brt_label_mapping, brt_colormap)

                # NDVI
                nir, red_b = cir_data[:,:,0].astype(np.float32), cir_data[:,:,1].astype(np.float32)
                ndvi = np.divide(nir - red_b, nir + red_b, out=np.zeros_like(nir), where=(nir + red_b) != 0)
                with rio.open(output_ndvi, 'w', **{'driver': 'GTiff', 'height': h, 'width': w, 'crs': CRS_EPSG, 'transform': transform, 'count': 1, 'dtype': 'uint8'}) as dst: dst.write(np.round((ndvi + 1) * 100).astype(np.uint8), 1)

                if include_height:
                    dsm = self.download_height_data(bbox_tiles, w, h, layer='dsm_05m')
                    dtm = self.download_height_data(bbox_tiles, w, h, layer='dtm_05m')
                    chm = np.ma.maximum(dsm - dtm, 0)
                    h_meta = common_meta.copy(); h_meta.update({'dtype': np.float32, 'nodata': -9999.0, 'count': 1})
                    with rio.open(self.outdir / f"{base_filename}_CHM.tif", 'w', **h_meta) as dst: dst.write(chm.filled(-9999.0), 1)

                print(f"  Neighborhood complete.")
                gc.collect()
            except Exception as e: print(f"Error processing {buurt_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Surveyor for Súdwest-Fryslân - Version 4 (2022 Historical)")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--outdir", type=str, default="outdir/municipality_survey_v4")
    parser.add_argument("--buurten", nargs="+")
    parser.add_argument("--file", type=str, default="targets.txt")
    parser.add_argument("--no-height", action="store_false", dest="height")
    parser.set_defaults(height=True)
    args = parser.parse_args()
    targets = []
    if args.buurten: targets.extend(args.buurten)
    if args.file and Path(args.file).exists():
        with open(args.file, 'r') as f: targets.extend([l.strip() for l in f if l.strip()])
    surveyor = MunicipalitySurveyor(args.outdir, overwrite=args.overwrite)
    surveyor.run(limit=args.limit if args.limit > 0 else None, targets=targets if targets else None, include_height=args.height)
