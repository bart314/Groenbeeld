import requests

# 1. Using the 'NGR' API with a specific search for 'pdok' to ensure results
api_endpoint = "https://www.nationaalgeoregister.nl/geonetwork/srv/dut/q"
query_params = {
    '_content_type': 'json',
    'q': 'pdok wmts', # Specific search terms
    'fast': 'index',
    'from': 1,
    'to': 10
}

try:
    print(f"{'Title':<50} | {'WMTS URL'}")
    print("-" * 110)

    response = requests.get(api_endpoint, params=query_params)
    data = response.json()

    # If 'metadata' is missing, the list will be empty
    metadata_list = data.get('metadata', [])
    
    if not metadata_list:
        print("No datasets found. The API might be down or the search term returned nothing.")

    for record in metadata_list:
        title = record.get('title', 'Unknown Title')
        
        # PDOK sometimes stores links in a list, sometimes as a single string
        links = record.get('link', [])
        if isinstance(links, str): # Handle cases where it's not a list
            links = [links]

        found_link = False
        for link_item in links:
            # Look for ANY link containing 'wmts'
            if 'wmts' in link_item.lower():
                print(f"{title[:48]:<50} | {link_item}")
                found_link = True
                break # Just show the first one found per title
        
        if not found_link:
             # This helps us see which titles are being skipped
             print(f"{title[:48]:<50} | [No WMTS link found in this record]")

except Exception as e:
    print(f"Error: {e}")

# 
from owslib.wmts import WebMapTileService
from owslib.wms import WebMapService

# 1. Put all your URLs into a list (square brackets)
urls_wmts = [
    "https://service.pdok.nl/hwh/luchtfotocir/wmts/v1_0?request=getcapabilities",
    "https://service.pdok.nl/brt/achtergrondkaart/wmts/v2_0?request=getcapabilities&service=wmts",
    "https://service.pdok.nl/brt/top10nl/wmts/v1_0?request=getcapabilities",
    "https://service.pdok.nl/lv/bgt/wmts/v1_0?request=GetCapabilities&service=WMTS",  
]

# 2. Use a loop to go through them one by one
for test_url in urls_wmts:
    print(f"\nChecking: {test_url}") # Helpful to see which one is being tested
    
    try:
        # Create the WMTS object
        wmts = WebMapTileService(test_url)
        
        print("--- SUCCESS ---")
        print(f"Service Title: {wmts.identification.title}")
        
        # List the layers found in this specific URL
        layers = list(wmts.contents.keys())
        print(f"Available Layers ({len(layers)}): {layers}")
        
    except Exception as e:
        print(f"--- FAILURE ---")
        print(f"Reason: {e}")

print("\n--- All tests complete ---")

# URL with WMS
url = [
    "https://cas.cloud.sogelink.com/public/data/org/gws/YWFMLMWERURF/kea_public/wms?service=wms&request=getcapabilities"
    ]

for wms_url in url:
    print(f"\nConnected to: {wms_url}")

try:
     # Create the WMS object
     wms = WebMapService(wms_url)

     print("--- SUCCES ---")
     print(f"Service Title: {wms.identification.title}")

     # List the layers found in this specific URL
     print("\nAvailable Layers:")
     layers = list(wms.contents)
     if not layers:
         print("No layers found. Check if the service requires authentication or a different version.")
     else: 
         for layer_id in layers:
             layer_title = wms[layer_id].title
             print(f" - {layer_id} ({layer_title})")

except Exception as e: 
    print(f"That URL didn't work. Error: {e}")
