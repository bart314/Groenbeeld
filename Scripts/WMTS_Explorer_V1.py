# coding: utf-8
"""
Title:"WMTS_Explorer_V1.py".

Finds and prints usefull parameters contained in an WMTS.

Development started on at least Jul ~~ 2025.
Script last inspected at least on dec 08 2025.

@author: Siebrant Hendriks
emails: siebrant.business@gmail.com 
        s.hendriks2@sudwestfryslan.nl
"""

from owslib.wmts import WebMapTileService as wmts
import xml.etree.ElementTree as ET
import requests


# xml verkennen
url = 'https://service.pdok.nl/hwh/luchtfotocir/wmts/v1_0?request=GetCapabilities&service=wmts'
link = wmts(url)
print("OWSlib WMTS version:", link.version)
print("title:", link.identification.title)
print("abstract:", link.identification.abstract)
print("vendor kwargs:", link.vendor_kwargs, "\n")
print("keywords:", link.identification.keywords, "\n")
print("provider:", link.provider.name)
print("provider url:", link.provider.url, "\n")
print("contents:", list(link.contents.keys()), "\n")
print("CRS compatible:", list(link.tilematrixsets.keys()), "\n")


matrix_set = link.tilematrixsets["EPSG:28992"]
print("crs:", matrix_set.crs)
# print(matrix_set.identifier)
print(matrix_set.tilematrix.keys(), "\n")

# Kies één van de tilematrix keys om de inhoud daarvan verder te bekijken
# e.g. "14".
matrix = matrix_set.tilematrix['14']
print(f'identifier:\t\t{matrix.identifier}')
print(f'scale:\t\t\t{matrix.scaledenominator}')
print(f'topleftcorner:  {matrix.topleftcorner}')
print(f'tilewidth:\t\t{matrix.tilewidth} pixels')
print(f'tileheight:\t\t{matrix.tileheight} pixels')
print(f'matrixwidth:\t{matrix.matrixwidth} tiles')
print(f'matrixheight:\t{matrix.matrixheight} tiles\n')

# Kies één van de content/layer keys om de inhoud daarvan verder te bekijken
# e.g. "2024_ortho25IR".
print("id:\t\t", link['2024_ortho25IR'].id)
print("naam:\t", link['2024_ortho25IR'].name)
print("index:\t", link['2024_ortho25IR'].index)
print("abstract:", link['2024_ortho25IR'].abstract)
print("bbox:\t", link['2024_ortho25IR'].boundingBoxWGS84)
print("urls:\t", link['2024_ortho25IR'].resourceURLs, "\n")


# Metadata (aanmaakdatums)
# OWSlib kan niet goed de metadata vinden,
# dus daarvoor gaan we nog ouderwets door de XML spitten.
xml_link = requests.get(url)
xml_root = ET.fromstring(xml_link.text)
print("xml tag:", xml_root.tag)
print("xml attribs:", xml_root.attrib, "\n")
for child in xml_root:
    if "Contents" in child.tag:
        con = child.tag
root_down = xml_root.find(con)
cnt = 0
for child in root_down:
    if "Layer" in child.tag:
        layer = child.tag
        break
for entry in root_down.iter(layer):
    for child in entry:
        if "Title" in child.tag:
            title = child.text
        if "Identifier" in child.tag:
            identifier = child.text
        if "Metadata" in child.tag:
            metadata = child.attrib
            key = list(metadata.keys())[0]
            metadata_url = metadata[key]
            metadata_link = requests.get(metadata_url)
            metadata_xml_root = ET.fromstring(metadata_link.text)
            for child in metadata_xml_root:
                if "identificationInfo" in child.tag:
                    down = metadata_xml_root.find(child.tag)
                    break
            down = down[0]
            for child in down:
                if "citation" in child.tag:
                    down = down.find(child.tag)
                    down = down[0]
            for child in down:
                if "date" in child.tag:
                    down = down.find(child.tag)
                    datum = down[0][0][0].text
    print(identifier, ":", title, "\nmetadata:", metadata, "\ndatum :",
          datum, "\n")
