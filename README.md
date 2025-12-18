# NDVI_SWF
Deze repository is voor de samenwerking tussen externen en de gemeente SWF wat betreft het maken van datasets voor het in kaart brengen van de vegetatie van de gemeente.

# Setup
Grotere data bestanden zijn op [google drive](https://drive.google.com/drive/folders/1T0Yv5aPW4eBZI19VUF1yLQH_ZcdhNZrQ?usp=sharing) beschikbaar.

Alle scripts zijn geschreven in python. Voor alle gebruikte python packages kan een Conda omgeving gemaakt worden met het bestand `NDVI-SWF.yaml` in de root van deze repo. 
Conda kan geinstalleerd worden samen met de suite van [anaconda](https://www.anaconda.com/download) maar ook zelfstanding met de [miniforge](https://conda-forge.org/download/) installer.

# Pipeline tot noch toe
Reeds verrichte werkzaamheden zijn hier per script toegelicht:
* `WMTS_Explore_V1.py`: Dit script benaderd de web map tile service (WMTS) van de bron [PDOK](https://www.pdok.nl/introductie/-/article/pdok-luchtfoto-infrarood-open-) en leest uit, naar de python interpreter, de randvoorwaarden die nodig zijn voor het ophalen van de data
* `NDVI_Retriever_V2.py`: Dit script haalt de data van de bron op, en berrekend die om naar de NDVI waardes. Alle randvoorwaarden zijn hardcoded in het script, in de toekomst zou dit een config.txt file mogen zijn. De dataset die het script op het moment op zou halen zijn de meetwaardes van 2024. Het resultaat van dit script is een laag raster data met de berrekende NDVI als meetwaardes. Deze meetwaardes zijn getransformeerd naar het datatype uint8 met als bereik 0-200 met 255 als de 'nodata' waarde. Dit resultaat wordt opgeslagen als een geotiff met .tiff of .tif als extensie.
* `NDVI_Clip_V3.py` Dit script neemt de vector (het gebied) van van de gemeente SWF, met uitzondering van het IJsselmeer. Dit gebied is opgeslagen in de geopackage `Gemeentegrens_zonder_IJsselmeer.gpkg` die ook aanwezig is in de folder data van deze repo. Dit gebied wordt genruikt om de data binnen dit gebied te snijden uit de geotiff van het vorige script. Het resultaat is dus een nieuwe geotiff met alleen de data die binnen het gebied van de gemeente SWF valt. Deze geotiff met de naam [ndvi_swf_clipped_geen_IJsselmeer.tiff](https://drive.google.com/file/d/1HuLmN1yynj1ou16_XS8yJrCO5snSauFP/view?usp=drive_link) zal het huidige uitgangspunt voor het vervolg van dit project zijn. Dit betand is beschikbaar in de google drive.
* `NDVI_explorer_V2.py` 
