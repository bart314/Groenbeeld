# Groenbeeld Súdwest‑Fryslân

## Introductie
In deze repository staan de scripts die de kaart voor het groenbeeld van de gemeente Súdwest-Fryslân maken. Het doel hiervan is het aan kunnen bieden van extra kaarten in de [gebiedsmonitor](https://sudwestfryslan.gebiedsmonitor.nl/) van de gemeente. De gebiedsmonitor is een groter overkoepelend project van de gemeente wat op het moment in actieve ontwikkeling is. Het doel van de gebiedsmonitor is om meer kaarten, met verschillende soorten interessante informatie, op toegankelijke manier aan collega's en inwoners te verschaffen. Dit kan dan gebruikt worden ter informatie en in toekomstige besluitvorming.

## Wat staat er in de kaart?
In deze kaart is er over het hele gebied van de gemeente Súdwest-Fryslân de "Normalized Difference Vegetation Index" (NDVI) berekend.
Dit is een waarde die per meetpunt aangeeft of er vegetatie op dat punt aanwezig is, en zo ja, hoe gezond die vegetatie zou zijn. Elk meetpunt betreft een gebied van 25 bij 25cm. Deze kaart kan hierdoor gebruikt worden om te kijken hoeveel groen er in de gemeente is, en op welke plekken. Maar dus ook op welke plekken het nog beter kan, en dus aagepakt zouden kunnen worden met het voeren van klimaatbeleid.

## Wat staat er in deze repository?
In deze repository staan een aantal opeenvolgende scripts die de nodige data ophalen en verder verwerken voor onze doeleinden.
Globaal worden de volgende handelingen vericht:
* Het ophalen van de benodigde infrarood luchfoto data van [PDOK]((https://www.pdok.nl/introductie/-/article/pdok-luchtfoto-infrarood-open-)).
* Omrekenen van kleurwaardes van de luchtfoto naar NDVI waardes.
* Selecteren van een specifiek gebied voor de kaart. Hier dus het grondgebied van de gemeente Súdwest-Fryslân.
* Analyseert de verschillende waardes in NDVI over het gebied zodat logische groepeeringen gekozen kunen worden. Bijvoorbeeld: "geen vegetatie", "Zwakke vegetatie" en "Gezonde vegetatie".
Op het moment verwerken deze scripts alleen nog de data van 2024 als *proof of concept*. Op een verder punt van dit project kunnen de scripts makkelijk aangepast worden om van anderen en of meerdere jaren de data op te halen en te verwerken. 

## Samenwerking
De direct aanleiding voor het maken van deze repositoy is het beschikbaar stellen van de scripts en data van dit project als zijnde bijlage voor de casus voor het eindproject van het Traineeship van YER (lichting 2024). Bij interesse in samenwerking bij dit project, of voor vragen, kan contact opgenomen worden met het team [AI & security](mailto:s.hendriks2@sudwestfryslan.nl) van de gemeente Súdwest-Fryslân.

## Setup
Grotere data bestanden zijn op [google drive](https://drive.google.com/drive/folders/1T0Yv5aPW4eBZI19VUF1yLQH_ZcdhNZrQ?usp=sharing) beschikbaar.

Alle scripts zijn geschreven in python. Voor alle gebruikte python packages kan een Conda omgeving gemaakt worden met het bestand `NDVI-SWF.yaml` in de root van deze repo. 
Conda kan geïnstalleerd worden samen met de suite van [anaconda](https://www.anaconda.com/download) maar ook zelfstanding met de [miniforge](https://conda-forge.org/download/) installer. Vergeet niet de python versie van de conda omgeving in te stellen als de nieuwe python interpreter voor je IDE indien nodig.

## Werkzaamheden per script uitgebreid uitgewerkt
Reeds verrichte werkzaamheden zijn hier in volgorde per script toegelicht:

### Essentiële scripts / scripts voor ophalen basis dataset
* `WMTS_Explore_V1.py`: Dit script benadert de web map tile service (WMTS) van de bron [PDOK](https://www.pdok.nl/introductie/-/article/pdok-luchtfoto-infrarood-open-) en leest uit, naar de python interpreter, de randvoorwaarden die nodig zijn voor het ophalen van de data.
* `NDVI_Retriever_V2.py`: Dit script haalt de data van de bron op, en rekent die om naar de NDVI waardes. Alle randvoorwaarden zijn hardcoded in het script, in de toekomst zou dit een config.txt file mogen zijn. De dataset die het script op het moment op zou halen zijn de meetwaardes van 2024. Het resultaat van dit script is een laag raster data met de berekende NDVI als meetwaardes. Deze meetwaardes zijn getransformeerd naar het datatype uint8 met als bereik 0-200 met 255 als de 'nodata' waarde. Dit resultaat wordt opgeslagen als een geotiff met `.tiff` of `.tif` als extensie.
* `NDVI_Clip_V3.py` Dit script neemt de vector (het gebied) van van de gemeente SWF, met uitzondering van het IJsselmeer. Dit gebied is opgeslagen in de geopackage `Gemeentegrens_zonder_IJsselmeer.gpkg` die ook aanwezig is in de folder data van deze repo. Dit gebied wordt gebruikt om de data binnen dit gebied te snijden uit de geotiff van het vorige script. Het resultaat is dus een nieuwe geotiff met alleen de data die binnen het gebied van de gemeente SWF valt. Deze geotiff met de naam [ndvi_swf_clipped_geen_IJsselmeer.tiff](https://drive.google.com/file/d/1IonB-2ssuUaaGaB4a8zMmfv8KCcm3JDq/view?usp=sharing) zal het huidige uitgangspunt voor het vervolg van dit project zijn. Dit bestand is beschikbaar in de google drive.

### Scripts voor verdere verwerking

* `NDVI_explorer_V2.py`: Simpel script wat een subset van de data plot voor inspectie en validatie.
* `NDVI_Histogram.py`: Telt hoe vaak elke individuele meetwaarde (in het bereik 0-200) voorkomt en plot die uit in een histogram.
* `NDVI_jenks_V2.py`: Past het [Fisher-Jenks algoritme](https://en.wikipedia.org/wiki/Jenks_natural_breaks_optimization) voor natural breaks toe op de data en berekent de optimale grenswaardes voor het classificeren naar 2-10 groepen.

### Scripts waar op het moment niet verder aan wordt gewerkt

* `NDVI_To_Class.py`: Dit script kan gebruikt worden om de meetwaardes van de dataset op te delen in gedefineerde klasses zoals bijvoorbeeld de uitkomst van het Fisher-Jenks algoritme.
* `NDVI_EGV.py`: Naast het opdelen in klasses maakt dit script ook nog een plot van die uitkomst binnen een bepaald bereik.

## Extra naslagwerk GEO-data in python
### Python for Geographic Data Analysis

De online module [Python for Geographic Data Analysis](https://pythongis.org/index.html) geeft een introductie van hoe geo-data in python verwerkt kan worden. Specifiek deel 2 kan waardevol zijn. Deel 1 geeft een algemene introductie aan python waar de meesten toch wel bekend mee zijn. Deel 3 lijkt op het moment van schrijven nog niet helemaal af te zijn.

### tutorial grondwaterstand
In de map Tutorial_grondwaterstand staat een jupyter notebook `Grondwaterstand python tutorial V4.ipynb` hierin wordt met extra detail een wat simpelere casus in zijn voledigheid behandeld.
