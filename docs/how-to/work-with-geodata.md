# Work with geodata

nemdb provides access to Geoscience Australia's National Electricity Infrastructure dataset. All functions cache results as Parquet files after the first fetch.

## Read substations

```python
from nemdb.geodata import read_substations

subs = read_substations()
print(subs.columns.tolist())
# ['name', 'state', 'operationalstatus', 'voltagekv', 'locality', 'geometry', ...]
```

Returns a GeoDataFrame with point geometries for substations in NEM states (NSW, VIC, QLD, SA, TAS, ACT).

## Read transmission lines

```python
from nemdb.geodata import read_transmission_lines

# Raw data (may contain topology errors)
lines_raw = read_transmission_lines(clean=False)

# Cleaned data (topology errors fixed)
lines_clean = read_transmission_lines(clean=True)
```

With `clean=True`, the data goes through a three-stage pipeline:

1. **`line_merge`** -- merges segments that share exact endpoints
2. **`make_continuous`** -- bridges gaps under 100m between nearby segments
3. **`clean_multilines`** -- reconstructs traversal paths for remaining MultiLineStrings

See [Transmission line cleaning](../explanation/transmission-line-cleaning.md) for the full algorithm documentation.

Key columns: `name`, `capacitykv`, `state`, `operationalstatus`, `geometry`.

## Read power stations

```python
from nemdb.geodata import read_major_powerstations

stations = read_major_powerstations()
print(stations[["name", "generationtype", "primaryfueltype", "generationmw"]].head())
```

Returns a GeoDataFrame with point geometries for major power stations.

## Match facilities to GIS features

Match OpenNEM facility records to the nearest Geoscience Australia power station or substation:

```python
from nemdb.geodata.matching import match_facilities_to_gis

matched = match_facilities_to_gis()
print(matched[["name", "gis_name", "match_type", "distance_m"]].head())
```

The matching uses a two-pass spatial join:

1. Each facility is matched to the nearest power station
2. Each facility is matched to the nearest substation
3. The closer match wins

You can provide pre-loaded data to avoid re-fetching:

```python
import asyncio
from nemdb.opennem.opennemapi import read_facilities
from nemdb.geodata import read_substations, read_major_powerstations

facilities = asyncio.run(read_facilities(network_id=["NEM"]))
powerstations = read_major_powerstations()
substations = read_substations()

matched = match_facilities_to_gis(
    facilities=facilities,
    powerstations=powerstations,
    substations=substations,
)
```

## Coordinate reference systems

- All data is fetched in **EPSG:4326** (WGS 84, geographic coordinates)
- Distance calculations use **EPSG:7856** (GDA2020 / MGA zone 56, metric)
- The cleaning pipeline converts to metric CRS internally and converts back

To work in metric coordinates:

```python
lines_metric = lines_clean.to_crs("EPSG:7856")
print(lines_metric.geometry.length.describe())  # lengths in meters
```
