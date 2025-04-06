import geopandas as gpd
import pandas as pd

from nemdb.utils import cache_to_parquet
from nemdb import Config
from nemdb import log

from . import transformations as tf

METRIC_CRS = "EPSG:7856"


@cache_to_parquet(
    Config.CACHE_DIR / "geodata" / "substations.parquet", type_=gpd.GeoDataFrame
)
def read_substations():
    """
    Fetches and returns geospatial data of substations.

    The function sends a request to a government API providing data on national
    electricity infrastructure, retrieves the data in GeoJSON format, and reads
    it into a GeoDataFrame using the GeoPandas library.

    Returns:
        GeoDataFrame: A GeoDataFrame containing the fetched substation data.
    """

    API_URL = "https://services.ga.gov.au/gis/rest/services/National_Electricity_Infrastructure/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson"
    log.info("Fetching substation data from %s", API_URL)
    gdf = gpd.read_file(API_URL)
    return gdf[
        gdf.state.isin(
            [
                "Victoria",
                "Queensland",
                "New South Wales",
                "Tasmania",
                "Australian Capital Territory",
                "South Australia",
            ]
        )
    ]


@cache_to_parquet(
    Config.CACHE_DIR / "geodata" / "transmission_lines.parquet", type_=gpd.GeoDataFrame
)
def _read_transmission_lines():
    """
    Reads in transmission line data.

    Returns:
        GeoDataFrame: A GeoDataFrame containing the fetched transmission line data.
    """
    tables = []
    for state in [
        "Victoria",
        "Queensland",
        "New%20South%20Wales",
        "Tasmania",
        "Australian%20Capital%20Territory",
        "South%20Australia",
    ]:
        log.info("Fetching lines data for state %s", state)
        api_url = "https://services.ga.gov.au/gis/rest/services/National_Electricity_Infrastructure/MapServer/2/query?where=state%20%3D%20'{state}'&outFields=class,name,operationalstatus,state,spatialconfidence,revised,st_length(shape),capacitykv,length_m&outSR=4326&f=geojson"
        tables.append(gpd.read_file(api_url.format(state=state)))
    return pd.concat(tables).reset_index(drop=True)


@cache_to_parquet(
    Config.CACHE_DIR / "geodata" / "transmission_lines_clean.parquet",
    type_=gpd.GeoDataFrame,
)
def read_transmission_lines(clean: bool = False):
    """
    Reads in transmission line data.

    If clean is set to True, attempts to clean the data by merging nearby lines and simplifying the geometry.

    Parameters
    ----------
    clean : bool, optional
        Whether to attempt to clean the transmission line data. Defaults to False.

    Returns
    -------
    GeoDataFrame
        A GeoDataFrame containing the fetched transmission line data.
    """
    gdf = _read_transmission_lines()
    if clean:
        log.info("Attempting to clean transmission lines")
        base_crs = gdf.crs
        gdf = gdf.to_crs(METRIC_CRS)
        gdf = tf.clean_transmission_lines(gdf)
        gdf = gdf.to_crs(base_crs)
    return gdf


@cache_to_parquet(
    Config.CACHE_DIR / "geodata" / "powerstations.parquet", type_=gpd.GeoDataFrame
)
def read_major_powerstations():
    tables = []
    for state in [
        "Victoria",
        "Queensland",
        "New%20South%20Wales",
        "Tasmania",
        "Australian%20Capital%20Territory",
        "South%20Australia",
    ]:
        api_url = "https://services.ga.gov.au/gis/rest/services/National_Electricity_Infrastructure/MapServer/1/query?where=state%20%3D%20'{state}'&outFields=*&f=geojson"
        tables.append(gpd.read_file(api_url.format(state=state)))
    return pd.concat(tables).reset_index(drop=True)


if __name__ == "__main__":
    gdf = read_major_powerstations()
    print(gdf.head())
    pass
