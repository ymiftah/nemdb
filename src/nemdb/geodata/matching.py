import asyncio

import geopandas as gpd
import pandas as pd
import polars as pl

from nemdb.geodata.geodata import read_major_powerstations, read_substations
from nemdb.opennem.opennemapi import read_facilities

METRIC_CRS = "EPSG:7856"


def facilities_to_gdf(facilities: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    Converts a Polars DataFrame of facilities to a GeoPandas GeoDataFrame.

    Args:
        facilities: Polars DataFrame with 'latitude' and 'longitude' columns.

    Returns:
        GeoDataFrame with Point geometry.
    """
    # Filter out rows without coordinates
    facilities_with_coords = facilities.filter(
        pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null()
    )

    gdf = gpd.GeoDataFrame(
        facilities_with_coords.to_pandas(),
        geometry=gpd.points_from_xy(
            facilities_with_coords["longitude"], facilities_with_coords["latitude"]
        ),
        crs="EPSG:4326",
    )
    return gdf


def find_nearest_gis_feature(
    facilities_gdf: gpd.GeoDataFrame,
    targets: gpd.GeoDataFrame,
    distance_col: str = "distance_m",
) -> gpd.GeoDataFrame:
    """
    For each facility, finds the nearest target GIS feature using sjoin_nearest.

    Both inputs are reprojected to a metric CRS for accurate distance calculation.

    Args:
        facilities_gdf: GeoDataFrame of facilities (in any CRS).
        targets: GeoDataFrame of target features (in any CRS).
        distance_col: Name of the output distance column.

    Returns:
        GeoDataFrame with facility rows joined to their nearest target and a
        distance column in meters. Preserves the original facility index order.
    """
    facilities_metric = facilities_gdf.to_crs(METRIC_CRS)
    targets_metric = targets.to_crs(METRIC_CRS)

    result = gpd.sjoin_nearest(
        facilities_metric,
        targets_metric,
        how="left",
        distance_col=distance_col,
        lsuffix="facility",
        rsuffix="target",
    )

    # Keep only the first (nearest) match per facility
    # sjoin_nearest preserves the left index, so group by that index
    # and keep only the first row (nearest match) for each facility
    if isinstance(result.index, pd.MultiIndex):
        result = result.reset_index()
        result = result.drop_duplicates(subset=result.columns[0], keep="first")
        result = result.set_index(result.columns[0])
    else:
        result = result.reset_index(drop=False)
        idx_col = result.columns[0] if len(result.columns) > 0 else None
        if idx_col:
            result = result.drop_duplicates(subset=[idx_col], keep="first")
            result = result.set_index(idx_col)

    return result.to_crs(facilities_gdf.crs)


def find_nearest_powerstation(
    facilities: pl.DataFrame, powerstations: gpd.GeoDataFrame
) -> pl.DataFrame:
    """
    For each facility, finds the nearest powerstation.

    Args:
        facilities: Polars DataFrame of facilities.
        powerstations: GeoPandas GeoDataFrame of powerstations.

    Returns:
        Polars DataFrame containing facilities joined with their nearest powerstation
        and the distance in meters.
    """
    facilities_gdf = facilities_to_gdf(facilities)

    # Standardize CRS to metric for accurate distance
    facilities_gdf_metric = facilities_gdf.to_crs(METRIC_CRS)
    powerstations_metric = powerstations.to_crs(METRIC_CRS)

    # Find nearest powerstation for each facility
    nearest_stations = gpd.sjoin_nearest(
        facilities_gdf_metric,
        powerstations_metric,
        how="left",
        distance_col="distance_to_station_m",
        lsuffix="facility",
        rsuffix="station",
    )

    # Convert back to polars and remove geometry
    results = pl.from_pandas(pd.DataFrame(nearest_stations).drop(columns=["geometry"]))
    return results


def _aggregate_facilities(facilities: pl.DataFrame) -> pl.DataFrame:
    """Aggregate unit-level facility rows to one row per facility.

    Sums capacity columns across units and keeps the first value
    for descriptive columns (name, fueltech, status, coordinates).
    """
    return facilities.group_by(["facility_code", "code"]).agg(
        pl.col("name").first(),
        pl.col("fueltech_id").first(),
        pl.col("status_id").first(),
        pl.col("latitude").first(),
        pl.col("longitude").first(),
        pl.col("network_id").first(),
        pl.col("capacity_registered").sum().alias("capacity_registered_mw"),
        pl.col("capacity_maximum").sum().alias("capacity_maximum_mw"),
    )


def match_facilities_to_gis(
    facilities: pl.DataFrame | None = None,
    powerstations: gpd.GeoDataFrame | None = None,
    substations: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """Match OpenNEM facilities to the nearest GIS power station or substation.

    Uses a two-pass approach: first matches against powerstations, then against
    substations, and keeps whichever match is closer for each facility.

    Args:
        facilities: OpenNEM facilities DataFrame (fetched if None).
        powerstations: GA powerstations GeoDataFrame (fetched if None).
        substations: GA substations GeoDataFrame (fetched if None).

    Returns:
        GeoDataFrame with facility info plus ``gis_name``, ``match_type``
        ("powerstation" or "substation"), and ``distance_m``.
    """
    if facilities is None:
        facilities = asyncio.run(read_facilities(network_id=["NEM"]))
    if powerstations is None:
        powerstations = read_major_powerstations()
    if substations is None:
        substations = read_substations()
    agg = _aggregate_facilities(facilities)
    fac_gdf = facilities_to_gdf(agg)

    # Pass 1: match against powerstations
    ps_targets = powerstations[["name", "geometry"]].rename(columns={"name": "gis_name"})
    matched_ps = find_nearest_gis_feature(fac_gdf, ps_targets, distance_col="distance_m")
    matched_ps = matched_ps.rename(
        columns={"gis_name": "ps_gis_name", "distance_m": "ps_distance_m"}
    )

    # Pass 2: match against substations
    sub_targets = substations[["name", "geometry"]].rename(columns={"name": "gis_name"})
    matched_sub = find_nearest_gis_feature(fac_gdf, sub_targets, distance_col="distance_m")
    matched_sub = matched_sub.rename(columns={"gis_name": "sub_gis_name"})
    matched_sub = matched_sub.rename(columns={"distance_m": "sub_distance_m"})

    # Combine: for each facility, pick the closer match
    fac_gdf = fac_gdf.copy()

    # Reset indices to align matching results with facility GeoDataFrame
    matched_ps = matched_ps.reset_index(drop=True)
    matched_sub = matched_sub.reset_index(drop=True)
    fac_gdf = fac_gdf.reset_index(drop=True)

    fac_gdf["ps_gis_name"] = matched_ps["ps_gis_name"].values
    fac_gdf["ps_distance_m"] = matched_ps["ps_distance_m"].values
    fac_gdf["sub_gis_name"] = matched_sub["sub_gis_name"].values
    fac_gdf["sub_distance_m"] = matched_sub["sub_distance_m"].values

    ps_closer = fac_gdf["ps_distance_m"] <= fac_gdf["sub_distance_m"]

    fac_gdf["gis_name"] = fac_gdf["ps_gis_name"].where(ps_closer, fac_gdf["sub_gis_name"])
    fac_gdf["match_type"] = "powerstation"
    fac_gdf.loc[~ps_closer, "match_type"] = "substation"
    fac_gdf["distance_m"] = fac_gdf["ps_distance_m"].where(ps_closer, fac_gdf["sub_distance_m"])

    fac_gdf = fac_gdf.drop(
        columns=["ps_gis_name", "ps_distance_m", "sub_gis_name", "sub_distance_m"]
    )
    return fac_gdf
