import geopandas as gpd


def nearest_bus_pair(
    source_buses: gpd.GeoDataFrame,
    candidate_buses: gpd.GeoDataFrame,
    *,
    max_distance_m: float,
    geometry_col: str = "geometry",
) -> tuple[str | None, str | None, float]:
    """Return the closest (source_bus_id, candidate_bus_id, distance_m) within max_distance_m.

    Both inputs must be GeoDataFrames in the same projected (metric) CRS.
    Callers apply any voltage/group filtering to candidate_buses; this function
    has no domain logic beyond "nearest point within radius".
    Returns (None, None, inf) when no pair is found.
    """
    if source_buses.empty or candidate_buses.empty:
        return None, None, float("inf")
    joined = gpd.sjoin_nearest(
        source_buses.set_geometry(geometry_col),
        candidate_buses.set_geometry(geometry_col),
        max_distance=max_distance_m,
        distance_col="_dist",
        lsuffix="src",
        rsuffix="cand",
    )
    if joined.empty:
        return None, None, float("inf")
    best = joined.loc[joined["_dist"].idxmin()]
    return best.get("bus_id_src", best.name), best.get("bus_id_cand"), float(best["_dist"])
