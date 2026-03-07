"""Tests for nemdb.geodata.matching.match_facilities_to_gis.

Covers:
- Unit-level behaviour with injected fixture data (fast, no IO).
- The ``source="pooch"`` path: verifies read_facilities_cached is called.
- The ``source="api"`` path: verifies read_facilities (API) is called.
Both paths must produce identically-shaped, correctly-typed output.
"""

from unittest.mock import patch

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point

from nemdb.geodata.matching import match_facilities_to_gis

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

_FACILITIES_DF = pl.DataFrame(
    {
        "facility_code": ["ERARING", "BAYSWATER"],
        "code": ["ERARING1", "BAYSWATER1"],
        "name": ["Eraring", "Bayswater"],
        "fueltech_id": ["coal_black", "coal_black"],
        "status_id": ["operating", "operating"],
        "latitude": [-33.07, -32.43],
        "longitude": [151.51, 150.97],
        "network_id": ["NEM", "NEM"],
        "capacity_registered": [2880.0, 2640.0],
        "capacity_maximum": [2880.0, 2640.0],
    }
)

# Minimal GIS layers — one feature each, close to the facility coordinates.
_POWERSTATIONS = gpd.GeoDataFrame(
    {"name": ["Eraring Power Station"], "state": ["New South Wales"]},
    geometry=[Point(151.51, -33.07)],
    crs="EPSG:4326",
)

_SUBSTATIONS = gpd.GeoDataFrame(
    {"name": ["Bayswater Substation"], "state": ["New South Wales"]},
    geometry=[Point(150.97, -32.43)],
    crs="EPSG:4326",
)


def _assert_output_shape(result: gpd.GeoDataFrame) -> None:
    """Common assertions on the return value of match_facilities_to_gis."""
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 2
    for col in ("gis_name", "match_type", "distance_m", "geometry"):
        assert col in result.columns, f"missing column: {col}"
    assert set(result["match_type"].unique()).issubset({"powerstation", "substation"})
    assert (result["distance_m"] >= 0).all()


# ---------------------------------------------------------------------------
# Unit test: facilities supplied directly (no source IO)
# ---------------------------------------------------------------------------


def test_match_facilities_with_injected_data():
    """When facilities/GIS layers are passed directly, no IO occurs."""
    result = match_facilities_to_gis(
        facilities=_FACILITIES_DF,
        powerstations=_POWERSTATIONS,
        substations=_SUBSTATIONS,
    )
    _assert_output_shape(result)


# ---------------------------------------------------------------------------
# source="pooch" — read_facilities_cached is called
# ---------------------------------------------------------------------------


def test_match_facilities_source_pooch_calls_cached(tmp_path):
    """source='pooch' delegates to read_facilities_cached, not the API."""
    with (
        patch(
            "nemdb.geodata.matching.read_facilities_cached",
            return_value=_FACILITIES_DF,
        ) as mock_cached,
        patch("nemdb.geodata.matching.read_facilities") as mock_api,
    ):
        result = match_facilities_to_gis(
            powerstations=_POWERSTATIONS,
            substations=_SUBSTATIONS,
            source="pooch",
        )

    mock_cached.assert_called_once()
    mock_api.assert_not_called()
    _assert_output_shape(result)


# ---------------------------------------------------------------------------
# source="api" — read_facilities (async) is called
# ---------------------------------------------------------------------------


def test_match_facilities_source_api_calls_live():
    """source='api' delegates to asyncio.run(read_facilities(...)), not pooch."""
    with (
        patch("nemdb.geodata.matching.read_facilities_cached") as mock_cached,
        patch(
            "nemdb.geodata.matching.asyncio.run",
            return_value=_FACILITIES_DF,
            # asyncio.run receives a coroutine; cancel it to avoid the
            # "coroutine never awaited" RuntimeWarning from joblib's cache wrapper.
            side_effect=lambda coro: (_FACILITIES_DF, coro.close() or None)[0],
        ) as mock_run,
    ):
        result = match_facilities_to_gis(
            powerstations=_POWERSTATIONS,
            substations=_SUBSTATIONS,
            source="api",
        )

    mock_cached.assert_not_called()
    mock_run.assert_called_once()
    _assert_output_shape(result)


# ---------------------------------------------------------------------------
# Both sources produce identical structure for identical input data
# ---------------------------------------------------------------------------


def test_pooch_and_api_sources_return_same_structure():
    """Pooch and API paths produce the same column schema and row count."""
    with patch(
        "nemdb.geodata.matching.read_facilities_cached",
        return_value=_FACILITIES_DF,
    ):
        result_pooch = match_facilities_to_gis(
            powerstations=_POWERSTATIONS,
            substations=_SUBSTATIONS,
            source="pooch",
        )

    with patch(
        "nemdb.geodata.matching.asyncio.run",
        side_effect=lambda coro: (_FACILITIES_DF, coro.close() or None)[0],
    ):
        result_api = match_facilities_to_gis(
            powerstations=_POWERSTATIONS,
            substations=_SUBSTATIONS,
            source="api",
        )

    assert list(result_pooch.columns) == list(result_api.columns)
    assert len(result_pooch) == len(result_api)


# ---------------------------------------------------------------------------
# Integration tests (require network + pooch cache)
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_match_facilities_pooch_integration():
    """End-to-end: downloads facilities parquet via pooch and matches to GIS."""
    result = match_facilities_to_gis(source="pooch")
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) > 100
    assert "gis_name" in result.columns
    assert "match_type" in result.columns
