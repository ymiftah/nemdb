"""Unit tests for pandapower builder functions.

All tests use minimal synthetic data — no full NEM model build required.
"""

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import pytest
import shapely as shp

pytest.importorskip("pandapower")
import pandapower as pp

from nemdb.models.pandapower.connectivity_fallback import _validate_and_fix_connectivity
from nemdb.models.pandapower.diagnostics import _log_check_result, sanity_checks
from nemdb.models.pandapower.geo_utils import nearest_bus_pair
from nemdb.models.pandapower.line_params import _HVDC_INTERCONNECTORS
from nemdb.models.pandapower.network_builder import (
    _add_buses_to_network,
    _add_external_grids,
    _add_generators_to_network,
    _add_hvdc_interconnectors,
    _add_lines_to_network,
    _add_loads_to_network,
    _add_transformers_to_network,
)
from nemdb.models.visualize.common import _apply_map_layout, bus_location_lookup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

METRIC_CRS = "EPSG:7856"
GEO_CRS = "EPSG:4326"


def _make_buses_df(n=3, with_geo=True):
    """Minimal buses DataFrame as expected by _add_buses_to_network."""
    geo = [shp.Point(151.0 + i * 0.1, -33.8) for i in range(n)] if with_geo else [None] * n
    return pd.DataFrame(
        {
            "bus_id": [f"bus_{i}_220kv" for i in range(n)],
            "vn_kv": [220.0] * n,
            "in_service": [True] * n,
            "geodata": geo,
        }
    )


def _make_net_with_buses(n=3):
    """Build a pp network with n pre-added buses; return (net, bus_idx_map)."""
    net = pp.create_empty_network()
    buses_df = _make_buses_df(n)
    bus_idx_map = _add_buses_to_network(net, buses_df)
    return net, bus_idx_map, buses_df


# ---------------------------------------------------------------------------
# _add_buses_to_network
# ---------------------------------------------------------------------------


def test_add_buses_with_geodata():
    net = pp.create_empty_network()
    buses_df = _make_buses_df(3, with_geo=True)
    bus_idx_map = _add_buses_to_network(net, buses_df)

    assert len(net.bus) == 3
    assert set(bus_idx_map.keys()) == set(buses_df["bus_id"])


def test_add_buses_without_geodata():
    net = pp.create_empty_network()
    buses_df = _make_buses_df(2, with_geo=False)
    bus_idx_map = _add_buses_to_network(net, buses_df)

    assert len(net.bus) == 2
    assert len(bus_idx_map) == 2


def test_add_buses_mixed_geodata():
    net = pp.create_empty_network()
    buses_df = pd.DataFrame(
        {
            "bus_id": ["bus_0_220kv", "bus_1_220kv", "bus_2_220kv"],
            "vn_kv": [220.0, 220.0, 220.0],
            "in_service": [True, True, True],
            "geodata": [shp.Point(151.0, -33.8), None, shp.Point(152.0, -34.0)],
        }
    )
    bus_idx_map = _add_buses_to_network(net, buses_df)

    assert len(net.bus) == 3
    assert len(bus_idx_map) == 3


# ---------------------------------------------------------------------------
# _add_lines_to_network
# ---------------------------------------------------------------------------


def _make_lines_df(bus_ids):
    n = len(bus_ids) - 1
    return pd.DataFrame(
        {
            "name": [f"line_{i}" for i in range(n)],
            "from_bus": bus_ids[:-1],
            "to_bus": bus_ids[1:],
            "length_km": [50.0] * n,
            "in_service": [True] * n,
            "voltagekv": [220.0] * n,
        }
    )


def test_add_lines_valid():
    net, bus_idx_map, buses_df = _make_net_with_buses(3)
    lines_df = _make_lines_df(list(buses_df["bus_id"]))
    _add_lines_to_network(net, bus_idx_map, lines_df)

    assert len(net.line) == 2


def test_add_lines_filters_unmapped_buses():
    """Lines referencing buses not in bus_idx_map are silently filtered; if ALL are filtered, raise."""
    net, bus_idx_map, _buses_df = _make_net_with_buses(2)
    lines_df = pd.DataFrame(
        {
            "name": ["bad_line"],
            "from_bus": ["nonexistent_bus_a"],
            "to_bus": ["nonexistent_bus_b"],
            "length_km": [10.0],
            "in_service": [True],
            "voltagekv": [220.0],
        }
    )
    with pytest.raises(ValueError, match="No valid lines found"):
        _add_lines_to_network(net, bus_idx_map, lines_df)


def test_add_lines_empty_raises():
    net, bus_idx_map, _ = _make_net_with_buses(2)
    with pytest.raises(ValueError, match="No valid lines found"):
        _add_lines_to_network(
            net,
            bus_idx_map,
            pd.DataFrame(
                columns=["from_bus", "to_bus", "length_km", "name", "in_service", "voltagekv"]
            ),
        )


# ---------------------------------------------------------------------------
# _add_transformers_to_network
# ---------------------------------------------------------------------------


def _make_trafos_df(hv_bus, lv_bus):
    return pd.DataFrame(
        {
            "name": ["trafo_0"],
            "hv_bus": [hv_bus],
            "lv_bus": [lv_bus],
            "vn_hv_kv": [275.0],
            "vn_lv_kv": [220.0],
            "sn_mva": [1000.0],
            "vk_percent": [12.2],
            "vkr_percent": [0.25],
            "pfe_kw": [60.0],
            "i0_percent": [0.06],
            "in_service": [True],
        }
    )


def test_add_transformers_valid():
    net = pp.create_empty_network()
    b_hv = pp.create_bus(net, vn_kv=275.0, name="hv_bus")
    b_lv = pp.create_bus(net, vn_kv=220.0, name="lv_bus")
    bus_idx_map = {"hv_bus": b_hv, "lv_bus": b_lv}
    trafos_df = _make_trafos_df("hv_bus", "lv_bus")

    _add_transformers_to_network(net, bus_idx_map, trafos_df)

    assert len(net.trafo) == 1


def test_add_transformers_empty_raises():
    net, bus_idx_map, _ = _make_net_with_buses(2)
    trafos_df = _make_trafos_df("missing_hv", "missing_lv")
    with pytest.raises(ValueError, match="No valid transformers found"):
        _add_transformers_to_network(net, bus_idx_map, trafos_df)


# ---------------------------------------------------------------------------
# _add_generators_to_network
# ---------------------------------------------------------------------------


def _make_gens_df(bus_id):
    return pd.DataFrame(
        {
            "bus_id": [bus_id],
            "name": ["Test Gen"],
            "p_mw": [100.0],
            "max_p_mw": [200.0],
            "type": ["coal_black"],
            "in_service": [True],
        }
    )


def test_add_generators_valid():
    net, bus_idx_map, buses_df = _make_net_with_buses(2)
    gens_df = _make_gens_df(buses_df["bus_id"].iloc[0])
    _add_generators_to_network(net, bus_idx_map, gens_df)

    assert len(net.gen) == 1


def test_add_generators_empty_raises():
    net, bus_idx_map, _ = _make_net_with_buses(2)
    gens_df = _make_gens_df("nonexistent_bus")
    with pytest.raises(ValueError, match="No valid generators found"):
        _add_generators_to_network(net, bus_idx_map, gens_df)


def test_add_generators_with_code_column():
    """Verify the 'code' column path is exercised when present."""
    net, bus_idx_map, buses_df = _make_net_with_buses(2)
    gens_df = _make_gens_df(buses_df["bus_id"].iloc[0])
    gens_df["code"] = ["TESTGEN1"]
    _add_generators_to_network(net, bus_idx_map, gens_df)

    assert len(net.gen) == 1


# ---------------------------------------------------------------------------
# _add_loads_to_network
# ---------------------------------------------------------------------------


def _make_loads_df(bus_id):
    return pd.DataFrame(
        {
            "bus_id": [bus_id],
            "name": ["Test Substation"],
            "in_service": [True],
        }
    )


def test_add_loads_valid():
    net, bus_idx_map, buses_df = _make_net_with_buses(2)
    loads_df = _make_loads_df(buses_df["bus_id"].iloc[0])
    _add_loads_to_network(net, bus_idx_map, loads_df)

    assert len(net.load) == 1


def test_add_loads_empty_raises():
    net, bus_idx_map, _ = _make_net_with_buses(2)
    loads_df = _make_loads_df("nonexistent_bus")
    with pytest.raises(ValueError, match="No valid loads found"):
        _add_loads_to_network(net, bus_idx_map, loads_df)


# ---------------------------------------------------------------------------
# _add_external_grids
# ---------------------------------------------------------------------------


def _make_net_for_ext_grid():
    """Build a network with named loads matching the expected ext_grid substations."""
    net = pp.create_empty_network()
    names = [
        "Torrens Island A",
        "Thomastown",
        "George Town",
        "Sydney West",
        "South Pine",
    ]
    for name in names:
        b = pp.create_bus(net, vn_kv=275.0, name=name)
        pp.create_load(net, b, p_mw=0.0, name=name)
    return net


def test_add_external_grids_added():
    net = _make_net_for_ext_grid()
    _add_external_grids(net)

    assert len(net.ext_grid) == 5


def test_add_external_grids_missing_substation_raises():
    """If one target substation load is absent, ValueError is raised."""
    net = pp.create_empty_network()
    # Only one of the five substations present
    b = pp.create_bus(net, vn_kv=275.0, name="Torrens Island A")
    pp.create_load(net, b, p_mw=0.0, name="Torrens Island A")

    with pytest.raises(ValueError, match="Could not find load entry"):
        _add_external_grids(net)


def test_add_external_grids_empty_net_raises():
    net = pp.create_empty_network()
    with pytest.raises(ValueError):
        _add_external_grids(net)


# ---------------------------------------------------------------------------
# _add_hvdc_interconnectors
# ---------------------------------------------------------------------------


def _make_net_with_hvdc_lines():
    """Build a net containing the AC placeholders for all HVDC interconnectors."""
    net = pp.create_empty_network()
    for link in _HVDC_INTERCONNECTORS:
        for line_name in link["lines"]:
            b1 = pp.create_bus(net, vn_kv=220.0)
            b2 = pp.create_bus(net, vn_kv=220.0)
            pp.create_line_from_parameters(
                net,
                from_bus=b1,
                to_bus=b2,
                length_km=100.0,
                r_ohm_per_km=0.05,
                x_ohm_per_km=0.3,
                c_nf_per_km=10.0,
                max_i_ka=1.5,
                name=line_name,
                in_service=True,
            )
    return net


def test_add_hvdc_interconnectors_replaces_ac_with_dc():
    net = _make_net_with_hvdc_lines()
    _add_hvdc_interconnectors(net)

    expected_dc_segments = sum(len(link["lines"]) for link in _HVDC_INTERCONNECTORS)
    assert len(net.dcline) == expected_dc_segments
    # All original AC lines should now be out of service
    for link in _HVDC_INTERCONNECTORS:
        for line_name in link["lines"]:
            ac_line = net.line[net.line["name"] == line_name]
            assert len(ac_line) == 1
            assert not ac_line.iloc[0]["in_service"]


def test_add_hvdc_missing_line_raises():
    net = pp.create_empty_network()
    # No lines at all — any HVDC link lookup will fail
    with pytest.raises(ValueError, match="Could not find AC line segment"):
        _add_hvdc_interconnectors(net)


# ---------------------------------------------------------------------------
# _validate_and_fix_connectivity
# ---------------------------------------------------------------------------


def _make_split_model():
    """Two disconnected bus islands, each with geodata."""
    buses = pd.DataFrame(
        {
            "bus_id": ["bus_0_220kv", "bus_1_220kv", "bus_2_220kv", "bus_3_220kv"],
            "vn_kv": [220.0, 220.0, 220.0, 220.0],
            "in_service": [True, True, True, True],
            "geodata": [
                shp.Point(151.0, -33.8),
                shp.Point(151.1, -33.9),
                shp.Point(152.0, -34.5),  # physically separated island
                shp.Point(152.1, -34.6),
            ],
        }
    )
    lines = pd.DataFrame(
        {
            "name": ["line_0_1", "line_2_3"],
            "from_bus": ["bus_0_220kv", "bus_2_220kv"],
            "to_bus": ["bus_1_220kv", "bus_3_220kv"],
            "length_km": [10.0, 10.0],
            "in_service": [True, True],
            "voltagekv": [220.0, 220.0],
        }
    )
    trafos = pd.DataFrame(columns=["hv_bus", "lv_bus", "in_service"])
    gens = pd.DataFrame(columns=["bus_id"])
    loads = pd.DataFrame(columns=["bus_id"])
    return {"buses": buses, "lines": lines, "trafos": trafos, "gens": gens, "loads": loads}


def test_validate_and_fix_connectivity_adds_synthetic_line():
    model = _make_split_model()
    fixed, diagnostics = _validate_and_fix_connectivity(model)

    assert diagnostics["added_lines"] >= 1
    assert len(fixed["lines"]) > 2  # original 2 + at least 1 synthetic


def test_validate_and_fix_connectivity_already_connected():
    buses = pd.DataFrame(
        {
            "bus_id": ["bus_0_220kv", "bus_1_220kv"],
            "vn_kv": [220.0, 220.0],
            "in_service": [True, True],
            "geodata": [shp.Point(151.0, -33.8), shp.Point(151.1, -33.9)],
        }
    )
    lines = pd.DataFrame(
        {
            "name": ["line_0"],
            "from_bus": ["bus_0_220kv"],
            "to_bus": ["bus_1_220kv"],
            "length_km": [10.0],
            "in_service": [True],
            "voltagekv": [220.0],
        }
    )
    trafos = pd.DataFrame(columns=["hv_bus", "lv_bus", "in_service"])
    gens = pd.DataFrame(columns=["bus_id"])
    loads = pd.DataFrame(columns=["bus_id"])
    model = {"buses": buses, "lines": lines, "trafos": trafos, "gens": gens, "loads": loads}

    fixed, diagnostics = _validate_and_fix_connectivity(model)

    assert diagnostics["added_lines"] == 0
    assert len(fixed["lines"]) == 1


# ---------------------------------------------------------------------------
# sanity_checks / _log_check_result
# ---------------------------------------------------------------------------


def _make_valid_net():
    net = pp.create_empty_network()
    b1 = pp.create_bus(net, vn_kv=220.0)
    b2 = pp.create_bus(net, vn_kv=220.0)
    pp.create_ext_grid(net, b1)
    pp.create_load(net, b2, p_mw=5.0)
    pp.create_line_from_parameters(
        net,
        b1,
        b2,
        length_km=50.0,
        r_ohm_per_km=0.059,
        x_ohm_per_km=0.285,
        c_nf_per_km=10.0,
        max_i_ka=0.96,
    )
    return net


def test_sanity_checks_returns_dict():
    net = _make_valid_net()
    results = sanity_checks(net)
    assert isinstance(results, dict)
    assert "disconnected_elements" in results
    assert "long_high_impedance_lines" in results


def test_sanity_checks_flags_no_ext_grid():
    net = pp.create_empty_network()
    b1 = pp.create_bus(net, vn_kv=220.0)
    b2 = pp.create_bus(net, vn_kv=220.0)
    pp.create_load(net, b2, p_mw=5.0)
    pp.create_line_from_parameters(
        net,
        b1,
        b2,
        length_km=10.0,
        r_ohm_per_km=0.059,
        x_ohm_per_km=0.285,
        c_nf_per_km=10.0,
        max_i_ka=0.96,
    )
    results = sanity_checks(net)
    # no_ext_grid check should flag an issue
    assert results.get("no_ext_grid") not in (None, [], {})


def test_log_check_result_string_error():
    ref = [False]
    _log_check_result("test", "some error message", ref)
    assert ref[0] is True


def test_log_check_result_dict_with_issues():
    ref = [False]
    _log_check_result("test", {"bus_1": "mismatch"}, ref)
    assert ref[0] is True


def test_log_check_result_dict_empty():
    ref = [False]
    _log_check_result("test", {}, ref)
    assert ref[0] is False


def test_log_check_result_list_with_issues():
    ref = [False]
    _log_check_result("test", [1, 2, 3], ref)
    assert ref[0] is True


def test_log_check_result_list_many_issues():
    """Exercises the '>5 more' truncation branch."""
    ref = [False]
    _log_check_result("test", list(range(10)), ref)
    assert ref[0] is True


def test_log_check_result_empty_list():
    ref = [False]
    _log_check_result("test", [], ref)
    assert ref[0] is False


def test_log_check_result_dict_many_entries():
    """Exercises the '>3 more' truncation branch for dict results."""
    ref = [False]
    issues = {f"key_{i}": f"val_{i}" for i in range(6)}
    _log_check_result("test", issues, ref)
    assert ref[0] is True


# ---------------------------------------------------------------------------
# geo_utils.nearest_bus_pair
# ---------------------------------------------------------------------------


def _metric_gdf(points: list, bus_ids: list) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame(
        {"bus_id": bus_ids, "geometry": points},
        geometry="geometry",
        crs=GEO_CRS,
    ).to_crs(METRIC_CRS)
    return gdf


def test_nearest_bus_pair_finds_closest():
    src = _metric_gdf([shp.Point(151.0, -33.8)], ["island_bus"])
    cand = _metric_gdf(
        [shp.Point(151.1, -33.9), shp.Point(153.0, -36.0)],
        ["near_bus", "far_bus"],
    )
    src_id, cand_id, dist = nearest_bus_pair(src, cand, max_distance_m=float("inf"))
    assert src_id == "island_bus"
    assert cand_id == "near_bus"
    assert dist < 20_000  # ~15 km


def test_nearest_bus_pair_empty_source():
    src = _metric_gdf([], [])
    cand = _metric_gdf([shp.Point(151.0, -33.8)], ["bus_a"])
    src_id, cand_id, dist = nearest_bus_pair(src, cand, max_distance_m=float("inf"))
    assert src_id is None
    assert cand_id is None
    assert dist == float("inf")


def test_nearest_bus_pair_beyond_max_distance():
    src = _metric_gdf([shp.Point(151.0, -33.8)], ["src_bus"])
    cand = _metric_gdf([shp.Point(155.0, -38.0)], ["far_bus"])  # ~600 km away
    src_id, _cand_id, _dist = nearest_bus_pair(src, cand, max_distance_m=1000.0)
    assert src_id is None


# ---------------------------------------------------------------------------
# visualize/common.py
# ---------------------------------------------------------------------------


def test_bus_location_lookup_with_geodata():
    buses_df = pd.DataFrame(
        {
            "bus_id": ["bus_a", "bus_b"],
            "geodata": [shp.Point(151.0, -33.8), shp.Point(144.9, -37.8)],
        }
    )
    result = bus_location_lookup(buses_df)
    assert result["bus_a"] == pytest.approx((151.0, -33.8))
    assert result["bus_b"] == pytest.approx((144.9, -37.8))


def test_bus_location_lookup_skips_missing_geodata():
    buses_df = pd.DataFrame(
        {
            "bus_id": ["bus_a", "bus_b"],
            "geodata": [shp.Point(151.0, -33.8), None],
        }
    )
    result = bus_location_lookup(buses_df)
    assert "bus_a" in result
    assert "bus_b" not in result


def test_bus_location_lookup_skips_nan_geodata():
    buses_df = pd.DataFrame(
        {
            "bus_id": ["bus_a"],
            "geodata": [float("nan")],
        }
    )
    result = bus_location_lookup(buses_df)
    assert result == {}


def test_apply_map_layout_sets_title():
    fig = go.Figure()
    _apply_map_layout(fig, "carto-positron", -27.0, 133.0, 4, 800, "Test Title")
    assert fig.layout.title.text == "Test Title"
    assert fig.layout.height == 800
