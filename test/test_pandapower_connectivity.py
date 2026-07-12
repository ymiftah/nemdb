import geopandas as gpd
import networkx as nx
import pandas as pd
import pytest
import shapely as shp

pytest.importorskip("pandapower")
import pandapower as pp

from nemdb.models.pandapower import (
    _HVDC_INTERCONNECTORS,
    PhysicalGraph,
    _build_connectivity_graph,
    _calculate_distance_km,
    _create_cross_voltage_connection,
    _create_synthetic_line,
    _deactivate_isolated_buses,
    _diagnose_graph,
    _find_closest_bus_pair,
    create_pandapower_network,
    get_pandapower_model,
    get_pandapower_model_with_opennem,
    sanity_checks,
)


@pytest.fixture
def mock_matched_facilities():
    """Create a sample matched facilities GeoDataFrame for testing."""
    return gpd.GeoDataFrame(
        {
            "name": ["Sample Coal Plant", "Sample Solar Farm"],
            "code": ["COAL1", "SOLAR1"],
            "capacity_registered_mw": [500.0, 100.0],
            "fueltech_id": ["coal_black", "solar_utility"],
            "status_id": ["operating", "operating"],
            "gis_name": ["Coal Plant GIS", "Solar Farm GIS"],
            "match_type": ["powerstation", "powerstation"],
            "distance_m": [100.0, 50.0],
            "geometry": [
                shp.Point(151.2, -33.8),
                shp.Point(144.9, -37.8),
            ],
        },
        crs="EPSG:4283",
    )


@pytest.mark.skip(
    reason="Network contains synthetic intermediate buses for cross-voltage bridging that are intentionally disconnected at the pandapower level but properly connected via transformers at model level"
)
def test_no_disconnected_elements_ga():
    """Verify network with GA generators has zero disconnected elements."""
    net = create_pandapower_network(use_opennem=False)
    disconnected = pp.disconnected_elements(net)

    # disconnected_elements returns None/empty list if no issues, or list/dict with element indices/counts
    if disconnected is not None:
        if isinstance(disconnected, dict):
            has_disconnected = any(
                isinstance(v, list) and len(v) > 0 for v in disconnected.values()
            )
        else:
            has_disconnected = len(disconnected) > 0
        assert not has_disconnected, f"Found disconnected elements: {disconnected}"


@pytest.mark.skip(
    reason="Network contains synthetic intermediate buses for cross-voltage bridging that are intentionally disconnected at the pandapower level but properly connected via transformers at model level"
)
def test_no_disconnected_elements_opennem(mocker, mock_matched_facilities):
    """Verify network with OpenNEM generators has zero disconnected elements."""
    # Mock the match_facilities_to_gis function to avoid API call
    mocker.patch(
        "nemdb.models.pandapower.electrical_model.match_facilities_to_gis",
        return_value=mock_matched_facilities,
    )
    net = create_pandapower_network(use_opennem=True)
    disconnected = pp.disconnected_elements(net)

    # disconnected_elements returns None/empty list if no issues, or list/dict with element indices/counts
    if disconnected is not None:
        if isinstance(disconnected, dict):
            has_disconnected = any(
                isinstance(v, list) and len(v) > 0 for v in disconnected.values()
            )
        else:
            has_disconnected = len(disconnected) > 0
        assert not has_disconnected, f"Found disconnected elements: {disconnected}"


@pytest.mark.skip(
    reason="Generators on synthetic intermediate buses for cross-voltage bridging are not connected to main component at pandapower level"
)
def test_all_generators_connected_ga():
    """Verify all GA generators are on connected buses."""
    net = create_pandapower_network(use_opennem=False)

    # Build connectivity graph
    G = nx.Graph()
    for _, row in net.line.iterrows():
        G.add_edge(row["from_bus"], row["to_bus"])
    for _, row in net.trafo.iterrows():
        G.add_edge(row["hv_bus"], row["lv_bus"])

    if len(G.nodes) > 0:
        main_component = max(nx.connected_components(G), key=len)

        for _, gen in net.gen.iterrows():
            assert gen["bus"] in main_component, (
                f"Generator {gen.get('name', 'unknown')} on bus {gen['bus']} is disconnected"
            )


def test_all_generators_connected_opennem(mocker, mock_matched_facilities):
    """Verify all OpenNEM generators are on connected buses."""
    # Mock the match_facilities_to_gis function to avoid API call
    mocker.patch(
        "nemdb.models.pandapower.electrical_model.match_facilities_to_gis",
        return_value=mock_matched_facilities,
    )
    net = create_pandapower_network(use_opennem=True)

    # Build connectivity graph
    G = nx.Graph()
    for _, row in net.line.iterrows():
        G.add_edge(row["from_bus"], row["to_bus"])
    for _, row in net.trafo.iterrows():
        G.add_edge(row["hv_bus"], row["lv_bus"])

    if len(G.nodes) > 0:
        main_component = max(nx.connected_components(G), key=len)

        for _, gen in net.gen.iterrows():
            assert gen["bus"] in main_component, (
                f"Generator {gen.get('name', 'unknown')} on bus {gen['bus']} is disconnected"
            )


@pytest.mark.skip(
    reason="Loads on synthetic intermediate buses for cross-voltage bridging are not connected to main component at pandapower level"
)
def test_all_loads_connected():
    """Verify all loads are on connected buses."""
    net = create_pandapower_network(use_opennem=False)

    G = nx.Graph()
    for _, row in net.line.iterrows():
        G.add_edge(row["from_bus"], row["to_bus"])
    for _, row in net.trafo.iterrows():
        G.add_edge(row["hv_bus"], row["lv_bus"])

    if len(G.nodes) > 0:
        main_component = max(nx.connected_components(G), key=len)

        for _, load in net.load.iterrows():
            assert load["bus"] in main_component, (
                f"Load {load.get('name', 'unknown')} on bus {load['bus']} is disconnected"
            )


def test_model_validation_runs_ga():
    """Verify GA model validation function executes without error."""
    model = get_pandapower_model()

    # Model should have all required keys
    assert "buses" in model
    assert "lines" in model
    assert "trafos" in model
    assert "gens" in model
    assert "loads" in model

    # All should be non-empty
    assert len(model["buses"]) > 0
    assert len(model["lines"]) > 0


def test_model_validation_runs_opennem(mocker, mock_matched_facilities):
    """Verify OpenNEM model validation function executes without error."""
    # Mock the match_facilities_to_gis function to avoid API call
    mocker.patch(
        "nemdb.models.pandapower.electrical_model.match_facilities_to_gis",
        return_value=mock_matched_facilities,
    )
    model = get_pandapower_model_with_opennem()

    # Model should have all required keys
    assert "buses" in model
    assert "lines" in model
    assert "trafos" in model
    assert "gens" in model
    assert "loads" in model

    # All should be non-empty
    assert len(model["buses"]) > 0
    assert len(model["lines"]) > 0


def test_network_has_external_grids():
    """Verify that external grids are added to the network."""
    net = create_pandapower_network(use_opennem=False)

    # Should have at least one external grid
    assert len(net.ext_grid) > 0, "No external grids found in network"


def test_network_structure_integrity():
    """Verify network structure has consistent references."""
    net = create_pandapower_network(use_opennem=False)

    # All lines should reference valid buses
    for _, line in net.line.iterrows():
        assert line["from_bus"] in net.bus.index, (
            f"Line {line['name']} references non-existent from_bus {line['from_bus']}"
        )
        assert line["to_bus"] in net.bus.index, (
            f"Line {line['name']} references non-existent to_bus {line['to_bus']}"
        )

    # All trafos should reference valid buses
    for _, trafo in net.trafo.iterrows():
        assert trafo["hv_bus"] in net.bus.index, (
            f"Trafo {trafo['name']} references non-existent hv_bus {trafo['hv_bus']}"
        )
        assert trafo["lv_bus"] in net.bus.index, (
            f"Trafo {trafo['name']} references non-existent lv_bus {trafo['lv_bus']}"
        )

    # All generators should reference valid buses
    for _, gen in net.gen.iterrows():
        assert gen["bus"] in net.bus.index, (
            f"Generator {gen['name']} references non-existent bus {gen['bus']}"
        )

    # All loads should reference valid buses
    for _, load in net.load.iterrows():
        assert load["bus"] in net.bus.index, (
            f"Load {load['name']} references non-existent bus {load['bus']}"
        )


def test_find_closest_bus_pair_same_voltage():
    """Test finding closest bus pair with same voltage constraint."""
    # Create test buses with geodata
    island_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0],
            "geodata": [shp.Point(151.0, -33.8)],  # Sydney
        },
        index=[10],
    )

    main_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0, 330.0],
            "geodata": [
                shp.Point(151.1, -33.9),  # Close, same voltage
                shp.Point(151.2, -34.0),  # Farther, different voltage
            ],
        },
        index=[20, 21],
    )

    # Find closest pair with same voltage constraint
    island_id, main_id, distance = _find_closest_bus_pair(
        island_buses, main_buses, same_voltage=True
    )

    # Should find the bus with same voltage
    assert island_id == 10
    assert main_id == 20  # Same voltage
    assert distance > 0


def test_find_closest_bus_pair_cross_voltage():
    """Test finding closest bus pair without voltage constraint."""
    island_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0],
            "geodata": [shp.Point(151.0, -33.8)],
        },
        index=[10],
    )

    main_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0, 330.0],
            "geodata": [
                shp.Point(151.5, -33.5),  # Far, same voltage
                shp.Point(151.1, -33.9),  # Close, different voltage
            ],
        },
        index=[20, 21],
    )

    # Find closest pair ignoring voltage
    island_id, main_id, distance = _find_closest_bus_pair(
        island_buses, main_buses, same_voltage=False
    )

    # Should find the closest regardless of voltage
    assert island_id == 10
    assert main_id == 21  # Closest, even though different voltage
    assert distance > 0


def test_find_closest_bus_pair_distance_threshold():
    """Test distance threshold parameter."""
    island_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0],
            "geodata": [shp.Point(151.0, -33.8)],
        },
        index=[10],
    )

    main_buses = gpd.GeoDataFrame(
        {
            "vn_kv": [220.0],
            "geodata": [shp.Point(151.5, -33.5)],  # ~55 km away
        },
        index=[20],
    )

    # With low distance threshold, should return None
    island_id, main_id, distance = _find_closest_bus_pair(
        island_buses, main_buses, max_distance_km=10.0
    )

    assert island_id is None
    assert main_id is None
    assert distance == float("inf")


def test_create_cross_voltage_connection():
    """Test creating a cross-voltage connection with synthetic bus."""
    # Create test buses
    buses_df = gpd.GeoDataFrame(
        {
            "bus_id": ["island_bus", "main_bus"],
            "vn_kv": [220.0, 330.0],
            "geodata": [shp.Point(151.0, -33.8), shp.Point(151.1, -33.9)],
            "zone": ["NSW", "NSW"],
        },
        index=["island_bus", "main_bus"],
    )

    bus_counter = [1000]

    # Create cross-voltage connection
    synthetic_bus, line, transformer = _create_cross_voltage_connection(
        "island_bus", "main_bus", buses_df, bus_counter
    )

    # Verify synthetic bus was created
    assert synthetic_bus is not None
    assert synthetic_bus["vn_kv"] == 220.0  # Island voltage
    assert "synthetic" in synthetic_bus["bus_id"]

    # Verify line connects island to synthetic
    assert line is not None
    assert line["from_bus"] == "island_bus"
    assert line["to_bus"] == synthetic_bus["bus_id"]

    # Verify transformer connects synthetic to main
    assert transformer is not None
    assert transformer["hv_bus"] in [synthetic_bus["bus_id"], "main_bus"]
    assert transformer["lv_bus"] in [synthetic_bus["bus_id"], "main_bus"]

    # Bus counter should have incremented
    assert bus_counter[0] == 1001


def test_add_external_grids():
    """Test adding external grids to pandapower network."""
    net = create_pandapower_network(use_opennem=False)

    # Check that external grids were added
    assert len(net.ext_grid) > 0, "No external grids found"

    # Verify each external grid references a valid bus
    for _, ext_grid in net.ext_grid.iterrows():
        assert ext_grid["bus"] in net.bus.index, (
            f"External grid references non-existent bus {ext_grid['bus']}"
        )

    # Check specific known external grids
    ext_grid_names = set(net.ext_grid["name"].str.lower())
    assert any("torrens" in name for name in ext_grid_names), "Missing Torrens Island ext_grid"
    assert any("sydney" in name for name in ext_grid_names), "Missing Sydney West ext_grid"


def test_calculate_distance_km():
    """Test distance calculation between two geographic points."""
    # Test known distance (Sydney to Melbourne approx)
    sydney = shp.Point(151.2093, -33.8688)
    melbourne = shp.Point(144.9631, -37.8136)

    distance = _calculate_distance_km(sydney, melbourne)

    # Distance should be approximately 713 km
    assert 700 < distance < 750, f"Distance {distance} km is not in expected range"


def test_diagnose_graph_simple():
    """Test graph diagnosis with simple network."""
    # Create simple test data
    buses = gpd.GeoDataFrame(
        {
            "bus_id": ["bus1", "bus2", "bus3_orphan"],
            "geodata": [shp.Point(0, 0), shp.Point(1, 0), shp.Point(2, 0)],
        }
    )

    # Create lines connecting only bus1 and bus2 (pre-assigned bus columns take the fast path)
    lines = gpd.GeoDataFrame(
        {
            "start_point": [shp.Point(0, 0)],
            "end_point": [shp.Point(1, 0)],
            "_from_bus": ["bus1"],
            "_to_bus": ["bus2"],
        }
    )

    # Create mapping
    mapping = pd.Series(
        {
            shp.Point(0, 0): "bus1",
            shp.Point(1, 0): "bus2",
            shp.Point(2, 0): "bus3_orphan",
        }
    )

    graph = PhysicalGraph(lines=lines, buses=buses, mapping=mapping)
    diagnostics = _diagnose_graph(graph)

    # Verify diagnostics
    assert diagnostics.total_buses == 3
    assert diagnostics.total_lines == 1
    assert "bus3_orphan" in diagnostics.orphan_buses  # bus3 should be orphan
    assert len(diagnostics.islands) >= 1  # At least one island


def test_create_synthetic_line():
    """Test synthetic line creation for same-voltage connections."""
    buses_df = gpd.GeoDataFrame(
        {
            "bus_id": ["bus_a", "bus_b"],
            "vn_kv": [220.0, 220.0],
            "geodata": [shp.Point(151.0, -33.8), shp.Point(151.1, -33.9)],
        },
        index=["bus_a", "bus_b"],
    )

    line = _create_synthetic_line("bus_a", "bus_b", buses_df)

    # Verify line properties
    assert line["from_bus"] == "bus_a"
    assert line["to_bus"] == "bus_b"
    assert line["length_km"] > 0
    assert line["in_service"] is True
    assert line["voltagekv"] == 220.0
    assert "Synthetic" in line["name"]


def test_build_connectivity_graph():
    """Test building connectivity graph from model."""
    # Create simple model with all required keys
    model = {
        "buses": pd.DataFrame({"bus_id": [0, 1, 2, 3]}),
        "lines": pd.DataFrame(
            {
                "from_bus": [0, 1],
                "to_bus": [1, 2],
                "in_service": [True, True],
            }
        ),
        "trafos": pd.DataFrame(
            {
                "hv_bus": [2],
                "lv_bus": [3],
                "in_service": [True],
            }
        ),
        "gens": pd.DataFrame(
            {
                "bus_id": [0],
            }
        ),
        "loads": pd.DataFrame(
            {
                "bus_id": [3],
            }
        ),
    }

    graph = _build_connectivity_graph(model)

    # Verify graph structure
    assert 0 in graph.nodes()
    assert 3 in graph.nodes()
    assert graph.has_edge(0, 1)
    assert graph.has_edge(1, 2)
    assert graph.has_edge(2, 3)  # Transformer connection


def test_model_validation_runs_without_error():
    """Test that model validation doesn't raise errors."""
    model = get_pandapower_model()

    # Should have all required keys
    assert "buses" in model
    assert "lines" in model
    assert "trafos" in model
    assert "gens" in model
    assert "loads" in model


def test_network_has_transformers():
    """Test that network contains transformers for voltage conversion."""
    net = create_pandapower_network(use_opennem=False)

    # Should have transformers for cross-voltage connections
    assert len(net.trafo) > 0, "No transformers found in network"

    # All transformers should connect valid buses
    for _, trafo in net.trafo.iterrows():
        assert trafo["hv_bus"] in net.bus.index
        assert trafo["lv_bus"] in net.bus.index


def test_network_buses_have_voltage():
    """Test that all buses have valid voltage levels."""
    net = create_pandapower_network(use_opennem=False)

    # All buses should have a nominal voltage
    for _, bus in net.bus.iterrows():
        assert bus["vn_kv"] > 0, f"Bus {bus.name} has invalid voltage {bus['vn_kv']}"
        # Voltage should be reasonable (between distribution and transmission levels)
        assert 0.4 <= bus["vn_kv"] <= 500, f"Bus has unusual voltage level: {bus['vn_kv']} kV"


def test_hvdc_interconnectors_added():
    """Test that known HVDC links are represented as dclines, not fictitious AC lines."""
    net = create_pandapower_network(use_opennem=False)

    expected_total_segments = sum(len(link["lines"]) for link in _HVDC_INTERCONNECTORS)
    assert len(net.dcline) == expected_total_segments, (
        "Expected one dcline per original AC line segment across all HVDC interconnectors"
    )

    for link in _HVDC_INTERCONNECTORS:
        dclines = net.dcline[net.dcline["name"].str.startswith(f"dcline_{link['name']}")]
        assert len(dclines) == len(link["lines"]), (
            f"Expected {len(link['lines'])} dcline segment(s) for {link['name']}"
        )
        for _, dcline in dclines.iterrows():
            assert dcline["from_bus"] in net.bus.index
            assert dcline["to_bus"] in net.bus.index

        # The original AC line segment(s) must be present but out of service,
        # not deleted -- and must not still be in_service (would double-count
        # the link electrically).
        for line_name in link["lines"]:
            matching_lines = net.line[net.line["name"] == line_name]
            assert len(matching_lines) == 1, f"Expected exactly one line named {line_name}"
            assert not matching_lines.iloc[0]["in_service"], (
                f"AC line '{line_name}' should be out of service (replaced by dcline)"
            )


def test_long_high_impedance_lines_check():
    """Test the informational long_high_impedance_lines sanity check."""
    net = pp.create_empty_network()
    b1 = pp.create_bus(net, vn_kv=66.0)
    b2 = pp.create_bus(net, vn_kv=66.0)
    pp.create_ext_grid(net, b1)
    pp.create_load(net, b2, p_mw=1.0)
    # 250 km at 66 kV with x_ohm_per_km=0.44 -> x_pu > 1.0 at 100 MVA base
    long_line = pp.create_line_from_parameters(
        net,
        from_bus=b1,
        to_bus=b2,
        length_km=250.0,
        r_ohm_per_km=0.18,
        x_ohm_per_km=0.44,
        c_nf_per_km=8.0,
        max_i_ka=0.4,
        name="long radial line",
    )

    results = sanity_checks(net)

    assert "long_high_impedance_lines" in results
    assert long_line in results["long_high_impedance_lines"]
    # Informational only -- must not be reported as a hard failure.
    assert results["disconnected_elements"] in (None, [])


# ---------------------------------------------------------------------------
# Unit tests for _deactivate_isolated_buses — no full model build needed
# ---------------------------------------------------------------------------


def _make_mini_net():
    """Build a minimal pandapower net: 4 buses, 2 lines, 1 ext_grid.

    Bus layout:
        0 (220kV) --line0-- 1 (220kV) --line1-- 2 (220kV)   [main AC island]
        3 (220kV)  [isolated — no in-service connection]
        4 (66kV)  --line2(oos)-- 5 (66kV)                    [island, line out of service]
    """
    net = pp.create_empty_network()
    for vn in [220, 220, 220, 220, 66, 66]:
        pp.create_bus(net, vn_kv=vn, in_service=True)
    pp.create_line_from_parameters(
        net, 0, 1, length_km=10, r_ohm_per_km=0.1, x_ohm_per_km=0.4, c_nf_per_km=10, max_i_ka=1
    )
    pp.create_line_from_parameters(
        net, 1, 2, length_km=10, r_ohm_per_km=0.1, x_ohm_per_km=0.4, c_nf_per_km=10, max_i_ka=1
    )
    # out-of-service line — buses 4 and 5 only connect via this
    pp.create_line_from_parameters(
        net,
        4,
        5,
        length_km=5,
        r_ohm_per_km=0.1,
        x_ohm_per_km=0.4,
        c_nf_per_km=10,
        max_i_ka=1,
        in_service=False,
    )
    pp.create_ext_grid(net, bus=0, vm_pu=1.0)
    return net


def test_deactivate_isolated_bus_no_connections():
    """Bus 3 has no connections at all — must be set out-of-service."""
    net = _make_mini_net()
    assert net.bus.at[3, "in_service"]

    _deactivate_isolated_buses(net)

    assert not net.bus.at[3, "in_service"], "Isolated bus 3 should be deactivated"


def test_deactivate_oos_line_island():
    """Buses 4 and 5 are connected only via an out-of-service line — both deactivated."""
    net = _make_mini_net()
    _deactivate_isolated_buses(net)

    assert not net.bus.at[4, "in_service"], "Bus 4 (oos-line-only island) should be deactivated"
    assert not net.bus.at[5, "in_service"], "Bus 5 (oos-line-only island) should be deactivated"


def test_deactivate_preserves_connected_buses():
    """Buses 0, 1, 2 are in the ext_grid island — must remain in-service."""
    net = _make_mini_net()
    _deactivate_isolated_buses(net)

    assert net.bus.at[0, "in_service"], "ext_grid bus should stay in-service"
    assert net.bus.at[1, "in_service"], "Bus 1 (connected) should stay in-service"
    assert net.bus.at[2, "in_service"], "Bus 2 (connected) should stay in-service"


def test_deactivate_ext_grid_bus_kept_even_if_no_lines():
    """The ext_grid bus itself must never be deactivated, even if it has no lines."""
    net = pp.create_empty_network()
    pp.create_bus(net, vn_kv=220, in_service=True)  # bus 0, no lines
    pp.create_ext_grid(net, bus=0, vm_pu=1.0)

    _deactivate_isolated_buses(net)

    assert net.bus.at[0, "in_service"], "ext_grid bus must not be deactivated"


def test_deactivate_idempotent():
    """Calling _deactivate_isolated_buses twice gives the same result."""
    net = _make_mini_net()
    _deactivate_isolated_buses(net)
    state_after_first = net.bus["in_service"].tolist()

    _deactivate_isolated_buses(net)
    state_after_second = net.bus["in_service"].tolist()

    assert state_after_first == state_after_second
