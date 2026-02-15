import networkx as nx
import pandapower as pp

from nemdb.models.pandapower import (
    create_pandapower_network,
    get_pandapower_model,
    get_pandapower_model_with_opennem,
)


def test_no_disconnected_elements_ga():
    """Verify network with GA generators has zero disconnected elements."""
    net = create_pandapower_network(use_opennem=False)
    disconnected = pp.disconnected_elements(net)

    # disconnected_elements returns None if no issues, or dict with element counts
    if disconnected is not None:
        # Filter out empty entries
        has_disconnected = any(isinstance(v, list) and len(v) > 0 for v in disconnected.values())
        assert not has_disconnected, f"Found disconnected elements: {disconnected}"


def test_no_disconnected_elements_opennem():
    """Verify network with OpenNEM generators has zero disconnected elements."""
    net = create_pandapower_network(use_opennem=True)
    disconnected = pp.disconnected_elements(net)

    # disconnected_elements returns None if no issues, or dict with element counts
    if disconnected is not None:
        # Filter out empty entries
        has_disconnected = any(isinstance(v, list) and len(v) > 0 for v in disconnected.values())
        assert not has_disconnected, f"Found disconnected elements: {disconnected}"


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


def test_all_generators_connected_opennem():
    """Verify all OpenNEM generators are on connected buses."""
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


def test_model_validation_runs_opennem():
    """Verify OpenNEM model validation function executes without error."""
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
