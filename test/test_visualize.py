"""Tests for network visualization module."""

import pandas as pd
import pytest

pytest.importorskip("plotly")
import plotly.graph_objects as go
import shapely as shp

from nemdb.models.visualize import (
    FUEL_COLORS,
    VOLTAGE_COLORS,
    _add_buses_to_figure,
    _add_generators_to_figure,
    _add_lines_to_figure,
    _add_loads_to_figure,
    _add_transformers_to_figure,
    _compute_island_assignment,
    visualize_islands,
    visualize_network,
)


@pytest.fixture
def sample_buses_df():
    """Create a sample buses DataFrame."""
    return pd.DataFrame(
        {
            "bus_id": ["bus_0_132kv", "bus_0_330kv", "bus_1_132kv"],
            "vn_kv": [132, 330, 132],
            "in_service": [True, True, False],
            "geodata": [
                shp.Point(151.0, -27.0),
                shp.Point(151.1, -27.1),
                shp.Point(150.9, -27.2),
            ],
        }
    )


@pytest.fixture
def sample_lines_df():
    """Create a sample lines DataFrame."""
    return pd.DataFrame(
        {
            "name": ["Line 1", "Line 2"],
            "from_bus": ["bus_0_132kv", "bus_0_330kv"],
            "to_bus": ["bus_1_132kv", "bus_1_330kv"],
            "length_km": [50.0, 75.5],
            "in_service": [True, True],
            "class": ["Transmission Line", "Transmission Line"],
            "geodata": [
                [(151.0, -27.0), (151.05, -27.1), (150.9, -27.2)],
                [(151.1, -27.1), (151.15, -27.15), (151.2, -27.2)],
            ],
            "voltagekv": [132, 330],
        }
    )


@pytest.fixture
def sample_trafos_df():
    """Create a sample transformers DataFrame."""
    return pd.DataFrame(
        {
            "name": ["Trafo 1", "Trafo 2"],
            "hv_bus": ["bus_0_330kv", "bus_1_330kv"],
            "lv_bus": ["bus_0_132kv", "bus_1_132kv"],
            "vn_hv_kv": [330, 330],
            "vn_lv_kv": [132, 132],
            "sn_mva": [1000.0, 500.0],
            "vk_percent": [12.2, 10.5],
            "vkr_percent": [0.25, 0.20],
            "pfe_kw": [60.0, 40.0],
            "i0_percent": [0.06, 0.05],
            "in_service": [True, False],
        }
    )


@pytest.fixture
def sample_gens_df():
    """Create a sample generators DataFrame."""
    return pd.DataFrame(
        {
            "bus_id": ["bus_0_132kv", "bus_1_132kv"],
            "name": ["Coal Plant 1", "Solar Farm 1"],
            "p_mw": [250.0, 50.0],
            "max_p_mw": [500.0, 100.0],
            "type": ["Thermal", "Photovoltaic"],
            "in_service": [True, True],
            "owner": ["Company A", "Company B"],
            "fueltype": ["Black Coal", "Solar"],
            "vn_kv": [132, 132],
            "geodata": [
                shp.Point(151.0, -27.0),
                shp.Point(151.1, -27.1),
            ],
        }
    )


@pytest.fixture
def sample_loads_df():
    """Create a sample loads DataFrame."""
    return pd.DataFrame(
        {
            "bus_id": ["bus_0_132kv", "bus_1_132kv"],
            "name": ["Substation A", "Substation B"],
            "type": ["Zone Substation", "Zone Substation"],
            "vn_kv": [132, 132],
            "in_service": [True, False],
            "locality": ["Sydney", "Melbourne"],
            "state": ["NSW", "VIC"],
            "geodata": [
                shp.Point(151.0, -27.0),
                shp.Point(151.1, -27.1),
            ],
        }
    )


@pytest.fixture
def sample_model(
    sample_buses_df, sample_lines_df, sample_trafos_df, sample_gens_df, sample_loads_df
):
    """Create a complete sample model dict."""
    return {
        "buses": sample_buses_df,
        "lines": sample_lines_df,
        "trafos": sample_trafos_df,
        "gens": sample_gens_df,
        "loads": sample_loads_df,
    }


@pytest.fixture
def two_island_lines_df():
    """Lines forming two disconnected islands: A-B-C (island 0) and D-E (island 1)."""
    return pd.DataFrame(
        {
            "name": ["Line AB", "Line BC", "Line DE"],
            "from_bus": ["isl1_bus_A_132kv", "isl1_bus_B_132kv", "isl2_bus_D_132kv"],
            "to_bus": ["isl1_bus_B_132kv", "isl1_bus_C_132kv", "isl2_bus_E_132kv"],
            "length_km": [50.0, 60.0, 40.0],
            "in_service": [True, True, True],
            "class": ["Transmission Line"] * 3,
            "geodata": [
                [(151.0, -27.0), (151.1, -27.1)],
                [(151.1, -27.1), (151.2, -27.2)],
                [(153.0, -28.0), (153.1, -28.1)],
            ],
            "voltagekv": [132, 132, 132],
        }
    )


@pytest.fixture
def two_island_buses_df():
    """Five buses across two disconnected islands."""
    return pd.DataFrame(
        {
            "bus_id": [
                "isl1_bus_A_132kv",
                "isl1_bus_B_132kv",
                "isl1_bus_C_132kv",
                "isl2_bus_D_132kv",
                "isl2_bus_E_132kv",
            ],
            "vn_kv": [132] * 5,
            "in_service": [True] * 5,
            "geodata": [
                shp.Point(151.0, -27.0),
                shp.Point(151.1, -27.1),
                shp.Point(151.2, -27.2),
                shp.Point(153.0, -28.0),
                shp.Point(153.1, -28.1),
            ],
        }
    )


@pytest.fixture
def two_island_gens_df():
    """Two generators, one per island."""
    return pd.DataFrame(
        {
            "bus_id": ["isl1_bus_A_132kv", "isl2_bus_D_132kv"],
            "name": ["Gen Island 1", "Gen Island 2"],
            "p_mw": [100.0, 50.0],
            "max_p_mw": [200.0, 100.0],
            "type": ["Thermal", "Wind"],
            "in_service": [True, True],
            "owner": ["Company A", "Company B"],
            "fueltype": ["Black Coal", "Wind"],
            "vn_kv": [132, 132],
            "geodata": [shp.Point(151.0, -27.0), shp.Point(153.0, -28.0)],
        }
    )


@pytest.fixture
def two_island_loads_df():
    """Two loads, one per island."""
    return pd.DataFrame(
        {
            "bus_id": ["isl1_bus_B_132kv", "isl2_bus_E_132kv"],
            "name": ["Load Island 1", "Load Island 2"],
            "type": ["Zone Substation"] * 2,
            "vn_kv": [132, 132],
            "in_service": [True, True],
            "locality": ["Sydney", "Brisbane"],
            "state": ["NSW", "QLD"],
            "geodata": [shp.Point(151.1, -27.1), shp.Point(153.1, -28.1)],
        }
    )


@pytest.fixture
def two_island_model(
    two_island_lines_df, two_island_buses_df, two_island_gens_df, two_island_loads_df
):
    return {
        "lines": two_island_lines_df,
        "buses": two_island_buses_df,
        "gens": two_island_gens_df,
        "loads": two_island_loads_df,
        "trafos": pd.DataFrame(),
    }


class TestVisualizeNetwork:
    """Test main visualization function."""

    def test_visualize_network_returns_figure(self, sample_model):
        """Test that visualize_network returns a Plotly Figure."""
        fig = visualize_network(sample_model)
        assert isinstance(fig, go.Figure)

    def test_visualize_network_basic_call(self, sample_model):
        """Test basic function call with default parameters."""
        fig = visualize_network(sample_model)
        assert fig.layout.title.text == "NEM Network Visualization"
        assert len(fig.data) > 0

    def test_visualize_network_with_custom_title(self, sample_model):
        """Test custom title parameter."""
        title = "Custom Network Map"
        fig = visualize_network(sample_model, title=title)
        assert fig.layout.title.text == title

    def test_visualize_network_with_custom_center(self, sample_model):
        """Test custom map centering."""
        fig = visualize_network(sample_model, center_lat=-33.0, center_lon=150.0, zoom=6)
        assert fig.layout.mapbox.center.lat == -33.0
        assert fig.layout.mapbox.center.lon == 150.0
        assert fig.layout.mapbox.zoom == 6

    def test_visualize_network_with_mapbox_style(self, sample_model):
        """Test different mapbox styles."""
        for style in ["carto-positron", "carto-darkmatter", "open-street-map"]:
            fig = visualize_network(sample_model, mapbox_style=style)
            assert fig.layout.mapbox.style == style

    def test_visualize_network_hide_buses(self, sample_model):
        """Test hiding buses layer."""
        fig = visualize_network(sample_model, show_buses=False)
        # Count traces that are buses (by legend group)
        bus_traces = [t for t in fig.data if hasattr(t, "legendgroup") and t.legendgroup == "buses"]
        assert len(bus_traces) == 0

    def test_visualize_network_hide_lines(self, sample_model):
        """Test hiding lines layer."""
        fig = visualize_network(sample_model, show_lines=False)
        line_traces = [
            t for t in fig.data if hasattr(t, "legendgroup") and t.legendgroup == "lines"
        ]
        assert len(line_traces) == 0

    def test_visualize_network_hide_generators(self, sample_model):
        """Test hiding generators layer."""
        fig = visualize_network(sample_model, show_generators=False)
        gen_traces = [
            t for t in fig.data if hasattr(t, "legendgroup") and t.legendgroup == "generators"
        ]
        assert len(gen_traces) == 0

    def test_visualize_network_hide_loads(self, sample_model):
        """Test hiding loads layer."""
        fig = visualize_network(sample_model, show_loads=False)
        load_traces = [
            t for t in fig.data if hasattr(t, "legendgroup") and t.legendgroup == "loads"
        ]
        assert len(load_traces) == 0

    def test_visualize_network_hide_transformers(self, sample_model):
        """Test hiding transformers layer."""
        fig = visualize_network(sample_model, show_transformers=False)
        trafo_traces = [
            t for t in fig.data if hasattr(t, "legendgroup") and t.legendgroup == "transformers"
        ]
        assert len(trafo_traces) == 0

    def test_visualize_network_empty_model(self):
        """Test with empty model dict."""
        fig = visualize_network({})
        assert isinstance(fig, go.Figure)
        # Should still have valid layout
        assert fig.layout.mapbox is not None

    def test_visualize_network_missing_geodata(self, sample_buses_df, sample_lines_df):
        """Test handling of missing geodata."""
        buses = sample_buses_df.copy()
        buses.loc[0, "geodata"] = None
        model = {"buses": buses, "lines": sample_lines_df}
        fig = visualize_network(model)
        # Should still work and skip missing data
        assert isinstance(fig, go.Figure)

    def test_visualize_network_legend_exists(self, sample_model):
        """Test that legend is configured."""
        fig = visualize_network(sample_model)
        assert fig.layout.showlegend is True
        assert fig.layout.legend is not None

    def test_visualize_network_hover_enabled(self, sample_model):
        """Test that hover mode is enabled."""
        fig = visualize_network(sample_model)
        assert fig.layout.hovermode == "closest"


class TestAddLinesToFigure:
    """Test transmission line visualization."""

    def test_add_lines_basic(self, sample_lines_df):
        """Test adding lines to figure."""
        fig = go.Figure()
        _add_lines_to_figure(fig, sample_lines_df)
        assert len(fig.data) == 2  # One trace per voltage level

    def test_add_lines_empty(self):
        """Test with empty lines DataFrame."""
        fig = go.Figure()
        _add_lines_to_figure(fig, pd.DataFrame())
        assert len(fig.data) == 0

    def test_add_lines_groups_by_voltage(self, sample_lines_df):
        """Test that lines are grouped by voltage level."""
        fig = go.Figure()
        _add_lines_to_figure(fig, sample_lines_df)
        # Should have separate traces for 132kV and 330kV
        assert len(fig.data) == 2
        names = [trace.name for trace in fig.data]
        assert any("132 kV" in name for name in names)
        assert any("330 kV" in name for name in names)

    def test_add_lines_hover_text(self, sample_lines_df):
        """Test that hover text contains line information."""
        fig = go.Figure()
        _add_lines_to_figure(fig, sample_lines_df)
        # Check hover text exists
        assert all(trace.hovertext for trace in fig.data)

    def test_add_lines_color_coding(self, sample_lines_df):
        """Test that lines are color-coded by voltage."""
        fig = go.Figure()
        _add_lines_to_figure(fig, sample_lines_df)
        # Check colors are set from VOLTAGE_COLORS
        assert all(trace.line.color in VOLTAGE_COLORS.values() for trace in fig.data)

    def test_add_lines_with_nan_geodata(self):
        """Test handling of NaN geodata."""
        df = pd.DataFrame(
            {
                "name": ["Line 1"],
                "voltagekv": [132],
                "from_bus": ["bus_0"],
                "to_bus": ["bus_1"],
                "length_km": [50.0],
                "in_service": [True],
                "class": ["Transmission Line"],
                "geodata": [None],
            }
        )
        fig = go.Figure()
        _add_lines_to_figure(fig, df)
        # Should handle gracefully
        assert isinstance(fig, go.Figure)


class TestAddBusesToFigure:
    """Test bus visualization."""

    def test_add_buses_basic(self, sample_buses_df):
        """Test adding buses to figure."""
        fig = go.Figure()
        _add_buses_to_figure(fig, sample_buses_df)
        # Should have traces for 132kV and 330kV
        assert len(fig.data) >= 1

    def test_add_buses_empty(self):
        """Test with empty buses DataFrame."""
        fig = go.Figure()
        _add_buses_to_figure(fig, pd.DataFrame())
        assert len(fig.data) == 0

    def test_add_buses_groups_by_voltage(self, sample_buses_df):
        """Test that buses are grouped by voltage."""
        fig = go.Figure()
        _add_buses_to_figure(fig, sample_buses_df)
        # Should have separate traces for each voltage
        names = [trace.name for trace in fig.data]
        assert len(names) >= 1
        assert all("kV" in name for name in names)

    def test_add_buses_hover_text(self, sample_buses_df):
        """Test that hover text contains bus information."""
        fig = go.Figure()
        _add_buses_to_figure(fig, sample_buses_df)
        assert all(trace.hovertext for trace in fig.data)
        # Check that bus IDs are in hover text
        all_hover = " ".join(
            str(ht)
            for trace in fig.data
            for ht in (
                trace.hovertext if isinstance(trace.hovertext, (list, tuple)) else [trace.hovertext]
            )
        )
        assert "bus_0" in all_hover

    def test_add_buses_color_coding(self, sample_buses_df):
        """Test that buses are color-coded by voltage."""
        fig = go.Figure()
        _add_buses_to_figure(fig, sample_buses_df)
        assert all(trace.marker.color in VOLTAGE_COLORS.values() for trace in fig.data)

    def test_add_buses_marker_size_by_voltage(self, sample_buses_df):
        """Test that marker size varies by voltage."""
        fig = go.Figure()
        _add_buses_to_figure(fig, sample_buses_df)
        # All traces should have size attribute
        sizes = [trace.marker.size for trace in fig.data]
        # Sizes should vary
        assert len(set(sizes)) > 1 or len(sizes) == 1


class TestAddTransformersTofigure:
    """Test transformer visualization."""

    def test_add_transformers_basic(self, sample_trafos_df, sample_buses_df):
        """Test adding transformers to figure."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, sample_trafos_df, sample_buses_df)
        assert len(fig.data) == 1

    def test_add_transformers_empty_trafos(self, sample_buses_df):
        """Test with empty trafos DataFrame."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, pd.DataFrame(), sample_buses_df)
        assert len(fig.data) == 0

    def test_add_transformers_empty_buses(self, sample_trafos_df):
        """Test with empty buses DataFrame."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, sample_trafos_df, pd.DataFrame())
        assert len(fig.data) == 0

    def test_add_transformers_hover_text(self, sample_trafos_df, sample_buses_df):
        """Test that hover text contains transformer info."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, sample_trafos_df, sample_buses_df)
        assert len(fig.data) > 0
        assert all(trace.hovertext for trace in fig.data)

    def test_add_transformers_positioned_at_midpoint(self, sample_trafos_df, sample_buses_df):
        """Test that transformers are positioned at midpoint between buses."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, sample_trafos_df, sample_buses_df)
        # Check that positions exist and are reasonable
        for trace in fig.data:
            assert len(trace.lon) > 0
            assert len(trace.lat) > 0
            # Check lon/lat are within expected bounds (Australia)
            assert all(139 <= lon <= 154 for lon in trace.lon if lon is not None)
            assert all(-44 <= lat <= -10 for lat in trace.lat if lat is not None)

    def test_add_transformers_marker_style(self, sample_trafos_df, sample_buses_df):
        """Test that transformers use distinctive marker style."""
        fig = go.Figure()
        _add_transformers_to_figure(fig, sample_trafos_df, sample_buses_df)
        assert len(fig.data) > 0
        # Should use star symbol and purple color
        assert fig.data[0].marker.symbol == "star"
        assert fig.data[0].marker.color == "#9370DB"


class TestAddLoadsTofigure:
    """Test load visualization."""

    def test_add_loads_basic(self, sample_loads_df):
        """Test adding loads to figure."""
        fig = go.Figure()
        _add_loads_to_figure(fig, sample_loads_df)
        assert len(fig.data) == 1

    def test_add_loads_empty(self):
        """Test with empty loads DataFrame."""
        fig = go.Figure()
        _add_loads_to_figure(fig, pd.DataFrame())
        assert len(fig.data) == 0

    def test_add_loads_hover_text(self, sample_loads_df):
        """Test that hover text contains load information."""
        fig = go.Figure()
        _add_loads_to_figure(fig, sample_loads_df)
        assert len(fig.data) > 0
        assert all(trace.hovertext for trace in fig.data)

    def test_add_loads_marker_style(self, sample_loads_df):
        """Test that loads use distinctive marker style."""
        fig = go.Figure()
        _add_loads_to_figure(fig, sample_loads_df)
        assert len(fig.data) > 0
        # Should use triangle-down symbol
        assert fig.data[0].marker.symbol == "triangle-down"


class TestAddGeneratorsTofigure:
    """Test generator visualization."""

    def test_add_generators_basic(self, sample_gens_df):
        """Test adding generators to figure."""
        fig = go.Figure()
        _add_generators_to_figure(fig, sample_gens_df)
        # Should have traces grouped by fuel type
        assert len(fig.data) >= 1

    def test_add_generators_empty(self):
        """Test with empty generators DataFrame."""
        fig = go.Figure()
        _add_generators_to_figure(fig, pd.DataFrame())
        assert len(fig.data) == 0

    def test_add_generators_groups_by_fuel(self, sample_gens_df):
        """Test that generators are grouped by fuel type."""
        fig = go.Figure()
        _add_generators_to_figure(fig, sample_gens_df)
        # Should have separate traces for Coal and Solar
        assert len(fig.data) == 2

    def test_add_generators_hover_text(self, sample_gens_df):
        """Test that hover text contains generator information."""
        fig = go.Figure()
        _add_generators_to_figure(fig, sample_gens_df)
        assert all(trace.hovertext for trace in fig.data)

    def test_add_generators_color_coding(self, sample_gens_df):
        """Test that generators are color-coded by fuel type."""
        fig = go.Figure()
        _add_generators_to_figure(fig, sample_gens_df)
        colors = [trace.marker.color for trace in fig.data]
        # Should have colors from FUEL_COLORS
        assert all(color in FUEL_COLORS.values() for color in colors)

    def test_add_generators_size_by_capacity(self, sample_gens_df):
        """Test that generator size varies by capacity."""
        fig = go.Figure()
        _add_generators_to_figure(fig, sample_gens_df)
        # Larger generators should have larger sizes
        for trace in fig.data:
            sizes = trace.marker.size
            # Sizes should be between 5 and 20
            assert all(5 <= s <= 20 for s in sizes)


class TestVoltageColorScale:
    """Test voltage color scale."""

    def test_voltage_colors_complete(self):
        """Test that all major Australian voltages have colors."""
        expected_voltages = [500, 330, 275, 220, 132, 110, 66]
        for voltage in expected_voltages:
            assert voltage in VOLTAGE_COLORS, f"Missing color for {voltage} kV"

    def test_voltage_colors_are_valid_hex(self):
        """Test that all voltage colors are valid hex codes."""
        for _voltage, color in VOLTAGE_COLORS.items():
            assert isinstance(color, str)
            assert color.startswith("#")
            assert len(color) == 7


class TestFuelColorScale:
    """Test fuel type color scale."""

    def test_fuel_colors_common_types(self):
        """Test that common fuel types have colors."""
        common_types = ["Coal", "Gas", "Hydro", "Wind", "Solar", "Battery"]
        for fuel_type in common_types:
            assert fuel_type in FUEL_COLORS, f"Missing color for {fuel_type}"

    def test_fuel_colors_are_valid_hex(self):
        """Test that all fuel colors are valid hex codes."""
        for _fuel_type, color in FUEL_COLORS.items():
            assert isinstance(color, str)
            assert color.startswith("#")
            assert len(color) == 7


class TestComputeIslandAssignment:
    """Tests for island assignment computation."""

    def test_two_disconnected_components(self):
        """Two disconnected line groups produce two distinct island indices."""
        lines = pd.DataFrame(
            {
                "from_bus": ["bus_A_132kv", "bus_C_132kv"],
                "to_bus": ["bus_B_132kv", "bus_D_132kv"],
            }
        )
        result = _compute_island_assignment(lines)
        assert set(result.keys()) == {"bus_A_132kv", "bus_B_132kv", "bus_C_132kv", "bus_D_132kv"}
        assert result["bus_A_132kv"] == result["bus_B_132kv"]
        assert result["bus_C_132kv"] == result["bus_D_132kv"]
        assert result["bus_A_132kv"] != result["bus_C_132kv"]

    def test_single_connected_component(self):
        """All buses in a chain get the same island index."""
        lines = pd.DataFrame(
            {
                "from_bus": ["bus_A_132kv", "bus_B_132kv"],
                "to_bus": ["bus_B_132kv", "bus_C_132kv"],
            }
        )
        result = _compute_island_assignment(lines)
        assert result["bus_A_132kv"] == result["bus_B_132kv"] == result["bus_C_132kv"]

    def test_largest_component_gets_index_zero(self):
        """The largest connected component is assigned island index 0."""
        lines = pd.DataFrame(
            {
                "from_bus": ["bus_A", "bus_B", "bus_X"],
                "to_bus": ["bus_B", "bus_C", "bus_Y"],
            }
        )
        result = _compute_island_assignment(lines)
        # {A, B, C} has 3 nodes → island 0; {X, Y} has 2 nodes → island 1
        assert result["bus_A"] == 0
        assert result["bus_B"] == 0
        assert result["bus_C"] == 0
        assert result["bus_X"] == 1
        assert result["bus_Y"] == 1

    def test_empty_lines_returns_empty(self):
        """Empty lines DataFrame returns empty dict."""
        result = _compute_island_assignment(pd.DataFrame(columns=["from_bus", "to_bus"]))
        assert result == {}


class TestVisualizeIslands:
    """Tests for island visualization function."""

    def test_returns_figure(self, two_island_model):
        fig = visualize_islands(two_island_model)
        assert isinstance(fig, go.Figure)

    def test_one_legend_entry_per_island(self, two_island_model):
        """Exactly one trace per island has showlegend=True."""
        fig = visualize_islands(two_island_model)
        legend_entries = [t for t in fig.data if t.showlegend]
        assert len(legend_entries) == 2

    def test_island_legend_names(self, two_island_model):
        """Legend entry names follow 'Island N' pattern."""
        fig = visualize_islands(two_island_model)
        legend_names = {t.name for t in fig.data if t.showlegend}
        assert legend_names == {"Island 1", "Island 2"}

    def test_all_traces_in_island_share_legendgroup(self, two_island_model):
        """Every trace for an island uses that island's legendgroup."""
        fig = visualize_islands(two_island_model)
        for island_name in ("Island 1", "Island 2"):
            traces = [t for t in fig.data if t.legendgroup == island_name]
            assert len(traces) > 0
            assert all(t.legendgroup == island_name for t in traces)

    def test_only_first_trace_per_island_shows_legend(self, two_island_model):
        """Only one trace per island has showlegend=True; the rest are False."""
        fig = visualize_islands(two_island_model)
        for island_name in ("Island 1", "Island 2"):
            traces = [t for t in fig.data if t.legendgroup == island_name]
            shown = [t for t in traces if t.showlegend]
            assert len(shown) == 1

    def test_islands_have_distinct_colors(self, two_island_model):
        """Island 1 and Island 2 use different colors."""
        fig = visualize_islands(two_island_model)
        island1_first = next(t for t in fig.data if t.legendgroup == "Island 1")
        island2_first = next(t for t in fig.data if t.legendgroup == "Island 2")

        def get_color(trace):
            if trace.mode == "lines":
                return trace.line.color
            return trace.marker.color

        assert get_color(island1_first) != get_color(island2_first)

    def test_custom_title(self, two_island_model):
        fig = visualize_islands(two_island_model, title="My Islands")
        assert fig.layout.title.text == "My Islands"

    def test_empty_model_returns_figure(self):
        fig = visualize_islands({})
        assert isinstance(fig, go.Figure)

    def test_map_layout_configured(self, two_island_model):
        fig = visualize_islands(two_island_model, center_lat=-33.0, center_lon=150.0, zoom=6)
        assert fig.layout.mapbox.center.lat == -33.0
        assert fig.layout.mapbox.center.lon == 150.0
        assert fig.layout.mapbox.zoom == 6
