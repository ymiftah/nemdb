# Island Visualization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `visualize_islands(model)` to `src/nemdb/models/visualize.py` that colors all network elements by connected island (networkx connected component), with one toggleable legend entry per island.

**Architecture:** Build a networkx graph from `lines["from_bus"]`/`["to_bus"]`, find connected components sorted by size, assign each a color from `plotly.colors.qualitative.Dark24`. For each island, filter all five element types and add traces that share a `legendgroup`; only the first non-empty trace per island gets `showlegend=True`.

**Tech Stack:** Python, pandas, plotly (Scattermap), networkx, shapely

---

## Files

- **Modify:** `src/nemdb/models/visualize.py` — add imports, `_ISLAND_COLORS` constant, `_compute_island_assignment`, `_apply_map_layout` (extracted helper), `_add_island_lines/buses/trafos/loads/gens`, `visualize_islands`; also update `visualize_network` to call `_apply_map_layout`
- **Modify:** `test/test_visualize.py` — add imports, fixtures, and two new test classes

---

## Task 1: Tests for `_compute_island_assignment`

**Files:**

- Modify: `test/test_visualize.py`

- [ ] **Step 1: Add import for `_compute_island_assignment` to the test file**

In `test/test_visualize.py`, update the import block (currently lines 10–19) to add `_compute_island_assignment`:

```python
from nemdb.models.visualize import (
    FUEL_COLORS,
    VOLTAGE_COLORS,
    _add_buses_to_figure,
    _add_generators_to_figure,
    _add_lines_to_figure,
    _add_loads_to_figure,
    _add_transformers_to_figure,
    _compute_island_assignment,
    visualize_network,
)
```

- [ ] **Step 2: Write failing tests for `_compute_island_assignment`**

Append this class at the end of `test/test_visualize.py`:

```python
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
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
uv run pytest test/test_visualize.py::TestComputeIslandAssignment -v
```

Expected: `ImportError` or `AttributeError` — `_compute_island_assignment` does not exist yet.

---

## Task 2: Implement `_compute_island_assignment`

**Files:**

- Modify: `src/nemdb/models/visualize.py`

- [ ] **Step 1: Add `networkx` and `plotly.colors` imports**

In `src/nemdb/models/visualize.py`, after `import math` (line 10), add:

```python
import networkx as nx
import plotly.colors
```

- [ ] **Step 2: Add `_ISLAND_COLORS` constant after `DEFAULT_FUEL_COLOR` (line 65)**

```python
# Island color palette (cycles for >24 islands)
_ISLAND_COLORS: list[str] = plotly.colors.qualitative.Dark24
```

- [ ] **Step 3: Add `_compute_island_assignment` function after `_ISLAND_COLORS`**

```python
def _compute_island_assignment(lines_df: pd.DataFrame) -> dict[str, int]:
    """Assign each bus to a connected island index.

    Builds an undirected graph from line from_bus/to_bus edges, finds connected
    components sorted by size (largest first), and returns a mapping from bus_id
    to island index (0 = largest island).

    Args:
        lines_df: DataFrame with 'from_bus' and 'to_bus' columns.

    Returns:
        Dict mapping bus_id to island index.
    """
    if lines_df.empty:
        return {}

    G: nx.Graph = nx.Graph()
    for _, row in lines_df.iterrows():
        G.add_edge(row["from_bus"], row["to_bus"])

    components = sorted(nx.connected_components(G), key=len, reverse=True)
    return {bus: idx for idx, component in enumerate(components) for bus in component}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest test/test_visualize.py::TestComputeIslandAssignment -v
```

Expected: 4 tests PASSED.

---

## Task 3: Tests for `visualize_islands`

**Files:**

- Modify: `test/test_visualize.py`

- [ ] **Step 1: Add `visualize_islands` to the import block**

Replace the import block (updated in Task 1):

```python
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
```

- [ ] **Step 2: Add fixtures for a two-island model**

Append these fixtures after `sample_model` (before the first test class):

```python
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
def two_island_model(two_island_lines_df, two_island_buses_df, two_island_gens_df, two_island_loads_df):
    return {
        "lines": two_island_lines_df,
        "buses": two_island_buses_df,
        "gens": two_island_gens_df,
        "loads": two_island_loads_df,
        "trafos": pd.DataFrame(),
    }
```

- [ ] **Step 3: Write failing tests for `visualize_islands`**

Append this class at the end of `test/test_visualize.py`:

```python
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
        color1 = island1_first.line.color if island1_first.mode == "lines" else island1_first.marker.color
        color2 = island2_first.line.color if island2_first.mode == "lines" else island2_first.marker.color
        assert color1 != color2

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
```

- [ ] **Step 4: Run tests to confirm they fail**

```bash
uv run pytest test/test_visualize.py::TestVisualizeIslands -v
```

Expected: `ImportError` — `visualize_islands` does not exist yet.

---

## Task 4: Implement `visualize_islands` and helpers

**Files:**

- Modify: `src/nemdb/models/visualize.py`

- [ ] **Step 1: Extract `_apply_map_layout` and update `visualize_network`**

Add this function immediately before `visualize_network` (before line 68):

```python
def _apply_map_layout(
    fig: go.Figure,
    mapbox_style: str,
    center_lat: float,
    center_lon: float,
    zoom: int,
    height: int,
    title: str,
) -> None:
    """Apply standard map layout settings to a figure."""
    fig.update_layout(
        title=title,
        mapbox={
            "style": mapbox_style,
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": zoom,
        },
        height=height,
        margin={"l": 0, "r": 0, "t": 40, "b": 0},
        hovermode="closest",
        showlegend=True,
        legend={
            "x": 0.01,
            "y": 0.99,
            "bgcolor": "rgba(255, 255, 255, 0.8)",
            "bordercolor": "rgba(0, 0, 0, 0.2)",
            "borderwidth": 1,
        },
    )
```

Replace the `fig.update_layout(...)` block in `visualize_network` (lines 148–166) with:

```python
    _apply_map_layout(fig, mapbox_style, center_lat, center_lon, zoom, height, title)
```

- [ ] **Step 2: Add `_add_island_lines`**

Append after `_add_generators_to_figure`:

```python
def _add_island_lines(
    fig: go.Figure,
    lines_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all lines for one island as a single trace. Returns True if a trace was added."""
    lats_list: list[float | None] = []
    lons_list: list[float | None] = []
    hover_text: list[str | None] = []

    for _, row in lines_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        try:
            coords = list(geodata)
        except (TypeError, ValueError):
            continue
        if not coords:
            continue

        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        lats_list.extend([*lats, None])
        lons_list.extend([*lons, None])

        name = row.get("name", "Unknown")
        voltage = row.get("voltagekv", 0)
        length = row.get("length_km", 0)
        from_bus = row.get("from_bus", "")
        to_bus = row.get("to_bus", "")
        cls = row.get("class", "")
        in_svc = row.get("in_service", True)
        text = (
            f"<b>{name}</b><br>"
            f"From Bus: {from_bus}<br>"
            f"To Bus: {to_bus}<br>"
            f"Length: {length:.2f} km<br>"
            f"Voltage: {voltage} kV<br>"
            f"Class: {cls}<br>"
            f"In Service: {in_svc}"
        )
        hover_text.extend([text] * len(lats) + [None])

    if not lats_list:
        return False

    fig.add_trace(
        go.Scattermap(
            lon=lons_list,
            lat=lats_list,
            mode="lines",
            name=island_name,
            line={"width": 1.5, "color": color},
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=show_legend,
            legendgroup=island_name,
        )
    )
    return True
```

- [ ] **Step 3: Add `_add_island_buses`**

```python
def _add_island_buses(
    fig: go.Figure,
    buses_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all buses for one island as a single trace. Returns True if a trace was added."""
    lats: list[float] = []
    lons: list[float] = []
    hover_text: list[str] = []

    for _, row in buses_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        try:
            lon = geodata.x
            lat = geodata.y
        except AttributeError:
            continue
        lats.append(lat)
        lons.append(lon)
        bus_id = row.get("bus_id", "Unknown")
        vn_kv = row.get("vn_kv", 0)
        in_svc = row.get("in_service", True)
        hover_text.append(f"<b>{bus_id}</b><br>Voltage: {vn_kv} kV<br>In Service: {in_svc}")

    if not lats:
        return False

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker={"size": 5, "color": color, "opacity": 0.7},
            name=island_name,
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=show_legend,
            legendgroup=island_name,
        )
    )
    return True
```

- [ ] **Step 4: Add `_add_island_trafos`**

```python
def _add_island_trafos(
    fig: go.Figure,
    trafos_df: pd.DataFrame,
    buses_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all transformers for one island as a single trace. Returns True if a trace was added."""
    bus_locations: dict[str, tuple[float, float]] = {}
    for _, row in buses_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        try:
            bus_locations[row.get("bus_id")] = (geodata.x, geodata.y)
        except AttributeError:
            continue

    lats: list[float] = []
    lons: list[float] = []
    hover_text: list[str] = []

    for _, row in trafos_df.iterrows():
        hv_bus = row.get("hv_bus")
        lv_bus = row.get("lv_bus")
        if hv_bus not in bus_locations or lv_bus not in bus_locations:
            continue
        hv_lon, hv_lat = bus_locations[hv_bus]
        lv_lon, lv_lat = bus_locations[lv_bus]
        lons.append((hv_lon + lv_lon) / 2)
        lats.append((hv_lat + lv_lat) / 2)
        name = row.get("name", "Unknown Trafo")
        sn_mva = row.get("sn_mva", 0)
        vk_pct = row.get("vk_percent", 0)
        vkr_pct = row.get("vkr_percent", 0)
        in_svc = row.get("in_service", True)
        hover_text.append(
            f"<b>{name}</b><br>"
            f"HV Bus: {hv_bus}<br>"
            f"LV Bus: {lv_bus}<br>"
            f"Power Rating (MVA): {sn_mva:.0f}<br>"
            f"Impedance (Vk %): {vk_pct:.2f}%<br>"
            f"Resistance (Vkr %): {vkr_pct:.2f}%<br>"
            f"In Service: {in_svc}"
        )

    if not lats:
        return False

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker={"size": 8, "color": color, "symbol": "star", "opacity": 0.8},
            name=island_name,
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=show_legend,
            legendgroup=island_name,
        )
    )
    return True
```

- [ ] **Step 5: Add `_add_island_loads`**

```python
def _add_island_loads(
    fig: go.Figure,
    loads_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all loads for one island as a single trace. Returns True if a trace was added."""
    lats: list[float] = []
    lons: list[float] = []
    hover_text: list[str] = []

    for _, row in loads_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        try:
            lon = geodata.x
            lat = geodata.y
        except AttributeError:
            continue
        lats.append(lat)
        lons.append(lon)
        name = row.get("name", "Unknown")
        ltype = row.get("type", "")
        voltage = row.get("vn_kv", 0)
        locality = row.get("locality", "")
        state = row.get("state", "")
        in_svc = row.get("in_service", True)
        hover_text.append(
            f"<b>{name}</b><br>"
            f"Type: {ltype}<br>"
            f"Voltage: {voltage} kV<br>"
            f"Locality: {locality}<br>"
            f"State: {state}<br>"
            f"In Service: {in_svc}"
        )

    if not lats:
        return False

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker={"size": 4, "color": color, "symbol": "triangle-down", "opacity": 0.6},
            name=island_name,
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=show_legend,
            legendgroup=island_name,
        )
    )
    return True
```

- [ ] **Step 6: Add `_add_island_gens`**

```python
def _add_island_gens(
    fig: go.Figure,
    gens_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all generators for one island as a single trace. Returns True if a trace was added."""
    lats: list[float] = []
    lons: list[float] = []
    sizes: list[float] = []
    hover_text: list[str] = []

    fuel_type_col = "type" if "type" in gens_df.columns else "fueltype"

    for _, row in gens_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        try:
            lon = geodata.x
            lat = geodata.y
        except AttributeError:
            continue
        lats.append(lat)
        lons.append(lon)
        cap = row.get("max_p_mw", 0)
        sizes.append(max(5, min(20, 5 + math.log10(max(1, cap)) * 3)))
        name = row.get("name", "Unknown")
        fuel_type = row.get(fuel_type_col, "")
        gtype = row.get("type", "")
        p_mw = row.get("p_mw", 0)
        owner = row.get("owner") or row.get("code") or "Unknown"
        in_svc = row.get("in_service", True)
        hover_text.append(
            f"<b>{name}</b><br>"
            f"Fuel Type: {fuel_type}<br>"
            f"Type: {gtype}<br>"
            f"Capacity: {cap:.0f} MW<br>"
            f"Output: {p_mw:.0f} MW<br>"
            f"Owner: {owner}<br>"
            f"In Service: {in_svc}"
        )

    if not lats:
        return False

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker={"size": sizes, "color": color, "opacity": 0.8},
            name=island_name,
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=show_legend,
            legendgroup=island_name,
        )
    )
    return True
```

- [ ] **Step 7: Add `visualize_islands`**

```python
def visualize_islands(
    model: dict,
    *,
    mapbox_style: str = "carto-positron",
    height: int = 800,
    center_lat: float = -27.0,
    center_lon: float = 133.0,
    zoom: int = 4,
    title: str = "NEM Network Islands",
) -> go.Figure:
    """Create an interactive Plotly map visualization colored by network island.

    Each connected component (island) in the transmission network is shown in a
    distinct color. All element types (buses, lines, generators, loads,
    transformers) share the same color per island. Clicking an island legend
    entry toggles all of its elements.

    Args:
        model: Dict returned by get_pandapower_model() with keys:
            - 'buses', 'lines', 'gens', 'loads', 'trafos'
        mapbox_style: Carto style ('carto-positron', 'carto-darkmatter', 'open-street-map')
        height: Map height in pixels
        center_lat: Center latitude (default: Australia center)
        center_lon: Center longitude (default: Australia center)
        zoom: Initial zoom level (4 = Australia-wide view)
        title: Figure title

    Returns:
        plotly.graph_objects.Figure: Interactive map with one legend entry per island.

    Example:
        >>> from nemdb.models.pandapower import get_pandapower_model
        >>> from nemdb.models.visualize import visualize_islands
        >>> model = get_pandapower_model()
        >>> fig = visualize_islands(model)
        >>> fig.show()
    """
    fig = go.Figure()

    lines_df = model.get("lines", pd.DataFrame())
    buses_df = model.get("buses", pd.DataFrame())
    gens_df = model.get("gens", pd.DataFrame())
    loads_df = model.get("loads", pd.DataFrame())
    trafos_df = model.get("trafos", pd.DataFrame())

    if not lines_df.empty:
        bus_to_island = _compute_island_assignment(lines_df)

        island_buses: dict[int, set[str]] = {}
        for bus, island_idx in bus_to_island.items():
            island_buses.setdefault(island_idx, set()).add(bus)

        for island_idx in sorted(island_buses.keys()):
            buses_in_island = island_buses[island_idx]
            island_name = f"Island {island_idx + 1}"
            color = _ISLAND_COLORS[island_idx % len(_ISLAND_COLORS)]
            legend_added = False

            island_lines_df = lines_df[lines_df["from_bus"].isin(buses_in_island)]
            island_buses_df = buses_df[buses_df["bus_id"].isin(buses_in_island)] if not buses_df.empty else pd.DataFrame()
            island_trafos_df = trafos_df[trafos_df["hv_bus"].isin(buses_in_island)] if not trafos_df.empty else pd.DataFrame()
            island_loads_df = loads_df[loads_df["bus_id"].isin(buses_in_island)] if not loads_df.empty else pd.DataFrame()
            island_gens_df = gens_df[gens_df["bus_id"].isin(buses_in_island)] if not gens_df.empty else pd.DataFrame()

            if not island_lines_df.empty:
                added = _add_island_lines(fig, island_lines_df, island_name, color, not legend_added)
                legend_added = legend_added or added
            if not island_buses_df.empty:
                added = _add_island_buses(fig, island_buses_df, island_name, color, not legend_added)
                legend_added = legend_added or added
            if not island_trafos_df.empty:
                added = _add_island_trafos(fig, island_trafos_df, buses_df, island_name, color, not legend_added)
                legend_added = legend_added or added
            if not island_loads_df.empty:
                added = _add_island_loads(fig, island_loads_df, island_name, color, not legend_added)
                legend_added = legend_added or added
            if not island_gens_df.empty:
                added = _add_island_gens(fig, island_gens_df, island_name, color, not legend_added)
                legend_added = legend_added or added

    _apply_map_layout(fig, mapbox_style, center_lat, center_lon, zoom, height, title)
    return fig
```

- [ ] **Step 8: Run all visualize tests**

```bash
uv run pytest test/test_visualize.py -v
```

Expected: All tests PASSED (including existing `TestVisualizeNetwork`, `TestAddLinesToFigure`, etc.).

---

## Task 5: Lint, type-check, and commit

- [ ] **Step 1: Run ruff and mypy**

```bash
uv run ruff check src/nemdb/models/visualize.py && uv run ruff format src/nemdb/models/visualize.py
uv run mypy src/nemdb/models/visualize.py
```

Fix any issues before committing.

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest test/test_visualize.py -v
```

Expected: All tests PASSED.

- [ ] **Step 3: Commit**

```bash
git add src/nemdb/models/visualize.py test/test_visualize.py
git commit -m "feat: add visualize_islands function with per-island color coding"
```
