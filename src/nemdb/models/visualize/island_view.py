"""Island-coloured network visualization for the NEM transmission network."""

import math

import networkx as nx
import pandas as pd
import plotly.colors
import plotly.graph_objects as go
import shapely as shp

from .common import _apply_map_layout, bus_location_lookup

# Island color palette (cycles for >24 islands)
_ISLAND_COLORS: list[str] = plotly.colors.qualitative.Dark24


def _compute_island_assignment(
    lines_df: pd.DataFrame, trafos_df: pd.DataFrame | None = None
) -> dict[str, int]:
    """Assign each bus to a connected island index.

    Builds an undirected graph from line from_bus/to_bus edges and (optionally)
    transformer hv_bus/lv_bus edges, finds connected components sorted by size
    (largest first), and returns a mapping from bus_id to island index
    (0 = largest island).

    Include trafos_df for topological island detection: transformers bridge
    voltage-split buses at the same physical location, so omitting them causes
    each voltage level at a multi-voltage substation to appear as a separate island.

    Args:
        lines_df: DataFrame with 'from_bus' and 'to_bus' columns.
        trafos_df: Optional DataFrame with 'hv_bus' and 'lv_bus' columns.

    Returns:
        Dict mapping bus_id to island index.
    """
    if lines_df.empty and (trafos_df is None or trafos_df.empty):
        return {}

    G: nx.Graph = nx.Graph()
    for _, row in lines_df.iterrows():
        G.add_edge(row["from_bus"], row["to_bus"])

    if trafos_df is not None and not trafos_df.empty:
        for _, row in trafos_df.iterrows():
            G.add_edge(row["hv_bus"], row["lv_bus"])

    components = sorted(nx.connected_components(G), key=len, reverse=True)
    return {bus: idx for idx, component in enumerate(components) for bus in component}


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


def _add_island_trafos(
    fig: go.Figure,
    trafos_df: pd.DataFrame,
    buses_df: pd.DataFrame,
    island_name: str,
    color: str,
    show_legend: bool,
) -> bool:
    """Add all transformers for one island as a single trace. Returns True if a trace was added."""
    bus_locations = bus_location_lookup(buses_df)

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
        bus_to_island = _compute_island_assignment(lines_df, trafos_df)

        island_buses: dict[int, set[str]] = {}
        for bus, island_idx in bus_to_island.items():
            island_buses.setdefault(island_idx, set()).add(bus)

        for island_idx in sorted(island_buses.keys()):
            buses_in_island = island_buses[island_idx]
            island_name = f"Island {island_idx + 1}"
            color = _ISLAND_COLORS[island_idx % len(_ISLAND_COLORS)]
            legend_added = False

            island_lines_df = lines_df[lines_df["from_bus"].isin(buses_in_island)]
            island_buses_df = (
                buses_df[buses_df["bus_id"].isin(buses_in_island)]
                if not buses_df.empty
                else pd.DataFrame()
            )
            island_trafos_df = (
                trafos_df[trafos_df["hv_bus"].isin(buses_in_island)]
                if not trafos_df.empty
                else pd.DataFrame()
            )
            island_loads_df = (
                loads_df[loads_df["bus_id"].isin(buses_in_island)]
                if not loads_df.empty
                else pd.DataFrame()
            )
            island_gens_df = (
                gens_df[gens_df["bus_id"].isin(buses_in_island)]
                if not gens_df.empty
                else pd.DataFrame()
            )

            if not island_lines_df.empty:
                added = _add_island_lines(
                    fig, island_lines_df, island_name, color, not legend_added
                )
                legend_added = legend_added or added
            if not island_buses_df.empty:
                added = _add_island_buses(
                    fig, island_buses_df, island_name, color, not legend_added
                )
                legend_added = legend_added or added
            if not island_trafos_df.empty:
                added = _add_island_trafos(
                    fig, island_trafos_df, buses_df, island_name, color, not legend_added
                )
                legend_added = legend_added or added
            if not island_loads_df.empty:
                added = _add_island_loads(
                    fig, island_loads_df, island_name, color, not legend_added
                )
                legend_added = legend_added or added
            if not island_gens_df.empty:
                added = _add_island_gens(fig, island_gens_df, island_name, color, not legend_added)
                legend_added = legend_added or added

    _apply_map_layout(fig, mapbox_style, center_lat, center_lon, zoom, height, title)
    return fig


def visualize_gis_islands(
    lines_geo,
    buses_geo,
    mapping: pd.Series,
    *,
    mapbox_style: str = "carto-positron",
    height: int = 800,
    center_lat: float = -27.0,
    center_lon: float = 133.0,
    zoom: int = 4,
    title: str = "NEM Network Islands (GIS)",
) -> go.Figure:
    """Visualize network islands using raw GIS geometry data.

    Works directly with the output of `_get_buses_and_lines()`, before voltage
    splitting. Each physical bus is a single node regardless of how many voltage
    levels it serves, so island detection is purely topological.

    Args:
        lines_geo: GeoDataFrame in EPSG:4326 with LineString geometry, 'name',
            'capacitykv', 'operationalstatus', 'class', 'length_km'.
        buses_geo: GeoDataFrame in EPSG:4326 with Point geometry and 'bus_id'.
        mapping: Series mapping GEO_CRS Point → bus_id (from _get_buses_and_lines).
        mapbox_style: Carto map style.
        height: Figure height in pixels.
        center_lat: Centre latitude.
        center_lon: Centre longitude.
        zoom: Initial zoom level.
        title: Figure title.

    Returns:
        go.Figure: Interactive map with one legend entry per island.

    Example:
        >>> from nemdb.models.pandapower import _get_buses_and_lines
        >>> from nemdb.models.visualize import visualize_gis_islands
        >>> lines, buses, mapping = _get_buses_and_lines()
        >>> fig = visualize_gis_islands(
        ...     lines.to_crs("EPSG:4326"),
        ...     buses.to_crs("EPSG:4326"),
        ...     mapping,
        ... )
        >>> fig.show()
    """
    # Resolve from_bus for every line (start-point lookup in mapping)
    from_buses: list[str | None] = []
    for _, row in lines_geo.iterrows():
        start = shp.get_point(row.geometry, 0)
        from_buses.append(mapping.get(start))

    lines_working = lines_geo.copy()
    lines_working["_from_bus"] = from_buses

    # Build edge list and compute island assignment
    valid = lines_working.dropna(subset=["_from_bus"])
    to_buses: list[str | None] = []
    for _, row in valid.iterrows():
        end = shp.get_point(row.geometry, -1)
        to_buses.append(mapping.get(end))

    valid = valid.copy()
    valid["_to_bus"] = to_buses
    valid = valid.dropna(subset=["_to_bus"])
    valid = valid[valid["_from_bus"] != valid["_to_bus"]]

    edges_df = valid[["_from_bus", "_to_bus"]].rename(
        columns={"_from_bus": "from_bus", "_to_bus": "to_bus"}
    )
    bus_to_island = _compute_island_assignment(edges_df)

    island_buses: dict[int, set[str]] = {}
    for bus, idx in bus_to_island.items():
        island_buses.setdefault(idx, set()).add(bus)

    fig = go.Figure()

    for island_idx in sorted(island_buses.keys()):
        buses_in_island = island_buses[island_idx]
        island_name = f"Island {island_idx + 1}"
        color = _ISLAND_COLORS[island_idx % len(_ISLAND_COLORS)]
        legend_added = False

        # Lines
        island_lines = lines_working[lines_working["_from_bus"].isin(buses_in_island)]
        lats_list: list[float | None] = []
        lons_list: list[float | None] = []
        hover_text: list[str | None] = []
        for _, row in island_lines.iterrows():
            coords = list(row.geometry.coords)
            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]
            lons_list.extend([*lons, None])
            lats_list.extend([*lats, None])
            text = (
                f"<b>{row.get('name', '')}</b><br>"
                f"Voltage: {row.get('capacitykv', '')} kV<br>"
                f"Length: {row.get('length_km', 0):.1f} km<br>"
                f"Class: {row.get('class', '')}<br>"
                f"Status: {row.get('operationalstatus', '')}"
            )
            hover_text.extend([text] * len(lats) + [None])

        if lats_list:
            fig.add_trace(
                go.Scattermap(
                    lon=lons_list,
                    lat=lats_list,
                    mode="lines",
                    name=island_name,
                    line={"width": 1.5, "color": color},
                    hovertext=hover_text,
                    hoverinfo="text",
                    showlegend=not legend_added,
                    legendgroup=island_name,
                )
            )
            legend_added = True

        # Buses
        island_bus_gdf = buses_geo[buses_geo["bus_id"].isin(buses_in_island)]
        if not island_bus_gdf.empty:
            bus_lons = island_bus_gdf.geometry.x.tolist()
            bus_lats = island_bus_gdf.geometry.y.tolist()
            hover_buses = [f"<b>{bid}</b>" for bid in island_bus_gdf["bus_id"]]
            fig.add_trace(
                go.Scattermap(
                    lon=bus_lons,
                    lat=bus_lats,
                    mode="markers",
                    marker={"size": 5, "color": color, "opacity": 0.7},
                    name=island_name,
                    hovertext=hover_buses,
                    hoverinfo="text",
                    showlegend=not legend_added,
                    legendgroup=island_name,
                )
            )

    _apply_map_layout(fig, mapbox_style, center_lat, center_lon, zoom, height, title)
    return fig
