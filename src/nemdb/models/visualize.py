"""Interactive Plotly visualization of pandapower network models for the NEM.

This module provides visualization functions for the NEM transmission network
using Plotly's MapLibre backend with Carto basemaps. All network elements
(buses, lines, transformers, generators, loads) are displayed with interactive
hover information and layer toggling via the legend.
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)

# Voltage color scale (kV -> color)
VOLTAGE_COLORS = {
    500: "#8B0000",  # Dark red
    330: "#DC143C",  # Crimson
    275: "#FF6347",  # Tomato
    220: "#FF8C00",  # Dark orange
    132: "#FFD700",  # Gold
    110: "#9ACD32",  # Yellow-green
    66: "#32CD32",   # Lime green
}

# Fuel type color scale
FUEL_COLORS = {
    "Coal": "#404040",
    "Black Coal": "#404040",
    "Brown Coal": "#5C5C5C",
    "Gas": "#4169E1",
    "CCGT": "#4169E1",
    "OCGT": "#4169E1",
    "ACGT": "#4169E1",
    "Hydro": "#1E90FF",
    "Wind": "#00CED1",
    "Biomass": "#8B4513",
    "Solar": "#FFD700",
    "Photovoltaic": "#FFD700",
    "Thermal": "#808080",
    "Battery": "#9370DB",
    "Pumped Storage": "#00BFFF",
    "Interconnector": "#FF1493",
    # OpenNEM fueltech codes
    "coal_black": "#404040",
    "coal_brown": "#5C5C5C",
    "gas_ccgt": "#4169E1",
    "gas_ocgt": "#4169E1",
    "gas_acgt": "#4169E1",
    "gas_recip": "#4169E1",
    "gas_steam": "#4169E1",
    "hydro": "#1E90FF",
    "wind": "#00CED1",
    "solar_utility": "#FFD700",
    "solar_rooftop": "#FFA500",
    "biomass": "#8B4513",
    "battery_lithium": "#9370DB",
    "battery_flow": "#9370DB",
    "pumps": "#00BFFF",
}

# Default fuel color for unknown types
DEFAULT_FUEL_COLOR = "#808080"


def visualize_network(
    model: dict,
    *,
    show_buses: bool = True,
    show_lines: bool = True,
    show_generators: bool = True,
    show_loads: bool = True,
    show_transformers: bool = True,
    mapbox_style: str = "carto-positron",
    height: int = 800,
    center_lat: float = -27.0,
    center_lon: float = 133.0,
    zoom: int = 4,
    title: str = "NEM Network Visualization",
) -> go.Figure:
    """Create an interactive Plotly map visualization of the NEM network.

    Displays transmission lines, buses, generators, loads, and transformers
    on a Carto MapLibre basemap with interactive hover information and
    layer toggling via legend.

    Args:
        model: Dict returned by get_pandapower_model() with keys:
            - 'buses': DataFrame with columns [bus_id, vn_kv, in_service, geodata]
            - 'lines': DataFrame with columns [name, from_bus, to_bus, length_km,
              in_service, class, geodata, voltagekv]
            - 'generators': DataFrame with columns [bus_id, name, p_mw, max_p_mw, type,
              in_service, owner, fueltype, vn_kv, geodata]
            - 'loads': DataFrame with columns [bus_id, name, type, vn_kv, in_service,
              locality, state, geodata]
            - 'trafos': DataFrame with columns [name, lv_bus, hv_bus, vn_lv_kv, vn_hv_kv,
              sn_mva, vk_percent, vkr_percent, pfe_kw, i0_percent, in_service]
        show_buses: Whether to display bus/substation nodes
        show_lines: Whether to display transmission lines
        show_generators: Whether to display generators
        show_loads: Whether to display loads (substations)
        show_transformers: Whether to display transformers
        mapbox_style: Carto style ('carto-positron', 'carto-darkmatter', 'open-street-map')
        height: Map height in pixels
        center_lat: Center latitude (default: Australia center)
        center_lon: Center longitude (default: Australia center)
        zoom: Initial zoom level (4 = Australia-wide view)
        title: Figure title

    Returns:
        plotly.graph_objects.Figure: Interactive map that can be displayed with
            .show() or saved with .write_html("filename.html")

    Example:
        >>> from nemdb.models.pandapower_model import get_pandapower_model
        >>> from nemdb.models.visualize import visualize_network
        >>> model = get_pandapower_model()
        >>> fig = visualize_network(model)
        >>> fig.show()
    """
    fig = go.Figure()

    buses_df = model.get("buses", pd.DataFrame())
    lines_df = model.get("lines", pd.DataFrame())
    gens_df = model.get("gens", pd.DataFrame())
    loads_df = model.get("loads", pd.DataFrame())
    trafos_df = model.get("trafos", pd.DataFrame())

    # Add layers in order (bottom to top z-order)
    if show_lines and not lines_df.empty:
        _add_lines_to_figure(fig, lines_df)

    if show_buses and not buses_df.empty:
        _add_buses_to_figure(fig, buses_df)

    if show_transformers and not trafos_df.empty:
        _add_transformers_to_figure(fig, trafos_df, buses_df)

    if show_loads and not loads_df.empty:
        _add_loads_to_figure(fig, loads_df)

    if show_generators and not gens_df.empty:
        _add_generators_to_figure(fig, gens_df)

    # Update layout with map configuration
    fig.update_layout(
        title=title,
        mapbox=dict(
            style=mapbox_style,
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom,
        ),
        height=height,
        margin=dict(l=0, r=0, t=40, b=0),
        hovermode="closest",
        showlegend=True,
        legend=dict(
            x=0.01,
            y=0.99,
            bgcolor="rgba(255, 255, 255, 0.8)",
            bordercolor="rgba(0, 0, 0, 0.2)",
            borderwidth=1,
        ),
    )

    return fig


def _add_lines_to_figure(fig: go.Figure, lines_df: pd.DataFrame) -> None:
    """Add transmission line traces to figure, grouped by voltage level."""
    if lines_df.empty:
        return

    # Group lines by voltage level
    for voltage in sorted(lines_df["voltagekv"].unique(), reverse=True):
        voltage_lines = lines_df[lines_df["voltagekv"] == voltage]

        # Create traces for all lines at this voltage
        lats_list = []
        lons_list = []
        names_list = []
        from_buses_list = []
        to_buses_list = []
        lengths_list = []
        classes_list = []
        in_service_list = []

        for _, row in voltage_lines.iterrows():
            geodata = row.get("geodata")
            if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
                continue

            # Extract coordinates from geodata (list of (lon, lat) tuples)
            try:
                coords = list(geodata)
            except (TypeError, ValueError):
                continue

            if not coords:
                continue

            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]

            # Add None between line segments for proper Plotly rendering
            lats_list.extend(lats + [None])
            lons_list.extend(lons + [None])

            names_list.extend([row.get("name", "Unknown")] * len(lats))
            from_buses_list.extend([row.get("from_bus", "")] * len(lats))
            to_buses_list.extend([row.get("to_bus", "")] * len(lats))
            lengths_list.extend([row.get("length_km", 0)] * len(lats))
            classes_list.extend([row.get("class", "")] * len(lats))
            in_service_list.extend([row.get("in_service", True)] * len(lats))

        if not lats_list:
            continue

        # Create hover text with all attributes
        hover_text = []
        for i in range(len(names_list)):
            if lats_list[i] is None:
                hover_text.append(None)
            else:
                text = (
                    f"<b>{names_list[i]}</b><br>"
                    f"From Bus: {from_buses_list[i]}<br>"
                    f"To Bus: {to_buses_list[i]}<br>"
                    f"Length: {lengths_list[i]:.2f} km<br>"
                    f"Voltage: {voltage} kV<br>"
                    f"Class: {classes_list[i]}<br>"
                    f"In Service: {in_service_list[i]}"
                )
                hover_text.append(text)

        color = VOLTAGE_COLORS.get(int(voltage), "#808080")
        line_width = max(0.8, min(4, 0.8 + (voltage / 200)))

        fig.add_trace(
            go.Scattermap(
                lon=lons_list,
                lat=lats_list,
                mode="lines",
                name=f"{int(voltage)} kV Lines",
                line=dict(width=line_width, color=color),
                hovertext=hover_text,
                hoverinfo="text",
                showlegend=True,
                legendgroup="lines",
            )
        )


def _add_buses_to_figure(fig: go.Figure, buses_df: pd.DataFrame) -> None:
    """Add bus/substation markers to figure, grouped by voltage level."""
    if buses_df.empty:
        return

    # Group buses by voltage level
    for voltage in sorted(buses_df["vn_kv"].unique(), reverse=True):
        voltage_buses = buses_df[buses_df["vn_kv"] == voltage]

        lats = []
        lons = []
        bus_ids = []
        in_services = []

        for _, row in voltage_buses.iterrows():
            geodata = row.get("geodata")
            if pd.isna(geodata) or geodata is None:
                continue

            # Extract lat/lon from shapely.Point
            try:
                lon = geodata.x
                lat = geodata.y
            except AttributeError:
                continue

            lats.append(lat)
            lons.append(lon)
            bus_ids.append(row.get("bus_id", "Unknown"))
            in_services.append(row.get("in_service", True))

        if not lats:
            continue

        # Create hover text
        hover_text = [
            f"<b>{bid}</b><br>Voltage: {voltage} kV<br>In Service: {svc}"
            for bid, svc in zip(bus_ids, in_services)
        ]

        color = VOLTAGE_COLORS.get(int(voltage), "#808080")
        size = max(3, min(12, 3 + (voltage / 100)))

        fig.add_trace(
            go.Scattermap(
                lon=lons,
                lat=lats,
                mode="markers",
                marker=dict(size=size, color=color, opacity=0.7),
                name=f"{int(voltage)} kV Buses",
                text=hover_text,
                hovertext=hover_text,
                hoverinfo="text",
                showlegend=True,
                legendgroup="buses",
            )
        )


def _add_transformers_to_figure(
    fig: go.Figure, trafos_df: pd.DataFrame, buses_df: pd.DataFrame
) -> None:
    """Add transformer markers to figure at bus locations."""
    if trafos_df.empty or buses_df.empty:
        return

    # Build bus location mapping
    bus_locations = {}
    for _, row in buses_df.iterrows():
        geodata = row.get("geodata")
        if pd.isna(geodata) or geodata is None:
            continue
        try:
            bus_locations[row.get("bus_id")] = (geodata.x, geodata.y)
        except AttributeError:
            continue

    lats = []
    lons = []
    names = []
    hv_buses = []
    lv_buses = []
    sn_mvas = []
    vk_percents = []
    vkr_percents = []
    in_services = []

    for _, row in trafos_df.iterrows():
        hv_bus = row.get("hv_bus")
        lv_bus = row.get("lv_bus")

        # Try to find both buses
        if hv_bus not in bus_locations or lv_bus not in bus_locations:
            continue

        hv_lon, hv_lat = bus_locations[hv_bus]
        lv_lon, lv_lat = bus_locations[lv_bus]

        # Position transformer at midpoint between HV and LV bus
        mid_lon = (hv_lon + lv_lon) / 2
        mid_lat = (hv_lat + lv_lat) / 2

        lons.append(mid_lon)
        lats.append(mid_lat)
        names.append(row.get("name", "Unknown Trafo"))
        hv_buses.append(hv_bus)
        lv_buses.append(lv_bus)
        sn_mvas.append(row.get("sn_mva", 0))
        vk_percents.append(row.get("vk_percent", 0))
        vkr_percents.append(row.get("vkr_percent", 0))
        in_services.append(row.get("in_service", True))

    if not lats:
        return

    # Create hover text with all transformer attributes
    hover_text = [
        f"<b>{name}</b><br>"
        f"HV Bus: {hv_bus}<br>"
        f"LV Bus: {lv_bus}<br>"
        f"Power Rating (MVA): {sn_mva:.0f}<br>"
        f"Impedance (Vk %): {vk_pct:.2f}%<br>"
        f"Resistance (Vkr %): {vkr_pct:.2f}%<br>"
        f"In Service: {svc}"
        for name, hv_bus, lv_bus, sn_mva, vk_pct, vkr_pct, svc in zip(
            names, hv_buses, lv_buses, sn_mvas, vk_percents, vkr_percents, in_services
        )
    ]

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker=dict(size=8, color="#9370DB", symbol="star", opacity=0.8),
            name="Transformers",
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=True,
            legendgroup="transformers",
        )
    )


def _add_loads_to_figure(fig: go.Figure, loads_df: pd.DataFrame) -> None:
    """Add load/substation markers to figure."""
    if loads_df.empty:
        return

    lats = []
    lons = []
    names = []
    types = []
    voltages = []
    localities = []
    states = []
    in_services = []

    for _, row in loads_df.iterrows():
        geodata = row.get("geodata")
        if pd.isna(geodata) or geodata is None:
            continue

        try:
            lon = geodata.x
            lat = geodata.y
        except AttributeError:
            continue

        lats.append(lat)
        lons.append(lon)
        names.append(row.get("name", "Unknown"))
        types.append(row.get("type", ""))
        voltages.append(row.get("vn_kv", 0))
        localities.append(row.get("locality", ""))
        states.append(row.get("state", ""))
        in_services.append(row.get("in_service", True))

    if not lats:
        return

    # Create hover text with all load attributes
    hover_text = [
        f"<b>{name}</b><br>"
        f"Type: {ltype}<br>"
        f"Voltage: {voltage} kV<br>"
        f"Locality: {locality}<br>"
        f"State: {state}<br>"
        f"In Service: {svc}"
        for name, ltype, voltage, locality, state, svc in zip(
            names, types, voltages, localities, states, in_services
        )
    ]

    fig.add_trace(
        go.Scattermap(
            lon=lons,
            lat=lats,
            mode="markers",
            marker=dict(size=4, color="#808080", symbol="triangle-down", opacity=0.6),
            name="Loads (Substations)",
            hovertext=hover_text,
            hoverinfo="text",
            showlegend=True,
            legendgroup="loads",
        )
    )


def _add_generators_to_figure(fig: go.Figure, gens_df: pd.DataFrame) -> None:
    """Add generator markers to figure, grouped by fuel type."""
    if gens_df.empty:
        return

    # Group generators by fuel type
    fuel_types = gens_df["fueltype"].unique()
    for fuel_type in fuel_types:
        fuel_gens = gens_df[gens_df["fueltype"] == fuel_type]

        lats = []
        lons = []
        names = []
        capacities = []
        p_mws = []
        types = []
        owners = []
        in_services = []

        for _, row in fuel_gens.iterrows():
            geodata = row.get("geodata")
            if pd.isna(geodata) or geodata is None:
                continue

            try:
                lon = geodata.x
                lat = geodata.y
            except AttributeError:
                continue

            lats.append(lat)
            lons.append(lon)
            names.append(row.get("name", "Unknown"))
            capacities.append(row.get("max_p_mw", 0))
            p_mws.append(row.get("p_mw", 0))
            types.append(row.get("type", ""))
            owners.append(row.get("owner", "Unknown"))
            in_services.append(row.get("in_service", True))

        if not lats:
            continue

        # Create hover text with all generator attributes
        hover_text = [
            f"<b>{name}</b><br>"
            f"Fuel Type: {fuel_type}<br>"
            f"Type: {gtype}<br>"
            f"Capacity: {cap:.0f} MW<br>"
            f"Output: {p_mw:.0f} MW<br>"
            f"Owner: {owner}<br>"
            f"In Service: {svc}"
            for name, gtype, cap, p_mw, owner, svc in zip(
                names, types, capacities, p_mws, owners, in_services
            )
        ]

        # Size based on log capacity
        import math

        sizes = [
            max(5, min(20, 5 + math.log10(max(1, cap)) * 3)) for cap in capacities
        ]

        color = FUEL_COLORS.get(fuel_type, DEFAULT_FUEL_COLOR)

        fig.add_trace(
            go.Scattermap(
                lon=lons,
                lat=lats,
                mode="markers",
                marker=dict(size=sizes, color=color, opacity=0.8),
                name=f"Generators ({fuel_type})",
                hovertext=hover_text,
                hoverinfo="text",
                showlegend=True,
                legendgroup="generators",
            )
        )
