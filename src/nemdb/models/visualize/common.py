"""Shared constants and helpers for network visualization."""

import logging

import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

# Voltage color scale (kV -> color)
VOLTAGE_COLORS = {
    500: "#8B0000",  # Dark red
    330: "#DC143C",  # Crimson
    275: "#FF6347",  # Tomato
    220: "#FF8C00",  # Dark orange
    132: "#FFD700",  # Gold
    110: "#9ACD32",  # Yellow-green
    66: "#32CD32",  # Lime green
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


def bus_location_lookup(buses_df: pd.DataFrame) -> dict[str, tuple[float, float]]:
    """Build a {bus_id: (x, y)} mapping from a buses DataFrame.

    Args:
        buses_df: DataFrame with 'bus_id' and 'geodata' (shapely Point) columns.

    Returns:
        Dict mapping bus_id to (lon, lat) tuple for buses that have geodata.
    """
    locations: dict[str, tuple[float, float]] = {}
    for _, row in buses_df.iterrows():
        geodata = row.get("geodata")
        if geodata is None or (isinstance(geodata, float) and pd.isna(geodata)):
            continue
        locations[row["bus_id"]] = (geodata.x, geodata.y)
    return locations


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
