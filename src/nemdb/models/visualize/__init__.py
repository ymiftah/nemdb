"""Interactive Plotly visualization of pandapower network models for the NEM.

This subpackage provides visualization functions for the NEM transmission network
using Plotly's MapLibre backend with Carto basemaps. All network elements
(buses, lines, transformers, generators, loads) are displayed with interactive
hover information and layer toggling via the legend.

Submodules:
    common: Shared constants (VOLTAGE_COLORS, FUEL_COLORS) and helpers.
    network_view: Voltage-coloured full-network visualization (visualize_network).
    island_view: Island-coloured visualization (visualize_islands, visualize_gis_islands).
"""

from .common import (
    DEFAULT_FUEL_COLOR,
    FUEL_COLORS,
    VOLTAGE_COLORS,
    _apply_map_layout,
    bus_location_lookup,
)
from .island_view import (
    _ISLAND_COLORS,
    _add_island_buses,
    _add_island_gens,
    _add_island_lines,
    _add_island_loads,
    _add_island_trafos,
    _compute_island_assignment,
    visualize_gis_islands,
    visualize_islands,
)
from .network_view import (
    _add_buses_to_figure,
    _add_generators_to_figure,
    _add_lines_to_figure,
    _add_loads_to_figure,
    _add_transformers_to_figure,
    visualize_network,
)

__all__ = [
    "DEFAULT_FUEL_COLOR",
    "FUEL_COLORS",
    "VOLTAGE_COLORS",
    "_ISLAND_COLORS",
    "_add_buses_to_figure",
    "_add_generators_to_figure",
    "_add_island_buses",
    "_add_island_gens",
    "_add_island_lines",
    "_add_island_loads",
    "_add_island_trafos",
    "_add_lines_to_figure",
    "_add_loads_to_figure",
    "_add_transformers_to_figure",
    "_apply_map_layout",
    "_compute_island_assignment",
    "bus_location_lookup",
    "visualize_gis_islands",
    "visualize_islands",
    "visualize_network",
]
