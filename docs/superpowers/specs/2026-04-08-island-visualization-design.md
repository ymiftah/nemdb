# Island Visualization Design

**Date:** 2026-04-08
**File:** `src/nemdb/models/visualize.py`

## Overview

Add a `visualize_islands(model)` standalone function to `visualize.py` that colors all network elements (buses, lines, generators, loads, transformers) by their connected island (networkx connected component). Clicking an island in the legend toggles all of its elements.

## Architecture

### New public function

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
```

Same signature shape as `visualize_network`. Takes the dict returned by `get_pandapower_model()`.

### New private helper

```python
def _compute_island_assignment(lines_df: pd.DataFrame) -> dict[str, int]:
```

- Builds a `networkx.Graph` with all unique bus IDs as nodes and `(from_bus, to_bus)` as edges
- Finds connected components via `nx.connected_components`
- Sorts components largest→smallest
- Returns `{bus_id: island_index}` (0-indexed, island 0 is the largest)

### Color palette

Uses `plotly.colors.qualitative.Dark24` (24 distinct colors). Islands beyond 24 cycle with modulo. Island labels in the legend are "Island 1", "Island 2", ... (1-indexed for display).

## Trace Structure

For each island, five traces are added in order: lines, buses, transformers, loads, generators. All five share:

- `legendgroup="Island N"` (e.g. `"Island 1"`)
- The same island color
- `showlegend=True` on the **first non-empty trace** for that island; `False` on all subsequent traces

This produces one legend entry per island that toggles all its elements together.

### Element membership

| Element    | Filtering key              |
|------------|---------------------------|
| Lines      | `from_bus` in island's buses |
| Buses      | `bus_id` in island's buses  |
| Generators | `bus_id` in island's buses  |
| Loads      | `bus_id` in island's buses  |
| Transformers | `hv_bus` in island's buses |

### Marker/line styles

Same visual styles as existing `_add_*` helpers (sizes, shapes, opacity), but color is the island color instead of voltage/fuel color. Line width still scales with `voltagekv`.

### Hover text

Identical field layout to the existing `_add_*` helpers.

## Data Flow

```text
model["lines"] ──► _compute_island_assignment ──► bus_to_island: dict[str, int]
                                                        │
model["buses"] ──────────────────────────────────────► filter by island
model["gens"]  ──────────────────────────────────────► filter by island
model["loads"] ──────────────────────────────────────► filter by island
model["trafos"] ─────────────────────────────────────► filter by island
                                                        │
                                                   add traces with island color
                                                        │
                                                   go.Figure
```

## Out of Scope

- Naming islands by region/state (always numbered)
- Per-element-type legend toggle within an island
- Merging with `visualize_network` as a flag/mode
