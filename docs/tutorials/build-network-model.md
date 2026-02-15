# Build a network model

This tutorial walks you through building a pandapower network model of the NEM transmission grid from Geoscience Australia's GIS data. You will read geographic data, build a power system model, and visualise it on an interactive map.

## Prerequisites

- nemdb installed with the `grid` and `viz` dependency groups:

  ```bash
  uv pip install -e ".[grid,viz]"
  ```

- Internet connection (GIS data is fetched from Geoscience Australia on first run)

## 1. Read geographic data

nemdb fetches infrastructure data from Geoscience Australia's REST API. Results are cached locally as Parquet files after the first call.

```python
from nemdb.geodata import read_substations, read_transmission_lines, read_major_powerstations

# Substations (point geometries)
substations = read_substations()
print(f"Substations: {len(substations)}")
print(substations[["name", "state", "voltagekv"]].head())

# Transmission lines (line/multiline geometries)
lines = read_transmission_lines(clean=True)
print(f"\nTransmission lines: {len(lines)}")
print(lines[["name", "capacitykv", "state"]].head())

# Power stations (point geometries)
stations = read_major_powerstations()
print(f"\nPower stations: {len(stations)}")
print(stations[["name", "generationtype", "generationmw"]].head())
```

The `clean=True` flag on `read_transmission_lines` applies a topology-cleaning pipeline that fixes disconnected segments, bridges small gaps, and reconstructs traversal paths. See [Transmission line cleaning](../explanation/transmission-line-cleaning.md) for details.

## 2. Build the pandapower model

The `get_pandapower_model()` function orchestrates the full pipeline:

1. Reads and cleans transmission lines
2. Extracts bus locations from line endpoints using DBSCAN clustering
3. Maps lines to from/to buses
4. Infers transformers where multiple voltage levels meet at a bus
5. Attaches generators (from power station data) and loads (from substations)
6. Validates and fixes connectivity (removes orphan buses, connects islands with synthetic transformers)

```python
from nemdb.models.pandapower import get_pandapower_model

model = get_pandapower_model()

print(f"Buses:        {len(model['buses'])}")
print(f"Lines:        {len(model['lines'])}")
print(f"Transformers: {len(model['trafos'])}")
print(f"Generators:   {len(model['gens'])}")
print(f"Loads:        {len(model['loads'])}")
```

Isolated voltage-level islands are detected and connected using a two-stage strategy: same-voltage connections within 50 km (preferred) or cross-voltage connections with synthetic transformers. See [Island Connectivity and Cross-Voltage Bridging](../explanation/island-connectivity.md) for architectural details.

### Using OpenNEM facilities as generators

For more detailed generator data (unit-level codes, fuel tech, capacity), use the OpenNEM variant:

```python
from nemdb.models.pandapower import get_pandapower_model_with_opennem

model = get_pandapower_model_with_opennem()
print(model["gens"][["name", "code", "type", "max_p_mw"]].head(10))
```

## 3. Create a pandapower network

Convert the model dict into a pandapower `Network` object for power flow analysis:

```python
from nemdb.models.pandapower import create_pandapower_network

net = create_pandapower_network(use_opennem=True)
print(net)
```

This adds external grid connections at key NEM substations (Torrens Island, Thomastown, George Town, Sydney West, South Pine) and handles disconnected components.

### Run diagnostics

```python
from nemdb.models.pandapower import sanity_checks

results = sanity_checks(net)
for check, issues in results.items():
    if issues:
        print(f"{check}: {issues}")
```

## 4. Visualise the network

Create an interactive Plotly map:

```python
from nemdb.models.visualize import visualize_network

fig = visualize_network(model)
fig.show()
```

The map displays:

- **Transmission lines** colour-coded by voltage (500 kV dark red to 66 kV green)
- **Buses** sized by voltage level
- **Generators** sized by capacity, coloured by fuel type
- **Loads** (substations) as grey triangles
- **Transformers** as purple stars

Toggle layers on/off using the legend. Hover over elements for detailed information.

Save to HTML for sharing:

```python
fig.write_html("nem_network.html")
```

### Customise the visualisation

```python
fig = visualize_network(
    model,
    show_transformers=False,
    show_loads=False,
    mapbox_style="carto-darkmatter",
    height=1000,
    center_lat=-33.8,
    center_lon=151.2,
    zoom=8,
    title="Sydney region transmission network",
)
fig.show()
```

## Next steps

- [Work with geodata](../how-to/work-with-geodata.md) -- more detail on GIS data functions
- [Transmission line cleaning](../explanation/transmission-line-cleaning.md) -- how the cleaning algorithms work
- [Island connectivity and cross-voltage bridging](../explanation/island-connectivity.md) -- how isolated voltage-level islands are connected
- [Use OpenNEM API](../how-to/use-opennem-api.md) -- query facility and generation data
