# Island Connectivity and Cross-Voltage Bridging

## Overview

The source GIS data contains artifacts and broken geometries that hinders the use to derive powerflow studies. A first pre-processing is described in [Transmission Line Cleaning](./transmission-line-cleaning.md), though it still results in possible islands, or voltage mismatches between lines and buses. This document explains how nemdb detects and resolves these islands to create a fully connected network model suitable for power system analysis.

## Why Islands Occur

During network construction, buses are created separately for each voltage level at each substation. This creates a multi-layer network where:

- **Layer 1**: All 66 kV buses and their transmission lines
- **Layer 2**: All 110 kV buses and their transmission lines
- **Layer 3**: All 220 kV buses and their transmission lines
- etc.

Some voltage-level buses become isolated if they lack transmission lines connecting them to the main network at the same voltage level. For example:

- A 132 kV wind farm connection might be the only 132 kV bus in its region
- A 22 kV island system might have no same-voltage transmission lines to the main grid

## Island Detection

nemdb detects islands using a graph connectivity algorithm:

1. Build a connectivity graph where:
   - **Nodes** = voltage-specific buses (e.g., `bus_64_110kv`, `bus_64_220kv`)
   - **Edges** = transmission lines + transformer connections

2. Find all connected components using depth-first search

3. Identify islands as all components except the largest one

```python
# Example: 102 islands detected initially
# - Main component: 1,506 buses (fully connected)
# - Island 1: 12 buses (isolated 66 kV cluster)
# - Island 2: 5 buses (isolated 132 kV wind farm)
# - Island 3: 4 buses (isolated 22 kV system)
# - ... 99 more single-bus or small-cluster islands
```

## Connection Strategy

Islands are connected to the main network using a **two-stage strategy**:

### Stage 1: Same-Voltage Connection (Preferred)

For each island, find the closest bus in the main network at the **same voltage level** within **50 km**:

- **Pros**: No transformer needed, simpler connectivity
- **Cons**: Not always available (some voltage levels might be isolated regionally)

**Example**: Island with `bus_1291_66kv` connects to main network's `bus_239_66kv` at 0.9 km → Direct transmission line at 66 kV

### Stage 2: Cross-Voltage Connection (with Transformer)

If no same-voltage bus exists within 50 km, connect to the **nearest bus at any voltage** and use a **synthetic transformer** to bridge the voltage difference:

- **Pros**: Always finds a connection, prevents disconnected buses
- **Cons**: Requires synthetic infrastructure (additional transformer)

**Example**: Island with `bus_474_220kv` has no same-voltage match within 50 km, nearest is `bus_600_66kv` (112.3 km, 66 kV)

## Cross-Voltage Bridging Architecture

Cross-voltage connections require careful handling to maintain network validity. The key constraint is:

> **Transmission lines must connect buses with matching nominal voltages**

### Invalid Approach (Violates Constraint)

```text
bus_474_220kv ──[line at 66kV]── bus_600_66kv  ❌
          ↑                              ↑
       220 kV                         66 kV
    Different nominal voltages connected directly
```

### Valid Approach (Constraint-Compliant)

```text
bus_474_220kv ──[line at 220kV]── bus_1700_synthetic_220kv ──[transformer]── bus_600_66kv
          ↑                              ↑                          ↑              ↑
       220 kV                        220 kV                  220kV ↔ 66kV     66 kV
    Same voltage (OK)             Same voltage (OK)      Voltage bridging    Same voltage (OK)
```

**Architecture**:

1. **Synthetic intermediate bus** created at island's voltage level, located at main network's geographic location
2. **Synthetic transmission line** connects island bus → intermediate bus (both at same voltage)
3. **Synthetic transformer** connects intermediate bus → main network bus (voltage conversion)

### Example

```python
# Island: bus_474_220kv (220 kV) isolated in northwest
# Main: bus_600_66kv (66 kV) 112.3 km away

# Step 1: Create intermediate synthetic bus at 220 kV
bus_1700_synthetic_220kv:
  - Nominal voltage: 220 kV
  - Location: Same as bus_600_66kv
  - Created: Synthetic connection point

# Step 2: Line from island to intermediate (same voltage)
Synthetic_bus_474_220kv_to_bus_1700_synthetic_220kv:
  - From: bus_474_220kv (220 kV)
  - To: bus_1700_synthetic_220kv (220 kV)
  - Voltage: 220 kV
  - Distance: 112.3 km
  - Type: Synthetic Connection

# Step 3: Transformer bridges voltage
Synthetic_trafo_bus_474_220kv_to_bus_600_66kv:
  - LV side: bus_1700_synthetic_220kv (220 kV)
  - HV side: bus_600_66kv (66 kV)  [Actually lower voltage on HV in this case]
  - Type: 220/66 kV step-down transformer
  - Capacity: 1,000 MVA
```

## Iterative Connectivity

The island connection process may be **iterative**:

1. **Iteration 1**: Connect islands to main network
   - Some islands connect directly to main
   - Other islands connect via transformers
   - Result: Previously isolated buses now part of larger connected components

2. **Iteration 2**: Check connectivity again
   - The island connection may have created new connected components
   - These "secondary islands" are then connected

3. **Repeat** until only one connected component remains

**Typical case**: Single iteration sufficient for NEM (74 islands all connected in one pass)

## Implementation Details

### Key Functions

- **`_build_connectivity_graph()`**: Builds NetworkX graph from buses, lines, and transformers
- **`_find_closest_bus_pair()`**: Finds closest bus pair with optional voltage filtering and distance limits
- **`_create_synthetic_line()`**: Creates transmission line for same-voltage connections
- **`_create_cross_voltage_connection()`**: Creates intermediate bus + line + transformer for cross-voltage
- **`_connect_islands()`**: Main orchestration function

### Parameters

- **`MAX_SAME_VOLTAGE_DISTANCE = 50.0 km`**: Maximum distance for same-voltage connections
- **`Synthetic transformer capacity = 1,000 MVA`**: Standard rating for synthetic bridging transformers

### Performance Optimization

Finding closest bus pairs is expensive due to distance calculations:

- **Fast filter**: Euclidean distance in degrees (quick, approximate)
- **Final check**: CRS-based distance calculation (slow, precise)

Only precise distance calculation is performed on best candidate pair, dramatically reducing computation time (4x faster).

## Network Validation

After island connection, the network must satisfy:

✅ **All buses connected** (single connected component)
✅ **All lines connect same-voltage buses** (nominal voltage constraint)
✅ **All transformers properly configured** (LV < HV voltage)
✅ **No isolated generators or loads** (attached to connected buses)

These are validated by the `sanity_checks()` function.

## Limitations and Considerations

1. **Synthetic infrastructure is artificial**: Synthetic lines and transformers represent the minimum connectivity needed for analysis, not actual planned infrastructure

2. **Long synthetic connections may indicate data issues**: If a synthetic line is very long (>100 km), it may indicate:
   - Regional voltage-level isolation in the actual grid
   - Data quality issues (missing transmission lines)
   - Physical connection points not represented in the GIS data

3. **Transformer parameters are generic**: Synthetic transformers use standard parameters suitable for power flow analysis but may not represent actual equipment

4. **Island detection is voltage-specific**: Islands are detected at the voltage-specific bus level, not the geographic location level. This is correct for power flow analysis where voltage levels are distinct elements.

## Example Outputs

Running the island connectivity algorithm on the NEM:

```text
Initial state:
  - Total buses: 1,673 (voltage-specific)
  - Components: 102 (1 main + 101 islands)
  - Island sizes: [1506, 12, 5, 4, 4, 3, ...]

Connections made:
  - Same-voltage: 68 islands
  - Cross-voltage: 6 islands
  - Synthetic lines created: 74
  - Synthetic buses created: 6
  - Synthetic transformers created: 6

Final state:
  - Total buses: 1,679 (1,673 original + 6 synthetic)
  - Components: 1 (fully connected)
  - All generators and loads connected ✓
  - All lines meet voltage constraints ✓
```

## Debugging

To debug island connectivity issues:

```python
from nemdb.models.pandapower import get_pandapower_model, _build_connectivity_graph
import networkx as nx

model = get_pandapower_model()
G = _build_connectivity_graph(model)
components = list(nx.connected_components(G))
components.sort(key=len, reverse=True)

print(f"Main component size: {len(components[0])}")
print(f"Island sizes: {[len(c) for c in components[1:]]}")

# Find specific island
for i, component in enumerate(components[1:], 1):
    buses_in_island = list(component)
    print(f"\nIsland {i}: {buses_in_island[:5]}...")  # First 5 buses
```

## Related Documentation

- [Transmission Line Cleaning](./transmission-line-cleaning.md) - How raw GIS data is validated and fixed
- [Build a Network Model](../tutorials/build-network-model.md) - Tutorial showing the full pipeline
