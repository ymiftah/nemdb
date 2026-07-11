import geopandas as gpd
import networkx as nx
import pandas as pd

from nemdb.logger import log
from nemdb.models.pandapower import geo_utils
from nemdb.models.pandapower.topology import GEO_CRS, METRIC_CRS


def _calculate_distance_km(geom1, geom2) -> float:
    """Calculate distance between two geometries in kilometers.

    Args:
        geom1: Shapely geometry in geographic CRS
        geom2: Shapely geometry in geographic CRS

    Returns:
        Distance in kilometers
    """
    gdf1 = gpd.GeoDataFrame([{"geometry": geom1}], crs=GEO_CRS).to_crs(METRIC_CRS)
    gdf2 = gpd.GeoDataFrame([{"geometry": geom2}], crs=GEO_CRS).to_crs(METRIC_CRS)
    return float(gdf1.geometry.iloc[0].distance(gdf2.geometry.iloc[0]) / 1000)


def _find_closest_bus_pair(
    island_buses, main_buses, same_voltage: bool = True, max_distance_km: float = float("inf")
):
    """Find the closest pair of buses between island and main component.

    Args:
        island_buses: DataFrame of buses in island (with 'geodata' geometry column)
        main_buses: DataFrame of buses in main component (with 'geodata' geometry column)
        same_voltage: If True, only connect buses at the same voltage level
        max_distance_km: Maximum distance threshold in km. Returns None if no pair within this distance.

    Returns:
        Tuple of (island_bus_id, main_bus_id, distance_km) or (None, None, inf)
    """
    candidates = main_buses
    if same_voltage and "vn_kv" in island_buses.columns and not island_buses.empty:
        island_voltage = island_buses["vn_kv"].iloc[0]
        candidates = main_buses[main_buses["vn_kv"] == island_voltage]

    def _to_metric_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
        # Materialise the index as a 'bus_id' column so sjoin_nearest produces
        # 'bus_id_src'/'bus_id_cand' columns that nearest_bus_pair relies on.
        # We must clear the index *name* first: if the index is already named
        # 'bus_id' (as it is when buses come from model["buses"].set_index("bus_id")),
        # geopandas' internal reset_index() would create a duplicate column.
        tmp = df.copy()
        tmp["bus_id"] = tmp.index
        tmp.index.name = None
        return gpd.GeoDataFrame(tmp, geometry="geodata", crs=GEO_CRS).to_crs(METRIC_CRS)

    src = _to_metric_gdf(island_buses)
    cand = _to_metric_gdf(candidates)
    src_id, cand_id, dist_m = geo_utils.nearest_bus_pair(
        src, cand, max_distance_m=max_distance_km * 1000, geometry_col="geodata"
    )
    if src_id is None:
        return None, None, float("inf")
    return src_id, cand_id, dist_m / 1000


def _create_synthetic_line(best_island_bus, best_main_bus, buses_df):
    """Create a synthetic transmission line to connect an island.

    Returns:
        Dictionary with line parameters for same-voltage connections.
    """
    island_geom = buses_df.loc[best_island_bus, "geodata"]
    main_geom = buses_df.loc[best_main_bus, "geodata"]
    distance_km = _calculate_distance_km(island_geom, main_geom)
    island_voltage = buses_df.loc[best_island_bus, "vn_kv"]

    return {
        "name": f"Synthetic_{best_island_bus}_to_{best_main_bus}",
        "from_bus": best_island_bus,
        "to_bus": best_main_bus,
        "length_km": distance_km,
        "in_service": True,
        "class": "Synthetic Connection",
        "geodata": [(island_geom.x, island_geom.y), (main_geom.x, main_geom.y)],
        "voltagekv": island_voltage,
    }


def _create_cross_voltage_connection(best_island_bus, best_main_bus, buses_df, bus_counter):
    """Create a cross-voltage connection using intermediate synthetic bus.

    Implements three-part architecture to avoid directly connecting buses with different
    nominal voltages (which violates network constraints):

    1. Create synthetic intermediate bus at island voltage (same as island_bus)
    2. Connect island_bus → intermediate with transmission line (same voltage ✓)
    3. Connect intermediate → main_bus with transformer (voltage conversion)

    This ensures all transmission lines connect buses at matching voltage levels while
    properly bridging the voltage difference through a transformer at the intermediate
    location.

    Args:
        best_island_bus: Bus ID in isolated island (e.g., 'bus_474_220kv')
        best_main_bus: Closest bus in main network (e.g., 'bus_600_66kv')
        buses_df: Bus dataframe with geodata and voltage info
        bus_counter: List with counter for generating unique synthetic bus IDs

    Returns:
        Tuple of (synthetic_bus_dict, line_dict, transformer_dict)
    """
    island_voltage = buses_df.loc[best_island_bus, "vn_kv"]
    main_voltage = buses_df.loc[best_main_bus, "vn_kv"]
    distance_km = _calculate_distance_km(
        buses_df.loc[best_island_bus, "geodata"],
        buses_df.loc[best_main_bus, "geodata"],
    )

    bus_counter[0] += 1
    # Create intermediate bus at ISLAND voltage, located at main bus location
    synthetic_bus_id = f"bus_{bus_counter[0]}_synthetic_{island_voltage:.0f}kv"

    synthetic_bus = {
        "bus_id": synthetic_bus_id,
        "vn_kv": island_voltage,  # Same voltage as island bus
        "geodata": buses_df.loc[best_main_bus, "geodata"],  # Located at main bus location
        "in_service": True,
        "type": "n",
        "zone": buses_df.loc[best_main_bus, "zone"] if "zone" in buses_df.columns else None,
    }

    # Line from island to intermediate (same voltage - no mismatch)
    island_geom = buses_df.loc[best_island_bus, "geodata"]
    main_geom = buses_df.loc[best_main_bus, "geodata"]
    line_to_intermediate = {
        "name": f"Synthetic_{best_island_bus}_to_{synthetic_bus_id}",
        "from_bus": best_island_bus,
        "to_bus": synthetic_bus_id,
        "length_km": distance_km,
        "in_service": True,
        "class": "Synthetic Connection",
        "geodata": [(island_geom.x, island_geom.y), (main_geom.x, main_geom.y)],
        "voltagekv": island_voltage,  # Same voltage as both ends
    }

    # Transformer from synthetic intermediate to main bus
    if island_voltage < main_voltage:
        lv_bus, hv_bus = synthetic_bus_id, best_main_bus
        vn_lv_kv, vn_hv_kv = island_voltage, main_voltage
    else:
        lv_bus, hv_bus = best_main_bus, synthetic_bus_id
        vn_lv_kv, vn_hv_kv = main_voltage, island_voltage

    transformer = {
        "name": f"Synthetic_trafo_{best_island_bus}_to_{best_main_bus}",
        "lv_bus": lv_bus,
        "hv_bus": hv_bus,
        "vn_lv_kv": vn_lv_kv,
        "vn_hv_kv": vn_hv_kv,
        "sn_mva": 1_000,
        "vk_percent": 12.2,
        "vkr_percent": 0.25,
        "pfe_kw": 60.0,
        "i0_percent": 0.06,
        "in_service": True,
    }

    return synthetic_bus, line_to_intermediate, transformer


def _connect_islands(
    model: dict, components: list, buses_df, main_component, diagnostics: dict
) -> bool:
    """Connect disconnected island components to the main network.

    Connection strategy:
    1. Try same voltage within 50 km (preferred - no transformer needed)
    2. Connect to nearest bus (any voltage, any distance) and add transformer to bridge voltages

    For cross-voltage connections, creates synthetic intermediate buses to avoid directly
    connecting buses with different nominal voltages.

    Args:
        model: Model dict with 'buses', 'lines', 'trafos', 'gens', 'loads'
        components: List of connected components from the graph
        buses_df: Bus dataframe indexed by bus_id
        main_component: The largest connected component
        diagnostics: Diagnostics dict to update with added line count

    Returns:
        True if islands were connected, False if unable to connect
    """
    new_lines = []
    new_trafos = []
    new_buses = []
    MAX_SAME_VOLTAGE_DISTANCE = 50.0  # 50 km threshold for same-voltage connections
    bus_counter = [max([int(b.split("_")[1]) for b in buses_df.index if "_" in b] or [1000])]

    for island_component in components[1:]:
        island_buses = buses_df.loc[list(island_component)]
        main_buses = buses_df.loc[list(main_component)]

        # Strategy 1: Same voltage within 50 km (preferred - no transformer needed)
        best_island_bus, best_main_bus, min_distance = _find_closest_bus_pair(
            island_buses, main_buses, same_voltage=True, max_distance_km=MAX_SAME_VOLTAGE_DISTANCE
        )
        connection_method = None
        needs_transformer = False

        if best_island_bus and best_main_bus:
            connection_method = f"same-voltage ({min_distance:.1f} km)"
        else:
            # Strategy 2: Connect to nearest bus (any voltage, any distance) with transformer
            best_island_bus, best_main_bus, min_distance = _find_closest_bus_pair(
                island_buses, main_buses, same_voltage=False, max_distance_km=float("inf")
            )
            if best_island_bus and best_main_bus:
                connection_method = f"cross-voltage via transformer ({min_distance:.1f} km)"
                needs_transformer = True

        if best_island_bus and best_main_bus:
            island_voltage = buses_df.loc[best_island_bus, "vn_kv"]
            main_voltage = buses_df.loc[best_main_bus, "vn_kv"]
            distance_km = _calculate_distance_km(
                buses_df.loc[best_island_bus, "geodata"],
                buses_df.loc[best_main_bus, "geodata"],
            )

            log.debug(
                f"  Connecting island bus {best_island_bus} to main bus {best_main_bus} "
                f"({distance_km:.1f} km, {island_voltage} kV, {len(island_component)} buses in island) "
                f"via {connection_method}"
            )

            if needs_transformer and island_voltage != main_voltage:
                # Cross-voltage: create synthetic intermediate bus
                syn_bus, line, trafo = _create_cross_voltage_connection(
                    best_island_bus, best_main_bus, buses_df, bus_counter
                )
                new_buses.append(syn_bus)
                new_lines.append(line)
                new_trafos.append(trafo)
                diagnostics["added_lines"] += 1
                log.debug(
                    f"    Created synthetic intermediate bus {syn_bus['bus_id']} ({main_voltage} kV)"
                )
                log.debug(
                    f"    Added synthetic transformer: {trafo['name']} "
                    f"({trafo['vn_lv_kv']} kV ↔ {trafo['vn_hv_kv']} kV)"
                )
            else:
                # Same voltage: simple line
                new_lines.append(_create_synthetic_line(best_island_bus, best_main_bus, buses_df))
                diagnostics["added_lines"] += 1

            diagnostics["connected_buses"] += len(island_component)

    if new_buses:
        model["buses"] = pd.concat([model["buses"], pd.DataFrame(new_buses)], ignore_index=True)

    if new_lines:
        model["lines"] = pd.concat([model["lines"], pd.DataFrame(new_lines)], ignore_index=True)

    if new_trafos:
        model["trafos"] = pd.concat([model["trafos"], pd.DataFrame(new_trafos)], ignore_index=True)

    return len(new_lines) > 0


def _build_connectivity_graph(model: dict) -> nx.Graph:
    """Build a graph from network connections.

    Args:
        model: Model dict with 'buses', 'lines', 'trafos', 'gens', 'loads'

    Returns:
        NetworkX graph of network connectivity
    """
    G = nx.Graph()

    # All buses are nodes — including those with no lines/trafos/gens/loads,
    # so isolated voltage-level orphans are visible as singleton components.
    G.add_nodes_from(model["buses"]["bus_id"])

    # Only in-service lines/trafos provide electrical connectivity.
    # Out-of-service elements (decommissioned lines, open switches) must not
    # be counted as edges or _validate_and_fix_connectivity will miss buses
    # that are disconnected in the live electrical model.
    for _, row in model["lines"].iterrows():
        if row.get("in_service", True) and pd.notna(row["from_bus"]) and pd.notna(row["to_bus"]):
            G.add_edge(row["from_bus"], row["to_bus"])

    for _, row in model["trafos"].iterrows():
        if row.get("in_service", True) and pd.notna(row["hv_bus"]) and pd.notna(row["lv_bus"]):
            G.add_edge(row["hv_bus"], row["lv_bus"])

    return G


def _validate_and_fix_connectivity(model: dict, max_iterations: int = 5) -> tuple[dict, dict]:
    """Validate and fix connectivity at the voltage-specific bus level.

    Builds a graph from lines and transformers (including voltage suffixes)
    and connects any disconnected islands to the main network.

    Args:
        model: Model dict with 'buses', 'lines', 'trafos', 'gens', 'loads'
        max_iterations: Maximum iterations for fixing connectivity

    Returns:
        (fixed_model, diagnostics) tuple
    """
    diagnostics = {
        "iterations": 0,
        "added_lines": 0,
        "connected_buses": 0,
    }

    buses_df = model["buses"].set_index("bus_id")

    for iteration in range(max_iterations):
        diagnostics["iterations"] = iteration + 1

        G = _build_connectivity_graph(model)

        if len(G.nodes) == 0:
            log.error("Network graph is empty")
            break

        components = list(nx.connected_components(G))
        if len(components) == 1:
            log.debug(f"Network fully connected after {iteration} iteration(s)")
            break

        components.sort(key=len, reverse=True)
        main_component = components[0]

        log.debug(
            f"Iteration {iteration + 1}: Found {len(components)} components, "
            f"connecting {len(components) - 1} island(s) to main network"
        )

        if not _connect_islands(model, components, buses_df, main_component, diagnostics):
            break

    if diagnostics["added_lines"] > 0:
        log.info(
            f"Connected {diagnostics['connected_buses']} buses in islands "
            f"by adding {diagnostics['added_lines']} synthetic transmission line(s)"
        )

    return model, diagnostics
