from typing import Any, Literal

import geopandas as gpd
import networkx as nx
import pandapower as pp
import pandas as pd
import shapely as shp
from pandapower.diagnostic.diagnostic_functions import (
    DeviationFromStdType,
    DisconnectedElements,
    ImplausibleImpedanceValues,
    InvalidValues,
    MissingBusIndices,
    MultipleVoltageControllingElementsPerBus,
    NoExtGrid,
    NominalVoltagesMismatch,
)
from sklearn.cluster import DBSCAN

from nemdb.geodata.geodata import (
    read_major_powerstations,
    read_substations,
    read_transmission_lines,
)
from nemdb.geodata.matching import match_facilities_to_gis
from nemdb.logger import log

METRIC_CRS = "EPSG:7856"
GEO_CRS = "EPSG:4326"

# NEM states (excludes WA and NT)
NEM_STATES = [
    "New South Wales",
    "Victoria",
    "Queensland",
    "South Australia",
    "Tasmania",
    "Australian Capital Territory",
]


def _diagnose_graph(lines, buses, mapping):
    """Diagnose graph structure for connectivity issues.

    Args:
        lines: GeoDataFrame with transmission lines
        buses: GeoDataFrame with buses
        mapping: Series mapping geometry points to bus_id

    Returns:
        dict with diagnostic information:
            - orphan_buses: list of bus_ids with no connections
            - islands: list of sets, each set contains bus_ids in an island
            - self_loops: list of line indices with same start/end bus
            - total_buses: total number of buses
            - total_lines: total number of lines
    """
    diagnostics = {
        "orphan_buses": [],
        "islands": [],
        "self_loops": [],
        "total_buses": len(buses),
        "total_lines": len(lines),
    }

    # Build graph from lines
    G = nx.Graph()

    # Add all buses as nodes
    for bus_id in buses["bus_id"]:
        G.add_node(bus_id)

    # Map line endpoints to bus_ids and add edges
    for idx, line in lines.iterrows():
        start_point = line["start_point"]
        end_point = line["end_point"]

        # Convert to geographic CRS for mapping lookup
        gdf_start = gpd.GeoDataFrame([{"geometry": start_point}], crs=METRIC_CRS).to_crs(GEO_CRS)
        gdf_end = gpd.GeoDataFrame([{"geometry": end_point}], crs=METRIC_CRS).to_crs(GEO_CRS)

        start_geom = gdf_start.geometry.iloc[0]
        end_geom = gdf_end.geometry.iloc[0]

        # Get bus_ids from mapping
        from_bus = mapping.get(start_geom)
        to_bus = mapping.get(end_geom)

        if from_bus is None or to_bus is None:
            log.warning(f"Line {idx} has unmapped endpoint(s): {from_bus} -> {to_bus}")
            continue

        # Check for self-loops
        if from_bus == to_bus:
            diagnostics["self_loops"].append(idx)
        else:
            G.add_edge(from_bus, to_bus)

    # Find orphan buses (nodes with degree 0)
    for node in G.nodes():
        if G.degree(node) == 0:
            diagnostics["orphan_buses"].append(node)

    # Find islands (connected components)
    components = list(nx.connected_components(G))
    diagnostics["islands"] = components

    return diagnostics


def _correct_graph(lines, buses, mapping, diagnostics):
    """Correct graph structure based on diagnostics.

    Args:
        lines: GeoDataFrame with transmission lines
        buses: GeoDataFrame with buses
        mapping: Series mapping geometry points to bus_id
        diagnostics: dict from _diagnose_graph

    Returns:
        Tuple of (corrected_lines, corrected_buses, corrected_mapping, correction_stats)
    """
    correction_stats = {
        "orphan_buses": 0,
        "self_loops": 0,
        "islands": len(diagnostics["islands"]),
    }

    # Remove self-loops
    if diagnostics["self_loops"]:
        log.info(f"Removing {len(diagnostics['self_loops'])} self-loop line(s)")
        lines = lines.drop(index=diagnostics["self_loops"]).reset_index(drop=True)
        correction_stats["self_loops"] = len(diagnostics["self_loops"])

    # Remove orphan buses
    if diagnostics["orphan_buses"]:
        log.info(f"Removing {len(diagnostics['orphan_buses'])} orphan bus(es)")
        buses_to_keep = ~buses["bus_id"].isin(diagnostics["orphan_buses"])
        buses = buses[buses_to_keep].reset_index(drop=True)

        # Update mapping to remove orphan buses
        mapping = mapping[~mapping.isin(diagnostics["orphan_buses"])]
        correction_stats["orphan_buses"] = len(diagnostics["orphan_buses"])

    # Report islands (but don't connect them at this stage)
    if len(diagnostics["islands"]) > 1:
        island_sizes = [len(island) for island in diagnostics["islands"]]
        log.warning(
            f"Graph has {len(diagnostics['islands'])} island(s) with sizes: {sorted(island_sizes, reverse=True)}. "
            f"Islands will be connected later at the model level with voltage-specific buses."
        )

    return lines, buses, mapping, correction_stats


def _validate_and_correct_graph(lines, buses, mapping):
    """Validate and correct graph structure.

    This ensures:
    1. No orphan buses (buses with no line connections)
    2. No self-loops (lines with same start and end bus)
    3. Reports islands for later correction at model level

    Args:
        lines: GeoDataFrame with transmission lines
        buses: GeoDataFrame with buses
        mapping: Series mapping geometry points to bus_id

    Returns:
        Tuple of (corrected_lines, corrected_buses, corrected_mapping, diagnostics)
    """
    # Run diagnostics
    diagnostics = _diagnose_graph(lines, buses, mapping)

    # Apply corrections
    lines, buses, mapping, correction_stats = _correct_graph(lines, buses, mapping, diagnostics)

    # Combine diagnostics and correction stats for return
    final_diagnostics = {
        "total_buses": len(buses),
        "total_lines": len(lines),
        "orphan_buses": correction_stats["orphan_buses"],
        "self_loops": correction_stats["self_loops"],
        "islands": correction_stats["islands"],
    }

    return lines, buses, mapping, final_diagnostics


def get_pandapower_model():
    lines, buses, mapping = _get_buses_and_lines()

    pf_lines = _get_lines_pp(lines, mapping)
    pf_buses = _get_bus_pp(pf_lines, buses)
    pf_trafos = _get_trafos_pp(pf_buses)
    pf_gens = _get_gens_pp(pf_buses)
    pf_loads = _get_loads_pp(pf_buses)

    model = {
        "buses": pf_buses,
        "lines": pf_lines,
        "trafos": pf_trafos,
        "gens": pf_gens,
        "loads": pf_loads,
    }
    return model


def _get_buses_and_lines():
    # Extract line segments
    lines = (
        read_transmission_lines(clean=True)
        .query("state in @NEM_STATES")  # Filter to NEM states only
        .reset_index(drop=True)
        .rename_axis("line_id")
        .reset_index("line_id")
        .explode()
    )
    lines = lines.to_crs(METRIC_CRS)
    lines["length_km"] = shp.length(lines.geometry) / 1000

    # Extract buses from extremeties
    lines["start_point"] = lines.geometry.map(
        lambda linestringgeometry: shp.get_point(linestringgeometry, 0)
    )
    lines["end_point"] = lines.geometry.map(
        lambda linestringgeometry: shp.get_point(linestringgeometry, -1)
    )

    extremeties: gpd.GeoDataFrame = (
        gpd.GeoDataFrame(
            geometry=lines["start_point"].drop_duplicates().to_list()
            + lines["end_point"].drop_duplicates().to_list(),
            crs=lines.crs,
        )
        .drop_duplicates()
        .reset_index(names=["extremeties_id"])
        .assign(x=lambda x: x.geometry.x, y=lambda x: x.geometry.y)
    )

    # join extremities together into buses using a DBSCAN algorithm
    fitted_ = DBSCAN(eps=500, min_samples=1).fit(extremeties[["x", "y"]])
    extremeties["bus_id"] = "bus_" + fitted_.labels_.astype(str)
    buses = (
        extremeties.groupby("bus_id", as_index=False)
        .agg({"geometry": lambda s: shp.centroid(shp.MultiPoint(s))})
        .set_geometry("geometry", crs=METRIC_CRS)
    )

    mapping = extremeties.to_crs(GEO_CRS).set_index("geometry")["bus_id"]

    # Validate and correct the graph structure
    lines, buses, mapping, diagnostics = _validate_and_correct_graph(lines, buses, mapping)

    log.info(
        f"Graph validation: {diagnostics['total_buses']} buses, "
        f"{diagnostics['total_lines']} lines, "
        f"{diagnostics['islands']} island(s), "
        f"{diagnostics['orphan_buses']} orphan bus(es) removed, "
        f"{diagnostics['self_loops']} self-loop(s) removed"
    )

    return lines, buses, mapping


def _get_lines_pp(lines, mapping):
    rows = []
    for _, row in lines.to_crs(GEO_CRS).iterrows():
        start_point = shp.get_point(row.geometry, 0)
        end_point = shp.get_point(row.geometry, -1)
        from_bus = mapping[start_point]
        to_bus = mapping[end_point]
        rows.append(
            {
                "name": row["name"],
                "from_bus": from_bus,
                "to_bus": to_bus,
                "length_km": row["length_km"],
                "in_service": row["operationalstatus"] == "Operational",
                "class": row["class"],
                "geodata": row["geometry"].coords,
                "voltagekv": row["capacitykv"],
            }
        )
    df = pd.DataFrame(rows)
    df = df[df["to_bus"] != df["from_bus"]]  # Remove lines going to the same bus
    df["from_bus"] = df["from_bus"] + "_" + df["voltagekv"].astype(str) + "kv"
    df["to_bus"] = df["to_bus"] + "_" + df["voltagekv"].astype(str) + "kv"

    return df


def _get_bus_pp(pf_lines, buses):
    pf_buses = (
        pd.concat(
            (
                pf_lines[["from_bus", "voltagekv"]].rename(columns={"from_bus": "bus"}),
                pf_lines[["to_bus", "voltagekv"]].rename(columns={"to_bus": "bus"}),
            ),
            axis=0,
        )
        .drop_duplicates()
        .sort_values("bus")
        .reset_index(drop=True)
        .assign(in_service=True)
        .rename(columns={"voltagekv": "vn_kv", "bus": "bus_id"})
    )
    pf_buses["geodata"] = (
        pf_buses["bus_id"]
        .str.rsplit("_", n=1, expand=True)[0]
        .map(buses.to_crs(GEO_CRS).set_index("bus_id")["geometry"].to_dict())
    )
    return pf_buses


def _get_trafos_pp(pf_buses):
    gdf = (
        pf_buses["bus_id"]
        .str.rsplit("_", n=1, expand=True)
        .set_axis(["bus_id", "vn_kv"], axis=1)
        .groupby("bus_id")
        .agg(lambda s: sorted(s.str.replace("kv", "").astype(int)))
    )
    gdf = gdf[gdf["vn_kv"].map(len) > 1].reset_index()

    trafos = []
    for _, row in gdf.iterrows():
        bus = row["bus_id"]
        for lv, hv in zip(row["vn_kv"][:-1], row["vn_kv"][1:], strict=False):
            trafos.append(
                {
                    "name": "trafo" + f"_{bus}_{lv}kv_to_{bus}_{hv}kv",
                    "lv_bus": bus + f"_{lv}kv",
                    "hv_bus": bus + f"_{hv}kv",
                    "vn_lv_kv": lv,
                    "vn_hv_kv": hv,
                    # ARRBITRARY VALUES TODO find acceptable values
                    "sn_mva": 1_000,
                    "vk_percent": 12.2,
                    "vkr_percent": 0.25,
                    "pfe_kw": 60.0,
                    "i0_percent": 0.06,
                    "in_service": True,
                }
            )
    pf_trafos = pd.DataFrame(trafos)
    return pf_trafos


def _get_gens_pp(pf_buses: pd.DataFrame):
    gens = (
        read_major_powerstations()
        .query("state in @NEM_STATES")  # Filter to NEM states only
        .reset_index(drop=True)
        .to_crs(METRIC_CRS)[
            [
                "featuretype",
                "class",
                "name",
                "operationalstatus",
                "owner",
                "generationtype",
                "primaryfueltype",
                "generationmw",
                "ga_guid",
                "state",
                "x_coordinate",
                "y_coordinate",
                "geometry",
            ]
        ]
    )
    pf_gens = (
        gens.sjoin_nearest(
            gpd.GeoDataFrame(
                pf_buses[["bus_id", "vn_kv", "geodata"]],
                geometry="geodata",
                crs=GEO_CRS,
            ).to_crs(METRIC_CRS)
        )
        .assign(
            name=lambda s: s["name"],
            p_mw=lambda s: s["generationmw"] / 2,
            max_p_mw=lambda s: s["generationmw"],
            type=lambda s: s["generationtype"],
            fueltype=lambda s: s["primaryfueltype"],
            in_service=lambda s: s["operationalstatus"] == "Operational",
        )[
            [
                "bus_id",
                "name",
                "p_mw",
                "max_p_mw",
                "type",
                "in_service",
                "owner",
                "fueltype",
                "vn_kv",
                "geometry",
            ]
        ]
        .sort_values(["name", "vn_kv"])
        .drop_duplicates(["name", "max_p_mw"], keep="last")
        .dropna(subset=["max_p_mw"])
        .to_crs(GEO_CRS)
        .rename(columns={"geometry": "geodata"})
    )
    return pf_gens


def _get_loads_pp(pf_buses: pd.DataFrame):
    subs = (
        read_substations()
        .query("state in @NEM_STATES")  # Filter to NEM states only
        .reset_index(drop=True)
        .to_crs(METRIC_CRS)
    )
    subs["name"] = subs["name"].fillna("NA_ZSS_" + subs["locality"])
    subs = subs[
        [
            "class",
            "name",
            "operationalstatus",
            "state",
            "locality",
            "voltagekv",
            "geometry",
            "ga_guid",
        ]
    ]
    pf_subs = (
        subs.sjoin_nearest(
            gpd.GeoDataFrame(
                pf_buses[["bus_id", "vn_kv", "geodata"]],
                geometry="geodata",
                crs=GEO_CRS,
            ).to_crs(METRIC_CRS),
        )
        .query("voltagekv == vn_kv")
        .drop_duplicates(subset=["name", "class", "ga_guid"], keep="first")
        .dropna(subset="bus_id")
        .assign(
            name=lambda s: s["name"],
            type=lambda s: s["class"],
            in_service=lambda s: s["operationalstatus"] == "Operational",
        )[
            [
                "bus_id",
                "name",
                "type",
                "vn_kv",
                "in_service",
                "locality",
                "state",
                "geometry",
            ]
        ]
        .to_crs(GEO_CRS)
        .rename(columns={"geometry": "geodata"})
    )
    return pf_subs


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
        island_buses: GeoDataFrame of buses in island
        main_buses: GeoDataFrame of buses in main component
        same_voltage: If True, only connect buses at the same voltage level
        max_distance_km: Maximum distance threshold in km. Returns None if no pair within this distance.

    Returns:
        Tuple of (island_bus_id, main_bus_id, distance_km) or (None, None, inf)
    """
    min_distance_km = float("inf")
    best_island_bus = None
    best_main_bus = None

    for island_bus_id, island_bus in island_buses.iterrows():
        if pd.isna(island_bus["geodata"]):
            continue

        # If enforcing same voltage, filter main buses to matching voltage
        candidate_main_buses = main_buses
        if same_voltage and "vn_kv" in island_bus.index:
            island_voltage = island_bus["vn_kv"]
            candidate_main_buses = main_buses[main_buses["vn_kv"] == island_voltage]

        for main_bus_id, main_bus in candidate_main_buses.iterrows():
            if pd.isna(main_bus["geodata"]):
                continue

            # Quick Euclidean distance in degrees as a fast filter
            # (sufficient for finding closest pairs without expensive CRS conversions)
            lat1, lon1 = island_bus["geodata"].y, island_bus["geodata"].x
            lat2, lon2 = main_bus["geodata"].y, main_bus["geodata"].x
            deg_distance = ((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2) ** 0.5

            # Skip if obviously too far (rough approximation: 1 degree ≈ 111 km)
            if deg_distance * 111 > max_distance_km:
                continue

            if deg_distance < min_distance_km:
                min_distance_km = deg_distance
                best_island_bus = island_bus_id
                best_main_bus = main_bus_id

    # Return None if distance exceeds threshold (recheck with better precision)
    if best_island_bus and best_main_bus:
        actual_distance_km = _calculate_distance_km(
            island_buses.loc[best_island_bus, "geodata"],
            main_buses.loc[best_main_bus, "geodata"],
        )
        if actual_distance_km > max_distance_km:
            return None, None, float("inf")
        return best_island_bus, best_main_bus, actual_distance_km

    return None, None, float("inf")


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

    # Add edges from lines
    for _, row in model["lines"].iterrows():
        if pd.notna(row["from_bus"]) and pd.notna(row["to_bus"]):
            G.add_edge(row["from_bus"], row["to_bus"])

    # Add edges from transformers
    for _, row in model["trafos"].iterrows():
        if pd.notna(row["hv_bus"]) and pd.notna(row["lv_bus"]):
            G.add_edge(row["hv_bus"], row["lv_bus"])

    # Add isolated buses with generators/loads
    for bus_id in model["gens"]["bus_id"].unique():
        if bus_id not in G:
            G.add_node(bus_id)

    for bus_id in model["loads"]["bus_id"].unique():
        if bus_id not in G:
            G.add_node(bus_id)

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


def _get_gens_from_opennem(
    pf_buses: pd.DataFrame, matched_facilities: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Build a pandapower-style generator DataFrame from OpenNEM facilities.

    Args:
        pf_buses: Bus DataFrame produced by ``_get_bus_pp``.
        matched_facilities: GeoDataFrame from
            ``nemdb.near.match_facilities_to_gis``.

    Returns:
        DataFrame with columns compatible with pandapower gen creation.
    """
    gens = matched_facilities.to_crs(METRIC_CRS).copy()

    pf_gens = (
        gens.sjoin_nearest(
            gpd.GeoDataFrame(
                pf_buses[["bus_id", "vn_kv", "geodata"]],
                geometry="geodata",
                crs=GEO_CRS,
            ).to_crs(METRIC_CRS),
        )
        .assign(
            p_mw=lambda s: s["capacity_registered_mw"] / 2,
            max_p_mw=lambda s: s["capacity_registered_mw"],
            type=lambda s: s["fueltech_id"],
            in_service=lambda s: s["status_id"] == "operating",
        )[
            [
                "bus_id",
                "name",
                "code",
                "p_mw",
                "max_p_mw",
                "type",
                "in_service",
                "vn_kv",
                "gis_name",
                "match_type",
                "distance_m",
                "geometry",
            ]
        ]
        .sort_values(["name", "vn_kv"])
        .drop_duplicates(["code"], keep="last")
        .dropna(subset=["max_p_mw"])
        .to_crs(GEO_CRS)
        .rename(columns={"geometry": "geodata"})
    )
    return pd.DataFrame(pf_gens)


def get_pandapower_model_with_opennem(
    matched_facilities: gpd.GeoDataFrame | None = None,
    source: Literal["pooch", "api"] = "pooch",
) -> dict:
    """Build a pandapower model dict using OpenNEM facilities as generators.

    Reuses the existing bus/line/trafo/load pipeline but replaces
    ``_get_gens_pp`` with ``_get_gens_from_opennem``.

    Args:
        matched_facilities: Pre-computed matched facilities GeoDataFrame.
            When *None*, facilities are fetched according to ``source`` and
            matched via :func:`~nemdb.geodata.matching.match_facilities_to_gis`.
        source: How to obtain facilities when ``matched_facilities`` is *None*.
            ``"pooch"`` (default) downloads the pre-built parquet from the
            GitHub release — no account required.  ``"api"`` calls the
            OpenElectricity API (requires an API key).

    Returns:
        dict with keys ``buses``, ``lines``, ``trafos``, ``gens``, ``loads``.
    """
    if matched_facilities is None:
        matched_facilities = match_facilities_to_gis(source=source)

    lines, buses, mapping = _get_buses_and_lines()

    pf_lines = _get_lines_pp(lines, mapping)
    pf_buses = _get_bus_pp(pf_lines, buses)
    pf_trafos = _get_trafos_pp(pf_buses)
    pf_gens = _get_gens_from_opennem(pf_buses, matched_facilities)
    pf_loads = _get_loads_pp(pf_buses)

    # CRITICAL: Ensure buses with generators have geodata
    # Copy generator geodata to bus if bus is missing geodata
    # This ensures islands with generators can be connected via _validate_and_fix_connectivity
    for _, gen in pf_gens.iterrows():
        bus_id = gen["bus_id"]
        bus_idx = pf_buses[pf_buses["bus_id"] == bus_id].index
        if len(bus_idx) > 0:
            bus_idx = bus_idx[0]
            if pd.isna(pf_buses.at[bus_idx, "geodata"]) and pd.notna(gen["geodata"]):
                pf_buses.at[bus_idx, "geodata"] = gen["geodata"]
                log.debug(f"Copied geodata from generator {gen['name']} to bus {bus_id}")

    model = {
        "buses": pf_buses,
        "lines": pf_lines,
        "trafos": pf_trafos,
        "gens": pf_gens,
        "loads": pf_loads,
    }

    # Validate and fix connectivity
    model, _diagnostics = _validate_and_fix_connectivity(model)

    return model


# Approximate line parameters per voltage class (GA data lacks impedance).
_LINE_PARAMS = {
    500: {
        "r_ohm_per_km": 0.02,
        "x_ohm_per_km": 0.28,
        "c_nf_per_km": 12.0,
        "max_i_ka": 3.0,
    },
    330: {
        "r_ohm_per_km": 0.03,
        "x_ohm_per_km": 0.32,
        "c_nf_per_km": 11.0,
        "max_i_ka": 2.0,
    },
    275: {
        "r_ohm_per_km": 0.04,
        "x_ohm_per_km": 0.33,
        "c_nf_per_km": 11.0,
        "max_i_ka": 1.5,
    },
    220: {
        "r_ohm_per_km": 0.06,
        "x_ohm_per_km": 0.37,
        "c_nf_per_km": 10.0,
        "max_i_ka": 1.0,
    },
    132: {
        "r_ohm_per_km": 0.10,
        "x_ohm_per_km": 0.40,
        "c_nf_per_km": 9.0,
        "max_i_ka": 0.6,
    },
    110: {
        "r_ohm_per_km": 0.12,
        "x_ohm_per_km": 0.42,
        "c_nf_per_km": 9.0,
        "max_i_ka": 0.5,
    },
    66: {
        "r_ohm_per_km": 0.18,
        "x_ohm_per_km": 0.44,
        "c_nf_per_km": 8.0,
        "max_i_ka": 0.4,
    },
}
_DEFAULT_LINE_PARAMS = {
    "r_ohm_per_km": 0.10,
    "x_ohm_per_km": 0.40,
    "c_nf_per_km": 9.0,
    "max_i_ka": 0.6,
}


def create_pandapower_network(
    use_opennem: bool = False,
    model: dict | None = None,
    source: Literal["pooch", "api"] = "pooch",
) -> Any:
    """Convert a model dict into a pandapower Network object.

    Args:
        use_opennem: If True, uses ``get_pandapower_model_with_opennem()``
            instead of ``get_pandapower_model()``.
        model: Pre-computed model dict. If None, one is built from scratch.
        source: Facilities data source when ``use_opennem`` is True and
            ``model`` is None.  ``"pooch"`` (default) fetches the pre-built
            parquet; ``"api"`` calls the OpenElectricity API.

    Returns:
        A ``pandapower.auxiliary.pandapowerNet`` network object.
    """
    if model is None:
        model = (
            get_pandapower_model_with_opennem(source=source)
            if use_opennem
            else get_pandapower_model()
        )

    net = pp.create_empty_network(name="NEM")
    bus_idx_map = _add_buses_to_network(net, model["buses"])
    _add_lines_to_network(net, bus_idx_map, model["lines"])
    _add_transformers_to_network(net, bus_idx_map, model["trafos"])
    _add_generators_to_network(net, bus_idx_map, model["gens"])
    _add_loads_to_network(net, bus_idx_map, model["loads"])
    _add_external_grids(net)
    sanity_checks(net)

    return net


def _add_buses_to_network(net: pp.auxiliary.pandapowerNet, buses_df: pd.DataFrame) -> dict:
    """Add buses from dataframe to network.

    Returns:
        Mapping of bus IDs to pandapower indices.
    """
    bus_idx_map = {}
    for _, row in buses_df.iterrows():
        idx = pp.create_bus(
            net,
            vn_kv=row["vn_kv"],
            name=row["bus_id"],
            in_service=row["in_service"],
            geodata=(row["geodata"].y, row["geodata"].x) if row["geodata"] else None,
        )
        bus_idx_map[row["bus_id"]] = idx
    return bus_idx_map


def _add_lines_to_network(
    net: pp.auxiliary.pandapowerNet, bus_idx_map: dict, lines_df: pd.DataFrame
) -> None:
    """Add lines from dataframe to network."""
    for _, row in lines_df.iterrows():
        from_idx = bus_idx_map.get(row["from_bus"])
        to_idx = bus_idx_map.get(row["to_bus"])
        if from_idx is None or to_idx is None:
            continue
        vkv = int(row["voltagekv"]) if pd.notna(row["voltagekv"]) else 0
        params = _LINE_PARAMS.get(vkv, _DEFAULT_LINE_PARAMS)
        pp.create_line_from_parameters(
            net,
            from_bus=from_idx,
            to_bus=to_idx,
            length_km=row["length_km"],
            name=row["name"],
            in_service=row["in_service"],
            **params,
        )


def _add_transformers_to_network(
    net: pp.auxiliary.pandapowerNet, bus_idx_map: dict, trafos_df: pd.DataFrame
) -> None:
    """Add transformers from dataframe to network."""
    for _, row in trafos_df.iterrows():
        hv_idx = bus_idx_map.get(row["hv_bus"])
        lv_idx = bus_idx_map.get(row["lv_bus"])
        if hv_idx is None or lv_idx is None:
            continue
        pp.create_transformer_from_parameters(
            net,
            hv_bus=hv_idx,
            lv_bus=lv_idx,
            sn_mva=row["sn_mva"],
            vn_hv_kv=row["vn_hv_kv"],
            vn_lv_kv=row["vn_lv_kv"],
            vkr_percent=row["vkr_percent"],
            vk_percent=row["vk_percent"],
            pfe_kw=row["pfe_kw"],
            i0_percent=row["i0_percent"],
            name=row["name"],
            in_service=row["in_service"],
        )


def _add_generators_to_network(
    net: pp.auxiliary.pandapowerNet, bus_idx_map: dict, gens_df: pd.DataFrame
) -> None:
    """Add generators from dataframe to network."""
    for _, row in gens_df.iterrows():
        bus_idx = bus_idx_map.get(row["bus_id"])
        if bus_idx is None:
            continue
        pp.create_gen(
            net,
            bus=bus_idx,
            p_mw=row["p_mw"],
            max_p_mw=row["max_p_mw"],
            name=row.get(
                "code", row["name"]
            ),  # Use 'code' if available (OpenNEM), else 'name' (GA data)
            type=row["type"],
            in_service=row["in_service"],
        )


def _add_loads_to_network(
    net: pp.auxiliary.pandapowerNet, bus_idx_map: dict, loads_df: pd.DataFrame
) -> None:
    """Add loads (substations as placeholders) from dataframe to network."""
    for _, row in loads_df.iterrows():
        bus_idx = bus_idx_map.get(row["bus_id"])
        if bus_idx is None:
            continue
        pp.create_load(
            net,
            bus=bus_idx,
            p_mw=0,
            name=row["name"],
            in_service=row["in_service"],
        )


def _add_external_grids(net: pp.auxiliary.pandapowerNet) -> pp.auxiliary.pandapowerNet:
    """Add external grid (slack bus) connections to major NEM substations.

    Creates ext_grid elements at key interconnection points:
    - Torrens Island A (275 kV, South Australia)
    - Thomastown (220 kV, Victoria)
    - George Town (220 kV, Tasmania)
    - Sydney West (330 kV, New South Wales)
    - South Pine (275 kV, Queensland)

    Args:
        net: A pandapower Network object (post-creation).

    Returns:
        The modified network with external grids added.
    """
    # Define target substations and their expected voltages
    # These are matched against load names in the network
    ext_grid_specs = [
        ("Torrens Island A", 275),
        ("Thomastown", 220),
        ("George Town", 220),
        ("Sydney West", 330),
        ("South Pine", 275),
    ]

    added_count = 0
    for sub_name, _voltage_kv in ext_grid_specs:
        # Find the load entry for this substation
        matching_loads = net.load[(net.load["name"].str.contains(sub_name, case=False, na=False))]

        if len(matching_loads) > 0:
            # Get the bus_id from the load
            load_entry = matching_loads.iloc[0]
            target_bus_id = load_entry["bus"]

            # Find the bus with that index
            if target_bus_id in net.bus.index:
                pp.create_ext_grid(
                    net,
                    bus=target_bus_id,
                    vm_pu=1.0,
                    va_degree=0.0,
                    in_service=True,
                    name=f"ext_grid_{sub_name}",
                )
                added_count += 1
                bus_vn = net.bus.loc[target_bus_id, "vn_kv"]
                log.debug(f"✓ Added ext_grid at {sub_name} ({bus_vn} kV) - bus {target_bus_id}")
            else:
                log.debug(
                    f"✗ Bus {target_bus_id} not found for {sub_name} "
                    f"(may be in disconnected island)"
                )
        else:
            log.debug(f"✗ Could not find load entry for {sub_name}")

    if added_count == 0:
        log.debug(
            "WARNING: No external grids were added. Check that substations exist in the network."
        )
    else:
        log.debug(f"\n✓ Successfully added {added_count} external grid(s)")

    return net


def _log_check_result(check_name: str, errors: list | str | dict, issues_found_ref: list) -> None:
    """Log the result of a single sanity check.

    Args:
        check_name: Name of the check
        errors: List of errors, error message string, or dict from pandapower diagnostics
        issues_found_ref: List to track if any issues found (mutable reference)
    """
    if isinstance(errors, str):
        # Error running the check
        issues_found_ref[0] = True
        log.error(f"  ✗ {check_name}: {errors}")
    elif isinstance(errors, dict):
        # Dict-type diagnostic results from pandapower
        if errors and len(errors) > 0:
            issues_found_ref[0] = True
            log.warning(f"  ✗ {check_name}: issues found")
            for key, value in list(errors.items())[:3]:  # Log first 3 entries
                log.debug(f"    - {key}: {value}")
            if len(errors) > 3:
                log.debug(f"    ... and {len(errors) - 3} more")
        else:
            log.debug(f"  ✓ {check_name}: OK")
    elif errors and len(errors) > 0:
        # List-type diagnostic results
        issues_found_ref[0] = True
        log.warning(f"  ✗ {check_name}: {len(errors)} issue(s) found")
        for error in errors[:5]:  # Log first 5
            log.debug(f"    - {error}")
        if len(errors) > 5:
            log.debug(f"    ... and {len(errors) - 5} more")
    else:
        # No issues
        log.debug(f"  ✓ {check_name}: OK")


def sanity_checks(net: pp.auxiliary.pandapowerNet) -> dict:
    """Run pandapower diagnostic checks on the network.

    Executes all available diagnostic functions to validate the network
    structure and catch common modeling errors.

    Note: Disconnected spatial fragments are filtered during bus/line extraction
    (_get_buses_and_lines). Any remaining disconnected elements indicate
    voltage-level isolation issues after transformer assignment.

    Args:
        net: A pandapower Network object to validate.

    Returns:
        dict with keys for each diagnostic check and their results.
            Results are lists of issues found, empty if no issues detected.
    """
    results = {}
    issues_found = [False]  # Use list for mutable reference in helper function

    log.info("Starting network sanity checks...")
    log.info(
        f"Network size: {len(net.bus)} buses, {len(net.line)} lines, "
        f"{len(net.trafo)} transformers, {len(net.gen)} generators, {len(net.load)} loads"
    )

    # Checks that don't require parameters
    simple_checks = {
        "invalid_values": InvalidValues(),
        "missing_bus_indices": MissingBusIndices(),
        "multiple_voltage_controlling_elements_per_bus": MultipleVoltageControllingElementsPerBus(),
        "no_ext_grid": NoExtGrid(),
        "deviation_from_std_type": DeviationFromStdType(),
    }

    for name, checker in simple_checks.items():
        try:
            errors = checker.diagnostic(net)
            results[name] = errors
            _log_check_result(name, errors, issues_found)
        except Exception as e:
            error_msg = f"Error running diagnostic: {e!s}"
            results[name] = error_msg
            _log_check_result(name, error_msg, issues_found)

    # Special handling for disconnected elements:
    # Spatial fragments are now filtered during bus/line extraction (_cleanup_disconnected_fragments).
    # Any remaining disconnections indicate voltage-level isolation after transformer assignment.
    try:
        disconnected = DisconnectedElements().diagnostic(net) or []
        results["disconnected_elements"] = disconnected
        if isinstance(disconnected, dict) and len(disconnected) > 0:
            issues_found[0] = True
            element_count = sum(len(v) if isinstance(v, list) else 0 for v in disconnected.values())
            log.warning(f"  ✗ disconnected_elements: {element_count} disconnected element(s) found")
            log.debug(f"    Disconnected: {disconnected}")
            results["note_disconnected"] = (
                f"Found {element_count} disconnected element(s). "
                "Spatial fragments were filtered during bus extraction. "
                "Remaining disconnections indicate voltage-level isolation after "
                "transformer assignment (may need manual review)."
            )
        else:
            log.debug("  ✓ disconnected_elements: OK")
    except Exception as e:
        error_msg = f"Error running diagnostic: {e!s}"
        results["disconnected_elements"] = error_msg
        _log_check_result("disconnected_elements", error_msg, issues_found)

    # Checks that require parameters - use sensible defaults
    try:
        errors = ImplausibleImpedanceValues().diagnostic(
            net, min_r_ohm=0.0, min_x_ohm=0.0, max_r_ohm=100.0, max_x_ohm=100.0
        )
        results["implausible_impedance_values"] = errors
        _log_check_result("implausible_impedance_values", errors, issues_found)
    except Exception as e:
        error_msg = f"Error running diagnostic: {e!s}"
        results["implausible_impedance_values"] = error_msg
        _log_check_result("implausible_impedance_values", error_msg, issues_found)

    try:
        errors = NominalVoltagesMismatch().diagnostic(net, nom_voltage_tolerance=0.05)
        results["nominal_voltages_mismatch"] = errors
        _log_check_result("nominal_voltages_mismatch", errors, issues_found)
    except Exception as e:
        error_msg = f"Error running diagnostic: {e!s}"
        results["nominal_voltages_mismatch"] = error_msg
        _log_check_result("nominal_voltages_mismatch", error_msg, issues_found)

    # Summary
    if issues_found[0]:
        log.warning(
            "Network sanity checks completed with issues found. Review logs above for details."
        )
    else:
        log.info("Network sanity checks completed successfully - no issues detected!")

    return results
