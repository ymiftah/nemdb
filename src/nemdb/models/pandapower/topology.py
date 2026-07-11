from typing import Any

import geopandas as gpd
import networkx as nx
import pandas as pd
import shapely as shp
from shapely.ops import substring

from nemdb.logger import log

METRIC_CRS = "EPSG:7856"
GEO_CRS = "EPSG:4326"


def _bus_pair_from_mapping(line, mapping) -> tuple[str | None, str | None]:
    """Look up the two bus IDs for a GEO_CRS line using the endpoint-to-bus mapping."""
    from_bus = mapping.get(shp.get_point(line.geometry, 0))
    to_bus = mapping.get(shp.get_point(line.geometry, -1))
    if isinstance(from_bus, pd.Series):
        from_bus = from_bus.iloc[0] if not from_bus.empty else None
    if isinstance(to_bus, pd.Series):
        to_bus = to_bus.iloc[0] if not to_bus.empty else None
    return from_bus, to_bus


def _add_line_edges(G, lines, mapping, diagnostics):
    """Populate G with one edge per line and record self-loops in diagnostics."""
    # Fast path: _from_bus/_to_bus columns are left by _snap_t_junctions and are the
    # authoritative post-snap assignments, avoiding CRS floating-point mismatches.
    # Standard path: batch CRS conversion consistent with how mapping keys were built.
    if "_from_bus" in lines.columns and "_to_bus" in lines.columns:
        for idx, line in lines.iterrows():
            from_bus = line["_from_bus"]
            to_bus = line["_to_bus"]
            if pd.isna(from_bus) or pd.isna(to_bus):
                continue
            if from_bus == to_bus:
                diagnostics["self_loops"].append(idx)
            else:
                G.add_edge(from_bus, to_bus)
    else:
        for idx, line in lines.to_crs(GEO_CRS).iterrows():
            from_bus, to_bus = _bus_pair_from_mapping(line, mapping)
            if from_bus is None or to_bus is None:
                log.warning(f"Line {idx} has unmapped endpoint(s): {from_bus} -> {to_bus}")
                continue
            if from_bus == to_bus:
                diagnostics["self_loops"].append(idx)
            else:
                G.add_edge(from_bus, to_bus)


def _diagnose_graph(lines, buses, mapping):
    """Diagnose graph structure for connectivity issues.

    Returns dict with keys: orphan_buses, islands, self_loops, total_buses, total_lines.
    """
    diagnostics: dict = {
        "orphan_buses": [],
        "islands": [],
        "self_loops": [],
        "total_buses": len(buses),
        "total_lines": len(lines),
    }

    G = nx.Graph()
    for bus_id in buses["bus_id"]:
        G.add_node(bus_id)

    _add_line_edges(G, lines, mapping, diagnostics)

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
            f"Remaining islands after T-junction snapping will be connected at the model level."
        )

    return lines, buses, mapping, correction_stats


def _apply_t_snap(
    lines: gpd.GeoDataFrame,
    buses: gpd.GeoDataFrame,
    mapping: pd.Series,
    bus_id: str,
    line_iloc: int,
    t: float,
    dist: float,
    new_bus_id: str,
    bus_geom_lookup: dict[str, Any],
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.Series] | None:
    """Split a line at parameter t and remap bus_id to the new split bus.

    Returns None if the resulting segments would be degenerate (< 1 m).
    """
    old_row = lines.iloc[line_iloc]
    old_index = lines.index[line_iloc]
    old_geom = old_row.geometry

    geom1 = substring(old_geom, 0, t)
    geom2 = substring(old_geom, t, old_geom.length)
    if geom1.length < 1.0 or geom2.length < 1.0:
        return None

    split_pt = old_geom.interpolate(t)
    split_pt_geo = (
        gpd.GeoDataFrame([{"geometry": split_pt}], crs=METRIC_CRS).to_crs(GEO_CRS).geometry.iloc[0]
    )

    # If the split point already has a bus (e.g. a line passes through an existing substation),
    # reuse it — adding a second mapping entry for the same Point would cause ambiguous lookups.
    existing_at_split = mapping.get(split_pt_geo)
    if isinstance(existing_at_split, pd.Series):
        existing_at_split = existing_at_split.iloc[0] if not existing_at_split.empty else None

    if existing_at_split is not None:
        actual_new_bus_id = existing_at_split
        bus_geom_lookup.setdefault(actual_new_bus_id, split_pt)
    else:
        actual_new_bus_id = new_bus_id
        buses = gpd.GeoDataFrame(
            pd.concat(
                [
                    buses,
                    gpd.GeoDataFrame(
                        [{"bus_id": actual_new_bus_id, "geometry": split_pt}], crs=METRIC_CRS
                    ),
                ],
                ignore_index=True,
            ),
            geometry="geometry",
            crs=METRIC_CRS,
        )
        bus_geom_lookup[actual_new_bus_id] = split_pt
        mapping = pd.concat([mapping, pd.Series([actual_new_bus_id], index=[split_pt_geo])])

    mapping = mapping.where(mapping != bus_id, actual_new_bus_id)

    old_attrs = old_row.to_dict()
    new_rows = [
        {
            **old_attrs,
            "geometry": geom1,
            "length_km": geom1.length / 1000,
            "start_point": shp.get_point(geom1, 0),
            "end_point": shp.get_point(geom1, -1),
            "_from_bus": old_row["_from_bus"],
            "_to_bus": actual_new_bus_id,
        },
        {
            **old_attrs,
            "geometry": geom2,
            "length_km": geom2.length / 1000,
            "start_point": shp.get_point(geom2, 0),
            "end_point": shp.get_point(geom2, -1),
            "_from_bus": actual_new_bus_id,
            "_to_bus": old_row["_to_bus"],
        },
    ]

    lines = lines.drop(index=old_index)
    lines = gpd.GeoDataFrame(
        pd.concat([lines, gpd.GeoDataFrame(new_rows, crs=METRIC_CRS)]).reset_index(drop=True),
        geometry="geometry",
        crs=METRIC_CRS,
    )

    # Propagate the bus remap to _from_bus/_to_bus on all surviving lines so that
    # _diagnose_graph can use these columns directly without CRS re-conversion.
    if actual_new_bus_id != bus_id:
        for col in ("_from_bus", "_to_bus"):
            if col in lines.columns:
                lines[col] = lines[col].where(lines[col] != bus_id, actual_new_bus_id)

    log.info(
        f"T-junction snap: {bus_id} → {actual_new_bus_id} on '{old_row.get('name', str(old_index))}' "
        f"(dist={dist:.0f} m, t={t:.0f}/{old_geom.length:.0f} m)"
    )
    return lines, buses, mapping


def _snap_t_junctions(
    lines: gpd.GeoDataFrame,
    buses: gpd.GeoDataFrame,
    mapping: pd.Series,
    island_sets: list[set[str]],
    max_snap_distance_m: float = 2_000.0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.Series, int]:
    """Snap island bus endpoints to the interior of nearby non-island lines.

    When a line's endpoint lies close to the interior (not the endpoint) of
    another line, DBSCAN misses the junction and the line becomes an isolated
    island. This function detects such T-junctions, splits the target line at
    the nearest interior point, creates a new bus there, and remaps the island
    bus endpoint to that new bus — physically connecting the island.

    Transformer creation is handled automatically downstream by _get_trafos_pp
    whenever the new bus appears in lines of differing capacitykv.

    Args:
        lines: GeoDataFrame in METRIC_CRS with geometry, start_point, end_point,
            length_km columns.
        buses: GeoDataFrame in METRIC_CRS with bus_id and geometry columns.
        mapping: Series mapping GEO_CRS Points to bus_id strings.
        island_sets: Connected components sorted largest-first.
        max_snap_distance_m: Maximum distance in metres to attempt a snap.

    Returns:
        Tuple of (updated_lines, updated_buses, updated_mapping, snaps_performed).
    """
    if len(island_sets) <= 1:
        return lines, buses, mapping, 0

    # Pre-compute line bus membership with a single CRS conversion
    lines_geo = lines.to_crs(GEO_CRS)
    lines = lines.copy()
    lines["_from_bus"] = [
        mapping.get(shp.get_point(r.geometry, 0)) for _, r in lines_geo.iterrows()
    ]
    lines["_to_bus"] = [mapping.get(shp.get_point(r.geometry, -1)) for _, r in lines_geo.iterrows()]

    bus_geom_lookup: dict[str, Any] = buses.set_index("bus_id")["geometry"].to_dict()

    existing_nums = [
        int(b.split("_")[1])
        for b in buses["bus_id"]
        if b.startswith("bus_") and len(b.split("_")) > 1 and b.split("_")[1].isdigit()
    ]
    next_bus_num = max(existing_nums, default=0) + 1

    snaps = 0

    for island_idx in range(1, len(island_sets)):
        island = island_sets[island_idx]

        for bus_id in list(island):
            bus_geom = bus_geom_lookup.get(bus_id)
            if bus_geom is None:
                continue

            # Candidate lines within snap distance via spatial index
            buffer = bus_geom.buffer(max_snap_distance_m)
            candidate_idxs = lines.sindex.query(buffer, predicate="intersects")

            best_dist = max_snap_distance_m
            best_line_iloc: int | None = None
            best_t: float | None = None

            for iloc in candidate_idxs:
                row = lines.iloc[iloc]
                if row["_from_bus"] in island or row["_to_bus"] in island:
                    continue  # skip lines belonging to this island

                line_geom = row.geometry
                t = line_geom.project(bus_geom)
                # Skip if t is at or very near an endpoint (DBSCAN handles those)
                if t <= 1.0 or t >= line_geom.length - 1.0:
                    continue

                dist = bus_geom.distance(line_geom.interpolate(t))
                if dist < best_dist:
                    best_dist = dist
                    best_line_iloc = iloc
                    best_t = t

            if best_line_iloc is None or best_t is None:
                continue

            new_bus_id = f"bus_{next_bus_num}"
            result = _apply_t_snap(
                lines,
                buses,
                mapping,
                bus_id,
                best_line_iloc,
                best_t,
                best_dist,
                new_bus_id,
                bus_geom_lookup,
            )
            if result is None:
                continue  # degenerate split, try next bus

            lines, buses, mapping = result
            next_bus_num += 1
            snaps += 1
            break  # one snap connects the whole island; re-diagnose after all islands

    # Columns are kept so the caller's re-diagnosis can use them directly (more
    # accurate than CRS-based lookups after remapping).  Caller drops them.
    return lines, buses, mapping, snaps


def _join_isolated_islands_to_nearest_bus(
    lines: gpd.GeoDataFrame,
    buses: gpd.GeoDataFrame,
    mapping: pd.Series,
    island_sets: list[set[str]],
    max_join_distance_m: float = 10_000.0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.Series, int]:
    """Merge small islands into the nearest bus of the main network.

    Some islands are caused by lines with broken/inaccurate geometry whose
    endpoints sit far from any other line interior, so _snap_t_junctions can't
    find a candidate to split. Rather than leave them disconnected, find the
    closest bus pair (one in the island, one outside it) within
    max_join_distance_m and merge the island bus into the external bus —
    physically connecting the island without altering any line geometry.

    Requires lines to have _from_bus/_to_bus columns (set up by
    _snap_t_junctions); these are remapped in place alongside mapping.

    Returns:
        Tuple of (updated_lines, updated_buses, updated_mapping, joins_performed).
    """
    if len(island_sets) <= 1:
        return lines, buses, mapping, 0

    bus_geom_lookup: dict[str, Any] = buses.set_index("bus_id")["geometry"].to_dict()
    joins = 0

    for island_idx in range(1, len(island_sets)):
        island = island_sets[island_idx]

        best_dist = max_join_distance_m
        best_island_bus: str | None = None
        best_target_bus: str | None = None

        for bus_id in island:
            bus_geom = bus_geom_lookup.get(bus_id)
            if bus_geom is None:
                continue

            buffer = bus_geom.buffer(max_join_distance_m)
            candidate_idxs = buses.sindex.query(buffer, predicate="intersects")
            for iloc in candidate_idxs:
                candidate = buses.iloc[iloc]
                if candidate["bus_id"] in island:
                    continue
                dist = bus_geom.distance(candidate.geometry)
                if dist < best_dist:
                    best_dist = dist
                    best_island_bus = bus_id
                    best_target_bus = candidate["bus_id"]

        if best_island_bus is None or best_target_bus is None:
            continue

        mapping = mapping.where(mapping != best_island_bus, best_target_bus)
        for col in ("_from_bus", "_to_bus"):
            if col in lines.columns:
                lines[col] = lines[col].where(lines[col] != best_island_bus, best_target_bus)

        buses = buses[buses["bus_id"] != best_island_bus].reset_index(drop=True)
        bus_geom_lookup.pop(best_island_bus, None)

        log.info(
            f"Joined isolated island bus {best_island_bus} → {best_target_bus} "
            f"(dist={best_dist:.0f} m)"
        )
        joins += 1

    return lines, buses, mapping, joins


def _validate_and_correct_graph(lines, buses, mapping):
    """Validate and correct graph structure.

    This ensures:
    1. No orphan buses (buses with no line connections)
    2. No self-loops (lines with same start and end bus)
    3. Snaps T-junction islands to line interiors where possible
    4. Joins remaining small islands (e.g. broken-geometry orphan lines) to
       the nearest bus in the main network, within a 10 km radius
    5. Reports remaining islands for correction at model level

    Args:
        lines: GeoDataFrame with transmission lines
        buses: GeoDataFrame with buses
        mapping: Series mapping geometry points to bus_id

    Returns:
        Tuple of (corrected_lines, corrected_buses, corrected_mapping, diagnostics)
    """
    # Run diagnostics
    diagnostics = _diagnose_graph(lines, buses, mapping)

    # Apply basic corrections (self-loops, orphan buses)
    lines, buses, mapping, correction_stats = _correct_graph(lines, buses, mapping, diagnostics)

    if correction_stats["islands"] > 1:
        islands_sorted = sorted(diagnostics["islands"], key=len, reverse=True)

        # Snap T-junction islands: endpoints near line interiors
        lines, buses, mapping, snaps = _snap_t_junctions(lines, buses, mapping, islands_sorted)
        if snaps > 0:
            log.info(f"Snapped {snaps} T-junction island(s) to line interiors")
            re_diag = _diagnose_graph(lines, buses, mapping)
            correction_stats["islands"] = len(re_diag["islands"])
            islands_sorted = sorted(re_diag["islands"], key=len, reverse=True)

        # Join any remaining small islands to the nearest bus in the main network
        if correction_stats["islands"] > 1:
            lines, buses, mapping, joins = _join_isolated_islands_to_nearest_bus(
                lines, buses, mapping, islands_sorted
            )
            if joins > 0:
                log.info(f"Joined {joins} isolated island(s) to nearest bus")
                re_diag = _diagnose_graph(lines, buses, mapping)
                correction_stats["islands"] = len(re_diag["islands"])

        lines = lines.drop(columns=["_from_bus", "_to_bus"], errors="ignore")

    # Combine diagnostics and correction stats for return
    final_diagnostics = {
        "total_buses": len(buses),
        "total_lines": len(lines),
        "orphan_buses": correction_stats["orphan_buses"],
        "self_loops": correction_stats["self_loops"],
        "islands": correction_stats["islands"],
    }

    return lines, buses, mapping, final_diagnostics
