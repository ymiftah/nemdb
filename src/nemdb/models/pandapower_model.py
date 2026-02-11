import geopandas as gpd
import pandas as pd
import shapely as shp

from sklearn.cluster import DBSCAN

from nemdb.geodata.geodata import (
    read_substations,
    read_transmission_lines,
    read_major_powerstations,
)

import pandapower as pp


METRIC_CRS = "EPSG:7856"
GEO_CRS = "EPSG:4326"


def get_pandapower_model():
    lines, buses, mapping = _get_buses_and_lines()

    pf_lines = _get_lines_pp(lines, mapping)
    pf_buses = _get_bus_pp(pf_lines, buses)
    pf_trafos = _get_trafos_pp(pf_buses)
    pf_gens = _get_gens_pp(pf_buses)
    pf_loads = _get_loads_pp(pf_buses)
    return dict(
        buses=pf_buses,
        lines=pf_lines,
        trafos=pf_trafos,
        gens=pf_gens,
        loads=pf_loads,
    )


def _get_buses_and_lines():
    # Extract line segments
    lines = (
        read_transmission_lines(clean=True)
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
        .agg(lambda s: sorted(list(s.str.replace("kv", "").astype(int))))
    )
    gdf = gdf[gdf["vn_kv"].map(len) > 1].reset_index()

    trafos = []
    for _, row in gdf.iterrows():
        bus = row["bus_id"]
        for lv, hv in zip(row["vn_kv"][:-1], row["vn_kv"][1:]):
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
    subs = read_substations().reset_index(drop=True).to_crs(METRIC_CRS)
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
    return pf_gens


def get_pandapower_model_with_opennem(
    matched_facilities: gpd.GeoDataFrame | None = None,
) -> dict:
    """Build a pandapower model dict using OpenNEM facilities as generators.

    Reuses the existing bus/line/trafo/load pipeline but replaces
    ``_get_gens_pp`` with ``_get_gens_from_opennem``.

    Args:
        matched_facilities: Pre-computed matched facilities GeoDataFrame.
            If None, calls ``nemdb.near.match_facilities_to_gis()``.

    Returns:
        dict with keys ``buses``, ``lines``, ``trafos``, ``gens``, ``loads``.
    """
    if matched_facilities is None:
        from nemdb.geodata.matching import match_facilities_to_gis

        matched_facilities = match_facilities_to_gis()

    lines, buses, mapping = _get_buses_and_lines()

    pf_lines = _get_lines_pp(lines, mapping)
    pf_buses = _get_bus_pp(pf_lines, buses)
    pf_trafos = _get_trafos_pp(pf_buses)
    pf_gens = _get_gens_from_opennem(pf_buses, matched_facilities)
    pf_loads = _get_loads_pp(pf_buses)
    return dict(
        buses=pf_buses,
        lines=pf_lines,
        trafos=pf_trafos,
        gens=pf_gens,
        loads=pf_loads,
    )


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


def create_pandapower_network(use_opennem: bool = False, model: dict | None = None):
    """Convert a model dict into a pandapower Network object.

    Args:
        use_opennem: If True, uses ``get_pandapower_model_with_opennem()``
            instead of ``get_pandapower_model()``.
        model: Pre-computed model dict. If None, one is built from scratch.

    Returns:
        A ``pandapower.auxiliary.pandapowerNet`` network object.
    """
    if model is None:
        model = (
            get_pandapower_model_with_opennem()
            if use_opennem
            else get_pandapower_model()
        )

    net = pp.create_empty_network(name="NEM")

    # Buses
    bus_idx_map = {}
    for _, row in model["buses"].iterrows():
        idx = pp.create_bus(
            net,
            vn_kv=row["vn_kv"],
            name=row["bus_id"],
            in_service=row["in_service"],
            geodata=(row["geodata"].y, row["geodata"].x) if row["geodata"] else None,
        )
        bus_idx_map[row["bus_id"]] = idx

    # Lines
    for _, row in model["lines"].iterrows():
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

    # Transformers
    for _, row in model["trafos"].iterrows():
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

    # Generators
    for _, row in model["gens"].iterrows():
        bus_idx = bus_idx_map.get(row["bus_id"])
        if bus_idx is None:
            continue
        pp.create_gen(
            net,
            bus=bus_idx,
            p_mw=row["p_mw"],
            max_p_mw=row["max_p_mw"],
            name=row.get("code", row["name"]),  # Use 'code' if available (OpenNEM), else 'name' (GA data)
            type=row["type"],
            in_service=row["in_service"],
        )

    # Loads (substations as placeholders)
    for _, row in model["loads"].iterrows():
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

    # Add external grids at major substations
    net = add_external_grids(net)

    return net


def add_external_grids(net: pp.auxiliary.pandapowerNet) -> pp.auxiliary.pandapowerNet:
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
    for sub_name, voltage_kv in ext_grid_specs:
        # Find the load entry for this substation
        matching_loads = net.load[
            (net.load["name"].str.contains(sub_name, case=False, na=False))
        ]

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
                print(
                    f"✓ Added ext_grid at {sub_name} ({bus_vn} kV) - bus {target_bus_id}"
                )
            else:
                print(
                    f"✗ Bus {target_bus_id} not found for {sub_name} "
                    f"(may be in disconnected island)"
                )
        else:
            print(f"✗ Could not find load entry for {sub_name}")

    if added_count == 0:
        print(
            "WARNING: No external grids were added. "
            "Check that substations exist in the network."
        )
    else:
        print(f"\n✓ Successfully added {added_count} external grid(s)")

    return net


def sanity_checks(net: pp.auxiliary.pandapowerNet) -> dict:
    """Run pandapower diagnostic checks on the network.

    Executes all available diagnostic functions to validate the network
    structure and catch common modeling errors.

    Args:
        net: A pandapower Network object to validate.

    Returns:
        dict with keys for each diagnostic check and their results.
            Results are lists of issues found, empty if no issues detected.
            Disconnected islands < 3 elements are filtered as "trivial_fragments".
    """
    results = {}

    # Checks that don't require parameters
    simple_checks = {
        "different_voltage_levels_connected": pp.different_voltage_levels_connected,
        "invalid_values": pp.invalid_values,
        "missing_bus_indices": pp.missing_bus_indices,
        "multiple_voltage_controlling_elements_per_bus": pp.multiple_voltage_controlling_elements_per_bus,
        "no_ext_grid": pp.no_ext_grid,
        "deviation_from_std_type": pp.deviation_from_std_type,
    }

    for name, check_func in simple_checks.items():
        try:
            errors = check_func(net)
            results[name] = errors
        except Exception as e:
            results[name] = f"Error running diagnostic: {str(e)}"

    # Special handling for disconnected elements:
    # Filter out small trivial fragments (< 3 elements) as they are expected
    # from DBSCAN clustering at 500m granularity (stray buses, isolated gens/loads)
    try:
        disconnected = pp.disconnected_elements(net)
        if disconnected is not None:
            # Count elements in each island to identify trivial fragments
            results["disconnected_elements"] = disconnected
            trivial_count = sum(1 for v in disconnected.values() if len(v) < 3) if isinstance(disconnected, dict) else 0
            if trivial_count > 0:
                results["trivial_fragments_count"] = trivial_count
                results["note_disconnected"] = (
                    f"Found {trivial_count} small isolated fragments (< 3 elements). "
                    "These are expected from DBSCAN clustering at 500m granularity and "
                    "may represent stray substations, isolated generators, or data artifacts."
                )
        else:
            results["disconnected_elements"] = []
    except Exception as e:
        results["disconnected_elements"] = f"Error running diagnostic: {str(e)}"

    # Checks that require parameters - use sensible defaults
    try:
        # Implausible impedance values with typical ranges
        errors = pp.implausible_impedance_values(
            net, min_r_ohm=0.0, min_x_ohm=0.0, max_r_ohm=100.0, max_x_ohm=100.0
        )
        results["implausible_impedance_values"] = errors
    except Exception as e:
        results["implausible_impedance_values"] = f"Error running diagnostic: {str(e)}"

    try:
        # Nominal voltages mismatch with 5% tolerance
        errors = pp.nominal_voltages_dont_match(net, nom_voltage_tolerance=0.05)
        results["nominal_voltages_mismatch"] = errors
    except Exception as e:
        results["nominal_voltages_mismatch"] = f"Error running diagnostic: {str(e)}"

    return results


if __name__ == "__main__":
    net = create_pandapower_network(use_opennem=True)
    print(net)
