from typing import Any, Literal

import pandapower as pp
import pandas as pd

from nemdb.logger import log
from nemdb.models.pandapower.diagnostics import sanity_checks
from nemdb.models.pandapower.electrical_model import (
    get_pandapower_model,
    get_pandapower_model_with_opennem,
)
from nemdb.models.pandapower.line_params import _DEFAULT_LINE_PARAMS, _LINE_PARAMS


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
    _add_hvdc_interconnectors(net)
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


# Real HVDC interconnectors that the GA dataset represents as ordinary line
# geometry, which _add_lines_to_network turns into fictitious long-distance AC
# lines (e.g. Basslink as a 360 km, 400 kV AC line -- there is no 400 kV class
# in the NEM). Capacities/voltages are well-known public figures; loss_percent
# is an engineering approximation (no authoritative source in this codebase)
# and should be refined if precise AEMO figures become available.
_HVDC_INTERCONNECTORS: list[dict[str, Any]] = [
    {
        "name": "Basslink",
        "lines": ["Basslink-Loy Yang to Basslink-George Town"],
        "p_mw": 500.0,
        "loss_percent": 3.0,
    },
    {
        "name": "Murraylink",
        "lines": ["Monash to Red Cliffs Terminal"],
        "p_mw": 220.0,
        "loss_percent": 3.0,
    },
    {
        "name": "Directlink",
        "lines": ["Mullumbimby to Bungalora", "Bungalora to Terranora"],
        "p_mw": 180.0,
        "loss_percent": 2.0,
    },
]


def _add_hvdc_interconnectors(net: pp.auxiliary.pandapowerNet) -> pp.auxiliary.pandapowerNet:
    """Replace fictitious AC representations of real HVDC links with dclines.

    Each named AC line segment is replaced 1:1 by a dcline between the same
    bus pair. Multi-segment links (Directlink) get one dcline per segment
    rather than a single end-to-end connection, so intermediate substations
    (e.g. Bungalora, which has its own load) keep their only connection to
    the rest of the network.

    Args:
        net: A pandapower Network object with lines already added.

    Returns:
        The modified network with HVDC dclines added in place of the
        corresponding AC line segments (which are set out of service).
    """
    for link in _HVDC_INTERCONNECTORS:
        segment_idxs: list[int] = []
        bus_pairs: list[tuple[int, int]] = []
        for line_name in link["lines"]:
            matches = net.line[net.line["name"] == line_name]
            if matches.empty:
                segment_idxs = []
                break
            idx = matches.index[0]
            segment_idxs.append(idx)
            bus_pairs.append((net.line.at[idx, "from_bus"], net.line.at[idx, "to_bus"]))

        if not segment_idxs:
            log.debug(f"✗ Could not find line segment(s) for {link['name']}")
            continue

        net.line.loc[segment_idxs, "in_service"] = False
        loss_percent_per_segment = link["loss_percent"] / len(segment_idxs)
        for seg_num, (from_bus, to_bus) in enumerate(bus_pairs, start=1):
            pp.create_dcline(
                net,
                from_bus=from_bus,
                to_bus=to_bus,
                p_mw=link["p_mw"],
                loss_percent=loss_percent_per_segment,
                loss_mw=0.0,
                vm_from_pu=1.0,
                vm_to_pu=1.0,
                name=f"dcline_{link['name']}" + (f"_{seg_num}" if len(bus_pairs) > 1 else ""),
            )
        log.debug(f"✓ Added {len(bus_pairs)} HVDC dcline segment(s) for {link['name']}")

    return net


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
                # A bus cannot have both a gen and an ext_grid controlling its
                # voltage (pandapower flags this as
                # multiple_voltage_controlling_elements_per_bus). Co-located
                # generation at a slack bus represents dispatch against the
                # external grid reference, not an independent voltage setpoint,
                # so convert any such generators to sgen (which doesn't control
                # voltage) before creating the ext_grid.
                colocated_gens = net.gen[net.gen["bus"] == target_bus_id]
                for _gen_idx, gen_row in colocated_gens.iterrows():
                    pp.create_sgen(
                        net,
                        bus=gen_row["bus"],
                        p_mw=gen_row["p_mw"],
                        name=gen_row["name"],
                        type=gen_row["type"],
                        in_service=gen_row["in_service"],
                    )
                    log.debug(
                        f"  ↳ Converted co-located generator '{gen_row['name']}' "
                        f"at bus {target_bus_id} to sgen (slack bus cannot host a gen)"
                    )
                if len(colocated_gens) > 0:
                    net.gen = net.gen.drop(index=colocated_gens.index).reset_index(drop=True)

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
