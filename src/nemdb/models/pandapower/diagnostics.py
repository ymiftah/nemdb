import pandapower as pp
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

from nemdb.logger import log


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


def sanity_checks(net: pp.auxiliary.pandapowerNet) -> dict:  # noqa: PLR0915
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
        if isinstance(disconnected, list) and len(disconnected) > 0:
            issues_found[0] = True
            element_count = sum(
                len(v) if isinstance(v, list) else 0
                for fragment in disconnected
                for v in fragment.values()
            )
            log.warning(
                f"  ✗ disconnected_elements: {len(disconnected)} fragment(s), "
                f"{element_count} disconnected element(s) found"
            )
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

    # Checks that require parameters - use sensible defaults.
    # max_x_ohm is set above pandapower's 100 ohm default because the NEM includes
    # genuinely long EHV/sub-transmission lines (up to ~360 km) whose total
    # reactance from the per-km _LINE_PARAMS naturally exceeds 100 ohm without
    # indicating a data problem.
    try:
        errors = ImplausibleImpedanceValues().diagnostic(
            net, min_r_ohm=0.0, min_x_ohm=0.0, max_r_ohm=100.0, max_x_ohm=200.0
        )
        results["implausible_impedance_values"] = errors
        _log_check_result("implausible_impedance_values", errors, issues_found)
    except Exception as e:
        error_msg = f"Error running diagnostic: {e!s}"
        results["implausible_impedance_values"] = error_msg
        _log_check_result("implausible_impedance_values", error_msg, issues_found)

    # Informational only (does not flip issues_found): very long/high-impedance
    # lines typically represent real lightly-loaded radial spans (e.g. rural
    # sub-transmission over 150+ km) rather than data errors -- a better Ohm/km
    # constant alone cannot fix the underlying long-line load-flow behavior, so
    # this is reported separately from the hard-failure checks above.
    try:
        in_service_lines = net.line[net.line["in_service"]]
        vn_kv = net.bus.loc[in_service_lines["from_bus"], "vn_kv"].to_numpy()
        z_base_ohm = vn_kv**2 / 100.0  # 100 MVA base, matching implausible_impedance_values
        x_pu = (
            in_service_lines["length_km"] * in_service_lines["x_ohm_per_km"]
        ).to_numpy() / z_base_ohm
        flagged = in_service_lines.index[x_pu > 1.0].tolist()
        results["long_high_impedance_lines"] = flagged
        if flagged:
            log.info(
                f"  ℹ long_high_impedance_lines: {len(flagged)} line(s) with x_pu > 1.0 "  # noqa: RUF001
                "(likely long/lightly-loaded radial spans, not a parameter bug)"
            )
            log.debug(f"    Lines: {flagged}")
        else:
            log.debug("  ✓ long_high_impedance_lines: OK")
    except Exception as e:
        results["long_high_impedance_lines"] = f"Error running diagnostic: {e!s}"
        log.error(f"  ✗ long_high_impedance_lines: {e!s}")

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
