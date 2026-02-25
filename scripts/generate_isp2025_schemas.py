#!/usr/bin/env python3
"""Generate CSVs and Pandera schemas for all ISP 2025 tables.

Run from repo root:
    uv run python scripts/generate_isp2025_schemas.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nemdb.isp.isp2025 import (
    affine_heat_rates,
    aggregated_energy_storages,
    appliance_uptake,
    auxiliary,
    battery_plugin_evs,
    build_cost_hydrogen_pipeline,
    build_costs,
    build_limits_phes,
    build_limits_rez,
    capacity_factors,
    coal_biomass_price,
    coal_min_stable_level,
    connection_cost,
    connection_cost_forecasts,
    connections_forecasts,
    data_centre_forecasts,
    desalination_demand_h2,
    distribution_cost_forecasts,
    dsp,
    economic_growth_forecasts,
    elec_retail_price_indices,
    electrification,
    embedded_energy_storages,
    emissions_intensity,
    end_use_fuel_consumption,
    energy_efficiency,
    ev_v2g,
    existing_generators,
    financial_parameters,
    fixed_opex,
    flow_path_augmentation,
    flow_path_cost_forecasts,
    fuel_cell_evs,
    fuel_price_summary,
    gas_liquid_h2_price,
    gas_system_properties,
    generator_reliability,
    gpg_emissions_reduction,
    gpg_min_stable_level,
    h2_gpg_limit,
    heat_rates,
    hydro_scheme_inflows,
    hydrogen_demand_domestic,
    hydrogen_demand_export,
    hydrogen_monthly_profiles,
    lead_time_project_life,
    locational_cost_factors,
    maintenance,
    marginal_loss_factors,
    max_ramp_rates,
    maximum_capacity,
    network_capability,
    network_losses,
    new_electrolyser_data,
    new_entrant_generators,
    onsg,
    power_system_security,
    pvnsg,
    renewable_energy_zones,
    retirement,
    retirement_costs,
    rez_augmentations,
    rez_cost_forecasts,
    rooftop_pv,
    seasonal_ratings,
    storage_properties,
    transmission_reliability,
    variable_opex,
    water_for_hydrogen,
)

SCRATCH_DIR = Path(__file__).parent.parent / "scratch" / "isp2025"
SCHEMA_FILE = Path(__file__).parent.parent / "src" / "nemdb" / "isp" / "schemas.py"

# (class_name, function) pairs — one per table
TABLES: list[tuple[str, object]] = [
    # Static sheets
    ("ExistingGenerators", existing_generators),
    ("NewEntrantGenerators", new_entrant_generators),
    ("NewElectrolyserData", new_electrolyser_data),
    ("StorageProperties", storage_properties),
    ("MaximumCapacity", maximum_capacity),
    ("SeasonalRatings", seasonal_ratings),
    ("Maintenance", maintenance),
    ("GeneratorReliability", generator_reliability),
    ("Retirement", retirement),
    ("RetirementCosts", retirement_costs),
    ("HeatRates", heat_rates),
    ("Auxiliary", auxiliary),
    ("EmissionsIntensity", emissions_intensity),
    ("FixedOpex", fixed_opex),
    ("VariableOpex", variable_opex),
    ("AffineHeatRates", affine_heat_rates),
    ("MaxRampRates", max_ramp_rates),
    ("CoalMinStableLevel", coal_min_stable_level),
    ("GpgMinStableLevel", gpg_min_stable_level),
    ("LeadTimeProjectLife", lead_time_project_life),
    ("FinancialParameters", financial_parameters),
    ("GasSystemProperties", gas_system_properties),
    ("RenewableEnergyZones", renewable_energy_zones),
    ("NetworkCapability", network_capability),
    ("NetworkLosses", network_losses),
    ("TransmissionReliability", transmission_reliability),
    ("ConnectionCost", connection_cost),
    ("FlowPathAugmentation", flow_path_augmentation),
    ("RezAugmentations", rez_augmentations),
    ("BuildLimitsRez", build_limits_rez),
    ("BuildLimitsPhes", build_limits_phes),
    ("LocationalCostFactors", locational_cost_factors),
    ("CapacityFactors", capacity_factors),
    ("MarginalLossFactors", marginal_loss_factors),
    ("HydroSchemeInflows", hydro_scheme_inflows),
    # Timeseries sheets
    ("BuildCosts", build_costs),
    ("ConnectionCostForecasts", connection_cost_forecasts),
    ("FlowPathCostForecasts", flow_path_cost_forecasts),
    ("RezCostForecasts", rez_cost_forecasts),
    ("DistributionCostForecasts", distribution_cost_forecasts),
    ("CoalBiomassPrice", coal_biomass_price),
    ("GasLiquidH2Price", gas_liquid_h2_price),
    ("RooftopPv", rooftop_pv),
    ("Pvnsg", pvnsg),
    ("Onsg", onsg),
    ("BatteryPluginEvs", battery_plugin_evs),
    ("FuelCellEvs", fuel_cell_evs),
    ("EvV2g", ev_v2g),
    ("Dsp", dsp),
    ("DataCentreForecasts", data_centre_forecasts),
    ("Electrification", electrification),
    ("EmbeddedEnergyStorages", embedded_energy_storages),
    ("AggregatedEnergyStorages", aggregated_energy_storages),
    ("EndUseFuelConsumption", end_use_fuel_consumption),
    ("ApplianceUptake", appliance_uptake),
    ("ElecRetailPriceIndices", elec_retail_price_indices),
    ("ConnectionsForecasts", connections_forecasts),
    ("EnergyEfficiency", energy_efficiency),
    ("HydrogenDemandDomestic", hydrogen_demand_domestic),
    ("HydrogenDemandExport", hydrogen_demand_export),
    ("HydrogenMonthlyProfiles", hydrogen_monthly_profiles),
    ("WaterForHydrogen", water_for_hydrogen),
    ("DesalinationDemandH2", desalination_demand_h2),
    ("H2GpgLimit", h2_gpg_limit),
    ("BuildCostHydrogenPipeline", build_cost_hydrogen_pipeline),
    ("GpgEmissionsReduction", gpg_emissions_reduction),
    # Bespoke sheets
    ("FuelPriceSummary", fuel_price_summary),
    ("EconomicGrowthForecasts", economic_growth_forecasts),
    ("PowerSystemSecurity", power_system_security),
]


def _to_py_name(col: str) -> str:
    """Convert a column name to a valid Python identifier (snake_case)."""
    name = re.sub(r"[^a-zA-Z0-9_]", "_", col).lower()
    name = re.sub(r"_+", "_", name).strip("_")
    if name and name[0].isdigit():
        name = f"col_{name}"
    return name or "col"


def _dtype_str(dtype: pl.DataType) -> str:
    """Return a pl.* annotation string for a Polars dtype."""
    if isinstance(dtype, pl.List):
        return f"pl.List({_dtype_str(dtype.inner)})"
    return f"pl.{type(dtype).__name__}"


def _class_body(class_name: str, df: pl.DataFrame) -> str:
    lines = [f"class {class_name}(pa.DataFrameModel):"]
    seen: dict[str, int] = {}
    for col, dtype in df.schema.items():
        base = _to_py_name(col)
        # Deduplicate: if we've seen this name before, append a counter
        count = seen.get(base, 0)
        seen[base] = count + 1
        py_name = base if count == 0 else f"{base}_{count}"
        ann = _dtype_str(dtype)
        if py_name == col:
            lines.append(f"    {py_name}: {ann} | None")
        else:
            # Column name is not a valid identifier — use alias
            # Escape backslashes, quotes, and control characters
            escaped = (
                col.replace("\\", "\\\\")
                .replace('"', '\\"')
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
            )
            lines.append(f'    {py_name}: {ann} | None = pa.Field(alias="{escaped}")')
    return "\n".join(lines)


def main() -> None:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

    class_blocks: list[str] = []

    for class_name, fn in TABLES:
        print(f"  {fn.__name__} ...", end=" ", flush=True)
        df = fn()
        # Write CSV
        csv_path = SCRATCH_DIR / f"{fn.__name__}.csv"
        df.write_csv(csv_path)
        # Generate schema class
        class_blocks.append(_class_body(class_name, df))
        print(f"{df.shape[0]} rows, {df.shape[1]} cols → {csv_path.name}")

    header = '''\
"""Pandera schemas for ISP 2025 tables.

Auto-generated by scripts/generate_isp2025_schemas.py — edit as needed.
"""

import pandera.polars as pa
import polars as pl

'''
    schema_code = header + "\n\n\n".join(class_blocks) + "\n"
    SCHEMA_FILE.write_text(schema_code)
    print(f"\nCSVs written to: {SCRATCH_DIR}")
    print(f"Schemas written to: {SCHEMA_FILE}")


if __name__ == "__main__":
    main()
