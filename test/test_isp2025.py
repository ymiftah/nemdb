import datetime

import polars as pl
import pytest

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
    build_limits_rez_group_constraints,
    build_limits_rez_modifiers,
    build_limits_rez_secondary_transmission_limits,
    build_limits_rez_transmission,
    build_limits_rez_transmission_limit_constraints,
    capacity_factors,
    coal_biomass_price,
    coal_min_stable_level,
    connection_cost,
    connection_cost_forecasts_other,
    connection_cost_forecasts_wind_and_solar,
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
    locational_cost_factors_phes,
    locational_cost_factors_technology_specific,
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
    read_sheet,
    read_timeseries,
    renewable_energy_zones,
    retirement,
    retirement_costs,
    rez_augmentations,
    rez_cost_forecasts,
    rooftop_pv,
    seasonal_ratings,
    storage_battery_properties,
    storage_hydro_properties,
    transmission_reliability,
    variable_opex,
    water_for_hydrogen,
)

# ── Core helpers ──────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_read_sheet_returns_dataframe():
    df = read_sheet("Heat rates", header_row=7)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 600
    assert "iasr_id" in df.columns


@pytest.mark.slow
def test_read_timeseries_unpivots_years():
    df = read_timeseries("Build costs", header_row=9)
    assert "year" in df.columns
    assert "value" in df.columns
    years = df["year"].unique().to_list()
    assert datetime.date(2025, 6, 30) in years
    assert len(years) >= 29


# ── Static sheets ─────────────────────────────────────────────────────────────


@pytest.mark.slow
@pytest.mark.parametrize(
    "fn, min_rows, required_col",
    [
        (existing_generators, 640, "iasr_id"),
        (new_entrant_generators, 510, "iasr_id"),
        (new_electrolyser_data, 140, "iasr_id"),
        (storage_battery_properties, 7, "technology"),
        (storage_hydro_properties, 11, "power_station"),
        (maximum_capacity, 640, "iasr_id"),
        (seasonal_ratings, 640, "iasr_id"),
        (maintenance, 10, "generator_type"),
        (generator_reliability, 10, "fuel_type"),
        (retirement, 640, "iasr_id"),
        (retirement_costs, 8, "technology_type"),
        (heat_rates, 640, "iasr_id"),
        (auxiliary, 640, "iasr_id"),
        (emissions_intensity, 640, "iasr_id"),
        (fixed_opex, 640, "iasr_id"),
        (variable_opex, 640, "iasr_id"),
        (affine_heat_rates, 170, "iasr_id"),
        (max_ramp_rates, 170, "iasr_id"),
        (coal_min_stable_level, 40, "iasr_id"),
        (gpg_min_stable_level, 130, "iasr_id"),
        (lead_time_project_life, 20, "technology"),
        (financial_parameters, 10, None),
        (gas_system_properties, 170, "name"),
        (renewable_energy_zones, 50, "id"),
        (network_capability, 20, "flow_paths"),
        (network_losses, 20, "flow_path"),
        (transmission_reliability, 5, "line_flowpath"),
        (connection_cost, 45, "rez_id"),
        (flow_path_augmentation, 55, "flow_path"),
        (rez_augmentations, 120, "rez_constraint_id"),
        (build_limits_rez, 51, "rez_id"),
        (build_limits_rez_transmission, 47, "rez_id"),
        (build_limits_rez_modifiers, 8, "rez_id"),
        (build_limits_rez_group_constraints, 81, "term"),
        (build_limits_rez_transmission_limit_constraints, 18, "term"),
        (build_limits_rez_secondary_transmission_limits, 13, "term"),
        (build_limits_phes, 10, "name"),
        (locational_cost_factors, 64, "cost_zone_rez_id"),
        (locational_cost_factors_phes, 45, "subregion"),
        (locational_cost_factors_technology_specific, 64, "cost_zone_rez_id"),
        (capacity_factors, 200, "rez_id_subregion"),
        (marginal_loss_factors, 650, "iasr_id"),
        (hydro_scheme_inflows, 120, "reference_year_fye"),
    ],
)
def test_static_sheet(fn, min_rows, required_col):
    df = fn()
    assert df.shape[0] >= min_rows, f"{fn.__name__}: expected >={min_rows} rows, got {df.shape[0]}"
    if required_col:
        assert required_col in df.columns, f"{fn.__name__}: missing column {required_col!r}"


# ── Time-series sheets ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "fn, min_rows, scenario_col",
    [
        (build_costs, 1500, "iasr_scenario"),
        (connection_cost_forecasts_wind_and_solar, 1000, None),
        (connection_cost_forecasts_other, 1000, None),
        (flow_path_cost_forecasts, 9000, None),
        (rez_cost_forecasts, 8000, None),
        (distribution_cost_forecasts, 2000, None),
        (coal_biomass_price, 500, "scenario"),
        (gas_liquid_h2_price, 9900, "price_scenario"),
        (rooftop_pv, 1000, "scenario"),
        (pvnsg, 1000, "scenario"),
        (onsg, 500, "scenario"),
        (battery_plugin_evs, 1000, "scenario"),
        (fuel_cell_evs, 500, "scenario"),
        (ev_v2g, 1000, "scenario"),
        (dsp, 1500, "scenario"),
        (data_centre_forecasts, 400, "scenario"),
        (electrification, 500, "scenario"),
        (embedded_energy_storages, 1000, "scenario"),
        (aggregated_energy_storages, 1000, "scenario"),
        (end_use_fuel_consumption, 700, "scenario"),
        (appliance_uptake, 500, "scenario"),
        (elec_retail_price_indices, 90, None),
        (connections_forecasts, 500, "scenario"),
        (energy_efficiency, 500, "scenario"),
        (hydrogen_demand_domestic, 500, "scenario"),
        (hydrogen_demand_export, 1500, "scenario"),
        (hydrogen_monthly_profiles, 300, None),
        (water_for_hydrogen, 500, "scenario"),
        (desalination_demand_h2, 500, "scenario"),
        (h2_gpg_limit, 100, None),
        (build_cost_hydrogen_pipeline, 4000, None),
        (gpg_emissions_reduction, 85, None),
    ],
)
def test_timeseries_sheet(fn, min_rows, scenario_col):
    df = fn()
    assert "year" in df.columns, f"{fn.__name__}: missing 'year' column"
    assert "value" in df.columns, f"{fn.__name__}: missing 'value' column"
    assert df.shape[0] >= min_rows, f"{fn.__name__}: expected >={min_rows} rows"
    if scenario_col:
        assert scenario_col in df.columns, f"{fn.__name__}: missing '{scenario_col}'"


# ── Bespoke sheets ────────────────────────────────────────────────────────────


def test_fuel_price_summary():
    df = fuel_price_summary()
    assert "generator_status" in df.columns
    assert set(df["generator_status"].unique().to_list()) == {"Existing", "New entrant"}
    assert "year" in df.columns
    assert df.shape[0] > 30_000


def test_economic_growth_forecasts():
    df = economic_growth_forecasts()
    assert set(df["scenario"].unique().to_list()) == {
        "Slower Growth",
        "Step Change",
        "Accelerated Transition",
    }
    assert df["metric"].n_unique() == 2
    assert "NSW" in df["state"].unique().to_list()


def test_power_system_security():
    df = power_system_security()
    assert df.shape[0] > 0
    assert df.shape[1] == 3
