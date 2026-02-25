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
    read_sheet,
    read_timeseries,
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

# ── Core helpers ──────────────────────────────────────────────────────────────


def test_read_sheet_returns_dataframe():
    df = read_sheet("Heat rates", header_row=7)
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 600
    assert "IASR ID" in df.columns


def test_read_timeseries_unpivots_years():
    df = read_timeseries("Build costs", header_row=9)
    assert "year" in df.columns
    assert "value" in df.columns
    years = df["year"].unique().to_list()
    assert "2025-26" in years
    assert len(years) >= 29


# ── Static sheets ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "fn, min_rows, required_col",
    [
        (existing_generators, 640, "IASR ID"),
        (new_entrant_generators, 510, "IASR ID"),
        (new_electrolyser_data, 140, "IASR ID"),
        (storage_properties, 8, "Technology"),
        (maximum_capacity, 650, "IASR ID"),
        (seasonal_ratings, 640, "IASR ID"),
        (maintenance, 10, "Generator type"),
        (generator_reliability, 10, "Fuel type"),
        (retirement, 640, "IASR ID"),
        (retirement_costs, 8, "Technology Type"),
        (heat_rates, 640, "IASR ID"),
        (auxiliary, 640, "IASR ID"),
        (emissions_intensity, 640, "IASR ID"),
        (fixed_opex, 640, "IASR ID"),
        (variable_opex, 640, "IASR ID"),
        (affine_heat_rates, 170, "IASR ID"),
        (max_ramp_rates, 170, "IASR ID"),
        (coal_min_stable_level, 40, "IASR ID"),
        (gpg_min_stable_level, 130, "IASR ID"),
        (lead_time_project_life, 20, "Technology"),
        (financial_parameters, 10, None),
        (gas_system_properties, 170, "Name"),
        (renewable_energy_zones, 50, "ID"),
        (network_capability, 140, "Flow Paths"),
        (network_losses, 20, "Flow Path"),
        (transmission_reliability, 5, "Line/Flowpath"),
        (connection_cost, 60, "REZ ID"),
        (flow_path_augmentation, 120, "Flow path"),
        (rez_augmentations, 120, "REZ / constraint ID"),
        (build_limits_rez, 240, "REZ ID"),
        (build_limits_phes, 10, "Name"),
        (locational_cost_factors, 210, "Cost zone / REZ ID"),
        (capacity_factors, 200, "REZ ID / Subregion"),
        (marginal_loss_factors, 650, "IASR ID"),
        (hydro_scheme_inflows, 140, "Reference Year (FYE)"),
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
        (build_costs, 1500, "IASR Scenario"),
        (connection_cost_forecasts, 10000, None),
        (flow_path_cost_forecasts, 9000, None),
        (rez_cost_forecasts, 8000, None),
        (distribution_cost_forecasts, 2000, None),
        (coal_biomass_price, 500, "Scenario"),
        (gas_liquid_h2_price, 10000, "Gas price scenario"),
        (rooftop_pv, 1000, "Scenario"),
        (pvnsg, 1000, "Scenario"),
        (onsg, 500, "Scenario"),
        (battery_plugin_evs, 1000, "Scenario"),
        (fuel_cell_evs, 500, "Scenario"),
        (ev_v2g, 1000, "Scenario"),
        (dsp, 1500, "Scenario"),
        (data_centre_forecasts, 400, "scenario"),
        (electrification, 500, "scenario"),
        (embedded_energy_storages, 1000, "Scenario"),
        (aggregated_energy_storages, 1000, "Scenario"),
        (end_use_fuel_consumption, 700, "scenario"),
        (appliance_uptake, 500, "scenario"),
        (elec_retail_price_indices, 90, None),
        (connections_forecasts, 500, "scenario"),
        (energy_efficiency, 500, "scenario"),
        (hydrogen_demand_domestic, 500, "Scenario"),
        (hydrogen_demand_export, 1500, "Scenario"),
        (hydrogen_monthly_profiles, 300, None),
        (water_for_hydrogen, 500, "Scenario"),
        (desalination_demand_h2, 500, "Scenario"),
        (h2_gpg_limit, 100, None),
        (build_cost_hydrogen_pipeline, 4000, None),
        (gpg_emissions_reduction, 100, None),
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
