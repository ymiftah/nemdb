import re
from pathlib import Path

import fastexcel
import polars as pl

import nemdb

_ISP = str(Path(*nemdb.__path__) / "artefacts" / "ISP_2025.xlsm")
_YEAR = re.compile(r"^\d{4}-\d{2}$")


def _unique_headers(raw_row: tuple) -> list[str]:
    """Convert a raw Excel row to unique column names, deduplicating where needed."""
    seen: dict[str, int] = {}
    result = []
    for i, v in enumerate(raw_row):
        name = str(v) if v is not None else f"_col{i}"
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            result.append(name)
    return result


# ── Core helpers ──────────────────────────────────────────────────────────────


def read_sheet(sheet_name: str, header_row: int) -> pl.DataFrame:
    """Load a sheet with a known header row. Returns a plain DataFrame."""
    return (
        fastexcel.read_excel(_ISP).load_sheet_by_name(sheet_name, header_row=header_row).to_polars()
    )


def read_timeseries(sheet_name: str, header_row: int) -> pl.DataFrame:
    """Load a sheet and unpivot all financial-year columns to a tidy long format.

    Year columns match the pattern YYYY-YY (e.g. "2025-26").
    Returns: [*id_cols, year (str), value (f64)]
    """
    df = read_sheet(sheet_name, header_row)
    year_cols = [c for c in df.columns if _YEAR.match(str(c))]
    id_cols = [c for c in df.columns if c not in year_cols]
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in year_cols])
    return df.unpivot(index=id_cols, on=year_cols, variable_name="year", value_name="value")


# ── Generator registry ────────────────────────────────────────────────────────


def existing_generators() -> pl.DataFrame:
    return read_sheet("Existing Gen Data Summary", 9)


def new_entrant_generators() -> pl.DataFrame:
    return read_sheet("New Entrant Data Summary", 8)


def new_electrolyser_data() -> pl.DataFrame:
    return read_sheet("New Electrolyser Data Summary", 4)


# ── Generator technical ───────────────────────────────────────────────────────


def storage_properties() -> pl.DataFrame:
    return read_sheet("Storage properties", 3)


def maximum_capacity() -> pl.DataFrame:
    return read_sheet("Maximum capacity", 8)


def seasonal_ratings() -> pl.DataFrame:
    return read_sheet("Seasonal ratings", 43)


def maintenance() -> pl.DataFrame:
    return read_sheet("Maintenance", 5)


def generator_reliability() -> pl.DataFrame:
    return read_sheet("Generator Reliability Settings", 9)


def heat_rates() -> pl.DataFrame:
    return read_sheet("Heat rates", 7)


def auxiliary() -> pl.DataFrame:
    return read_sheet("Auxiliary", 5)


def emissions_intensity() -> pl.DataFrame:
    return read_sheet("Emissions intensity", 7)


def fixed_opex() -> pl.DataFrame:
    return read_sheet("Fixed OPEX", 5)


def variable_opex() -> pl.DataFrame:
    return read_sheet("Variable OPEX", 5)


def affine_heat_rates() -> pl.DataFrame:
    return read_sheet("Affine Heat rates", 6)


def max_ramp_rates() -> pl.DataFrame:
    return read_sheet("Max Ramp Rates", 7)


def coal_min_stable_level() -> pl.DataFrame:
    return read_sheet("Coal Min Stable Level", 11)


def gpg_min_stable_level() -> pl.DataFrame:
    return read_sheet("GPG Min Stable Level", 10)


def capacity_factors() -> pl.DataFrame:
    return read_sheet("Capacity Factors ", 6)  # note trailing space in sheet name


def hydro_scheme_inflows() -> pl.DataFrame:
    return read_sheet("Hydro Scheme Inflows", 9)


# ── Costs / finance ───────────────────────────────────────────────────────────


def lead_time_project_life() -> pl.DataFrame:
    return read_sheet("Lead time and project life", 5)


def financial_parameters() -> pl.DataFrame:
    return read_sheet("Financial parameters", 5)


def locational_cost_factors() -> pl.DataFrame:
    return read_sheet("Locational Cost Factors", 9)


def connection_cost() -> pl.DataFrame:
    return read_sheet("Connection cost", 6)


def gas_system_properties() -> pl.DataFrame:
    return read_sheet("Gas System Properties", 10)


# ── Network ───────────────────────────────────────────────────────────────────


def renewable_energy_zones() -> pl.DataFrame:
    return read_sheet("Renewable energy zones", 5)


def network_capability() -> pl.DataFrame:
    return read_sheet("Network capability", 5)


def network_losses() -> pl.DataFrame:
    return read_sheet("Network losses", 6)


def transmission_reliability() -> pl.DataFrame:
    return read_sheet("Transmission Reliability", 6)


def flow_path_augmentation() -> pl.DataFrame:
    return read_sheet("Flow path augmentation options", 11)


def rez_augmentations() -> pl.DataFrame:
    return read_sheet("REZ augmentations options", 10)


def build_limits_rez() -> pl.DataFrame:
    return read_sheet("Build limits - REZs", 5)


def build_limits_phes() -> pl.DataFrame:
    return read_sheet("Build limits - PHES", 7)


# ── Retirement — main table (cols 0-5) + side table (cols 7-8) ───────────────


def retirement() -> pl.DataFrame:
    raw = fastexcel.read_excel(_ISP).load_sheet_by_name("Retirement", header_row=None).to_polars()
    header_row = 12
    header = _unique_headers(raw.row(header_row))
    data = raw.slice(header_row + 1)
    data.columns = header
    return data.select(data.columns[:6]).filter(pl.col(data.columns[0]).is_not_null())


def retirement_costs() -> pl.DataFrame:
    """Technology-level retirement cost lookup ($/MW). Side table on 'Retirement' sheet."""
    raw = fastexcel.read_excel(_ISP).load_sheet_by_name("Retirement", header_row=None).to_polars()
    header_row = 12
    header = _unique_headers(raw.row(header_row))
    data = raw.slice(header_row + 1)
    data.columns = header
    side = data.select([data.columns[7], data.columns[8]])
    side.columns = ["Technology Type", "Retirement ($/MW)"]
    return side.filter(pl.col("Technology Type").is_not_null())


# ── Marginal Loss Factors — existing (cols 0-11) + new entrant (cols 12-17) ──


def marginal_loss_factors() -> pl.DataFrame:
    """MLFs for existing, new entrant, and new entrant electrolyser generators.

    Three groups sit side-by-side in the sheet. Each is extracted separately
    and concatenated with a 'generator_group' label column.
    """
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("Marginal Loss Factors", header_row=None)
        .to_polars()
    )
    header_row = 11
    data = raw.slice(header_row + 1)

    def _group(col_start: int, col_end: int, label: str) -> pl.DataFrame:
        cols = raw.columns[col_start:col_end]
        names = [
            str(v) if v is not None else f"_col{i + col_start}"
            for i, v in enumerate(raw.row(header_row)[col_start:col_end])
        ]
        g = data.select(cols).filter(pl.col(cols[0]).is_not_null())
        g.columns = names
        return g.with_columns(pl.lit(label).alias("generator_group"))

    # Row 9 section labels: col 0 = Existing, col 7 = New Entrant, col 13 = New Entrant Electrolysers
    existing = _group(0, 6, "Existing")
    new_entrant = _group(7, 12, "New Entrant")
    electrolysers = _group(13, 18, "New Entrant Electrolysers")
    return pl.concat([existing, new_entrant, electrolysers], how="diagonal_relaxed")


# ── Build / capital costs ─────────────────────────────────────────────────────


def build_costs() -> pl.DataFrame:
    return read_timeseries("Build costs", 9)


def connection_cost_forecasts() -> pl.DataFrame:
    return read_timeseries("Connection cost forecasts", 8)


def flow_path_cost_forecasts() -> pl.DataFrame:
    return read_timeseries("Flow path cost forecasts", 11)


def rez_cost_forecasts() -> pl.DataFrame:
    return read_timeseries("REZ cost forecasts", 12)


def distribution_cost_forecasts() -> pl.DataFrame:
    return read_timeseries("Distribution cost forecasts", 5)


def build_cost_hydrogen_pipeline() -> pl.DataFrame:
    return read_timeseries("Build Cost - Hydrogen pipeline", 5)


# ── Fuel prices ───────────────────────────────────────────────────────────────


def coal_biomass_price() -> pl.DataFrame:
    return read_timeseries("Coal and Biomass price", 8)


def gas_liquid_h2_price() -> pl.DataFrame:
    return read_timeseries("Gas, Liquid fuel, H2 price", 8)


def gpg_emissions_reduction() -> pl.DataFrame:
    return read_timeseries("GPG emissions reduction - BioM", 7)


# ── DER / demand forecasts (Scenario col already embedded in data) ────────────


def rooftop_pv() -> pl.DataFrame:
    return read_timeseries("Rooftop PV", 14)


def pvnsg() -> pl.DataFrame:
    return read_timeseries("PVNSG", 14)


def onsg() -> pl.DataFrame:
    return read_timeseries("ONSG", 9)


def battery_plugin_evs() -> pl.DataFrame:
    return read_timeseries("Battery & Plug-in EVs", 13)


def fuel_cell_evs() -> pl.DataFrame:
    return read_timeseries("Fuel cell EVs", 13)


def ev_v2g() -> pl.DataFrame:
    return read_timeseries("EV V2G", 13)


def dsp() -> pl.DataFrame:
    return read_timeseries("DSP", 8)


def embedded_energy_storages() -> pl.DataFrame:
    return read_timeseries("Embedded energy storages", 13)


def aggregated_energy_storages() -> pl.DataFrame:
    return read_timeseries("Aggregated energy storages", 12)


def elec_retail_price_indices() -> pl.DataFrame:
    return read_timeseries("Elec. Retail Price Indices", 8)


def hydrogen_demand_domestic() -> pl.DataFrame:
    return read_timeseries("Hydrogen demand - Domestic", 7)


def hydrogen_demand_export() -> pl.DataFrame:
    return read_timeseries("Hydrogen demand-Export&Commod", 6)


def _stacked_blocks(sheet_name: str, blocks: list[tuple]) -> pl.DataFrame:
    """Extract stacked multi-scenario blocks (Pattern F sheets).

    Each block: (scenario, year_row, data_start, data_end_exclusive, key_col_name)
    Year row has [None-or-label, year1, year2, ...] — col0 may be ignored.
    """
    raw = fastexcel.read_excel(_ISP).load_sheet_by_name(sheet_name, header_row=None).to_polars()
    parts = []
    for scenario, year_row, data_start, data_end, key_col in blocks:
        years = [str(v) for v in raw.row(year_row)[1:] if v is not None and _YEAR.match(str(v))]
        for r in range(data_start, data_end):
            key = raw[r, 0]
            if key is None:
                continue
            for c_idx, year in enumerate(years):
                val = raw[r, c_idx + 1]
                parts.append(
                    {
                        "scenario": scenario,
                        key_col: str(key),
                        "year": year,
                        "value": float(val) if val is not None else None,
                    }
                )
    return pl.DataFrame(parts)


def data_centre_forecasts() -> pl.DataFrame:
    """Data centre electricity demand by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Data Centre Forecasts",
        [
            ("Slower Growth", 10, 11, 16, "state"),
            ("Step Change", 18, 19, 24, "state"),
            ("Accelerated Transition", 26, 27, 32, "state"),
        ],
    )


def electrification() -> pl.DataFrame:
    """Electrification of end-use energy by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Electrification",
        [
            ("Slower Growth", 12, 13, 19, "state"),
            ("Step Change", 21, 22, 28, "state"),
            ("Accelerated Transition", 30, 31, 37, "state"),
        ],
    )


def appliance_uptake() -> pl.DataFrame:
    """Residential appliance uptake by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Appliance Uptake Forecasts",
        [
            ("Slower Growth", 13, 14, 20, "state"),
            ("Step Change", 22, 23, 29, "state"),
            ("Accelerated Transition", 31, 32, 38, "state"),
        ],
    )


def connections_forecasts() -> pl.DataFrame:
    """Residential connections by scenario/state/year."""
    return _stacked_blocks(
        "Connections Forecasts",
        [
            ("Slower Growth", 15, 16, 22, "state"),
            ("Step Change", 24, 25, 31, "state"),
            ("Accelerated Transition", 33, 34, 40, "state"),
        ],
    )


def end_use_fuel_consumption() -> pl.DataFrame:
    """End-use fuel consumption by scenario/fuel sector/year (PJ).

    Note: scenario name is embedded in col0 of the year-header row for each block.
    """
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("End use fuel consumption data", header_row=None)
        .to_polars()
    )
    # Each block row has [scenario_name, year1, year2, ...] then data rows below
    YEAR_ROWS = [5, 16, 27]
    DATA_RANGES = [(6, 15), (17, 26), (28, 37)]
    parts = []
    for year_row, (data_start, data_end) in zip(YEAR_ROWS, DATA_RANGES, strict=True):
        scenario = str(raw[year_row, 0])
        years = [str(v) for v in raw.row(year_row)[1:] if v is not None and _YEAR.match(str(v))]
        for r in range(data_start, data_end):
            fuel = raw[r, 0]
            if fuel is None:
                continue
            for c_idx, year in enumerate(years):
                val = raw[r, c_idx + 1]
                parts.append(
                    {
                        "scenario": scenario,
                        "fuel_sector": str(fuel),
                        "year": year,
                        "value": float(val) if val is not None else None,
                    }
                )
    return pl.DataFrame(parts)


def energy_efficiency() -> pl.DataFrame:
    """Energy efficiency savings by section/scenario/state/year (GWh).

    Two sections: Residential and Business. Each has 4 scenarios including
    a 'Reduced Energy Efficiency' sensitivity.
    """
    BLOCKS = [
        ("Residential", "Slower Growth", 13, 14, 19),
        ("Residential", "Step Change", 21, 22, 27),
        ("Residential", "Accelerated Transition", 29, 30, 35),
        ("Residential", "Reduced Energy Efficiency", 37, 38, 43),
        ("Business", "Slower Growth", 47, 48, 53),
        ("Business", "Step Change", 55, 56, 61),
        ("Business", "Accelerated Transition", 63, 64, 69),
        ("Business", "Reduced Energy Efficiency", 71, 72, 77),
    ]
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("Energy Efficiency", header_row=None)
        .to_polars()
    )
    parts = []
    for section, scenario, year_row, data_start, data_end in BLOCKS:
        years = [str(v) for v in raw.row(year_row)[1:] if v is not None and _YEAR.match(str(v))]
        for r in range(data_start, data_end):
            state = raw[r, 0]
            if state is None:
                continue
            for c_idx, year in enumerate(years):
                val = raw[r, c_idx + 1]
                parts.append(
                    {
                        "section": section,
                        "scenario": scenario,
                        "state": str(state),
                        "year": year,
                        "value": float(val) if val is not None else None,
                    }
                )
    return pl.DataFrame(parts)


def hydrogen_monthly_profiles() -> pl.DataFrame:
    return read_timeseries("Hydrogen monthly profiles", 7)


def water_for_hydrogen() -> pl.DataFrame:
    return read_timeseries("Water for Hydrogen", 6)


def desalination_demand_h2() -> pl.DataFrame:
    return read_timeseries("Desalination demand for H2", 6)


def h2_gpg_limit() -> pl.DataFrame:
    return read_timeseries("H2 as fuel for GPG Limit", 5)


# ── Bespoke: Fuel Price Summary — two vertically-stacked tables ───────────────


def fuel_price_summary() -> pl.DataFrame:
    """Fuel prices for all generators. Adds 'generator_status' column."""
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("Fuel Price Summary", header_row=None)
        .to_polars()
    )

    def _extract(header_row: int, end_row: int, status: str) -> pl.DataFrame:
        header = [
            str(v) if v is not None else f"_col{i}" for i, v in enumerate(raw.row(header_row))
        ]
        data = raw.slice(header_row + 1, end_row - header_row - 1)
        data.columns = header
        data = data.filter(pl.col(data.columns[0]).is_not_null())
        year_cols = [c for c in data.columns if _YEAR.match(str(c))]
        id_cols = [c for c in data.columns if c not in year_cols]
        data = data.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in year_cols])
        return data.unpivot(
            index=id_cols, on=year_cols, variable_name="year", value_name="price_gj"
        ).with_columns(pl.lit(status).alias("generator_status"))

    existing = _extract(header_row=11, end_row=659, status="Existing")
    new_entrant = _extract(header_row=660, end_row=raw.shape[0], status="New entrant")
    return pl.concat([existing, new_entrant], how="diagonal_relaxed")


# ── Bespoke: Economic Growth Forecasts — null in col0 on year-header row ──────


def economic_growth_forecasts() -> pl.DataFrame:
    """GSP and HDI per state/scenario/year. Hardcoded block boundaries."""
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("Economic Growth Forecasts", header_row=None)
        .to_polars()
    )

    BLOCKS = [
        ("Gross State Product ($ millions, real 2024-25)", "Slower Growth", 12, 13, 19),
        ("Gross State Product ($ millions, real 2024-25)", "Step Change", 21, 22, 28),
        ("Gross State Product ($ millions, real 2024-25)", "Accelerated Transition", 30, 31, 37),
        (
            "Household Disposable Income ($ millions, real 2024-25)",
            "Slower Growth",
            41,
            42,
            48,
        ),
        (
            "Household Disposable Income ($ millions, real 2024-25)",
            "Step Change",
            50,
            51,
            57,
        ),
        (
            "Household Disposable Income ($ millions, real 2024-25)",
            "Accelerated Transition",
            59,
            60,
            66,
        ),
    ]

    parts = []
    for metric, scenario, year_row, data_start, data_end in BLOCKS:
        years = [str(v) for v in raw.row(year_row)[1:] if v is not None]
        rows = raw.slice(data_start, data_end - data_start)
        for r_idx in range(rows.shape[0]):
            state = rows[r_idx, 0]
            if state is None:
                continue
            for c_idx, year in enumerate(years):
                val = rows[r_idx, c_idx + 1]
                parts.append(
                    {
                        "metric": metric,
                        "scenario": scenario,
                        "state": str(state),
                        "year": year,
                        "value": float(val) if val is not None else None,
                    }
                )

    return pl.DataFrame(parts)


# ── Bespoke: Power System Security — cols 0-2 only ───────────────────────────


def power_system_security() -> pl.DataFrame:
    """Coal generator fault-level replacement costs ($M)."""
    raw = (
        fastexcel.read_excel(_ISP)
        .load_sheet_by_name("Power System Security", header_row=None)
        .to_polars()
    )
    header_row = 4
    header = [
        str(raw[header_row, c]) if raw[header_row, c] is not None else f"_col{c}" for c in range(3)
    ]
    data = raw.slice(header_row + 1).select(raw.columns[:3])
    data.columns = header
    return data.filter(pl.col(data.columns[0]).is_not_null())


# ── Convenience class ─────────────────────────────────────────────────────────


class ISP2025:
    """Cached access to all ISP 2025 sheets as tidy Polars DataFrames."""

    def __init__(self) -> None:
        self._cache: dict = {}

    def _get(self, key: str, fn):
        if key not in self._cache:
            self._cache[key] = fn()
        return self._cache[key]

    @property
    def existing_generators(self):
        return self._get("existing_generators", existing_generators)

    @property
    def new_entrant_generators(self):
        return self._get("new_entrant_generators", new_entrant_generators)

    @property
    def new_electrolyser_data(self):
        return self._get("new_electrolyser_data", new_electrolyser_data)

    @property
    def heat_rates(self):
        return self._get("heat_rates", heat_rates)

    @property
    def retirement(self):
        return self._get("retirement", retirement)

    @property
    def build_costs(self):
        return self._get("build_costs", build_costs)

    @property
    def fuel_prices(self):
        return self._get("fuel_prices", fuel_price_summary)

    @property
    def rooftop_pv(self):
        return self._get("rooftop_pv", rooftop_pv)

    @property
    def economic_growth(self):
        return self._get("economic_growth", economic_growth_forecasts)
