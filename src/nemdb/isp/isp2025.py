import functools
import os
import re
import zipfile

import fastexcel
import polars as pl
import pooch
from pandera import check_types
from pandera.typing.polars import DataFrame

from . import schemas

_ISP_SHA256 = "b1cbb764b7d9cfb0189f589da7998f65ff26516343bc1ee3b86c144a6f6c2d1b"
_ISP_FETCHER = pooch.create(
    path=pooch.os_cache("nemdb"),
    base_url="https://github.com/ymiftah/nemdb/releases/download/data-v1/",
    registry={"ISP_2025.zip": f"sha256:{_ISP_SHA256}"},
)
_YEAR = re.compile(r"^\d{4}[-_]\d{2}$")  # Matches YYYY-YY or YYYY_YY (after normalization)


def _get_isp_path() -> str:
    """Return path to ISP_2025.zip, using local override if set via NEMDB_ISP_2025."""
    local = os.environ.get("NEMDB_ISP_2025")
    if local:
        return local
    return str(_ISP_FETCHER.fetch("ISP_2025.zip"))


def _open_isp() -> fastexcel.ExcelReader:
    """Open the ISP workbook from the zip archive."""
    with zipfile.ZipFile(_get_isp_path()) as z:
        data = z.read(z.namelist()[0])
    return fastexcel.read_excel(data)


def _to_snake_case(name: str) -> str:
    """Convert a string to lowercase snake_case.

    Handles spaces, hyphens, slashes, and special characters.
    """
    # Replace spaces, hyphens, slashes, and newlines with underscores
    name = re.sub(r"[\s\-/\n]+", "_", name)
    # Remove parentheses, periods, commas
    name = re.sub(r"[().,]", "", name)
    # Replace $ signs by aud
    name = re.sub(r"\$", "aud", name)
    # Convert to lowercase
    name = name.lower()
    # Remove leading/trailing underscores
    name = name.strip("_")
    # Replace multiple underscores with single
    name = re.sub(r"_+", "_", name)
    # Replace multiple underscores with single
    name = re.sub(r"\%", "pct", name)
    return name


def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize all column names to lowercase snake_case, handling duplicates."""
    rename_map = {}
    seen = {}

    for col in df.columns:
        normalized = _to_snake_case(col)
        if normalized not in seen:
            seen[normalized] = 0
            rename_map[col] = normalized
        else:
            seen[normalized] += 1
            rename_map[col] = f"{normalized}_{seen[normalized]}"

    return df.rename(rename_map)


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
    """Load a sheet with a known header row. Returns a plain DataFrame (untyped)."""
    return (
        _open_isp()
        .load_sheet_by_name(sheet_name, header_row=header_row)
        .to_polars()
        .pipe(_normalize_columns)
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
    return (
        df.unpivot(index=id_cols, on=year_cols, variable_name="year", value_name="value")
        .pipe(_parse_year_columns)
        .pipe(_normalize_columns)
    )


def _parse_year_columns(df) -> pl.DataFrame:
    return df.with_columns(
        pl.col("year").str.extract(r"(\d{4})", 0).cast(int).add(1).alias("year")
    ).with_columns(
        pl.date(
            pl.col("year"),
            6,
            30,
        ).alias("year")
    )


def _read_sheet_block(
    raw: pl.DataFrame, header_row: int, data_start: int, data_end: int
) -> pl.DataFrame:
    """Extract one flat (non-timeseries) block using explicit row bounds.

    Applies the header at `header_row` to data rows [data_start, data_end),
    deduplicates column names, drops all-null auto-generated column names
    (_col{n} artefacts from merged-cell or short header rows).
    """
    col_names = _unique_headers(raw.row(header_row))
    data = raw.slice(data_start, data_end - data_start)
    data.columns = col_names
    keep = [c for c in col_names if not (c.startswith("_col") and data[c].is_null().all())]
    data = data.select(keep)
    # Normalize columns and handle any resulting duplicates
    data = data.pipe(_normalize_columns)
    # After normalization, deduplicate any column names that became identical
    if len(data.columns) != len(set(data.columns)):
        seen = {}
        new_cols = []
        for col in data.columns:
            if col not in seen:
                seen[col] = 0
                new_cols.append(col)
            else:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
        data.columns = new_cols
    return data


def _read_multiheader_block(
    raw: pl.DataFrame, header_rows: list[int], data_start: int
) -> pl.DataFrame:
    """Extract a block whose header spans multiple Excel rows.

    Flattens all header rows into a single combined column name per column.
    The first header row is forward-filled to handle merged cells.
    Data rows run from `data_start` to the end of the sheet.
    """
    n_cols = raw.shape[1]
    rows = [list(raw.row(r)) for r in header_rows]

    # Forward-fill the first header row for merged cells
    last_l1 = None
    for i in range(n_cols):
        if rows[0][i] is not None:
            last_l1 = rows[0][i]
        elif any(rows[j][i] is not None for j in range(1, len(rows))):
            rows[0][i] = last_l1

    # Combine non-None parts from all levels into one name per column
    col_names = []
    for i in range(n_cols):
        parts = [str(rows[j][i]).strip() for j in range(len(rows)) if rows[j][i] is not None]
        col_names.append(_to_snake_case(" ".join(parts)) if parts else f"_col{i}")

    data = raw.slice(data_start, raw.shape[0] - data_start)
    data.columns = col_names
    keep = [c for c in col_names if not (c.startswith("_col") and data[c].is_null().all())]
    data = data.select(keep)
    # Treat empty strings as null (Excel empty cells arrive as "" with header_row=None)
    return data.with_columns(pl.col(pl.String).replace("", None))


def _parse_timeseries_block(
    raw: pl.DataFrame, header_row: int, data_start: int, data_end: int
) -> pl.DataFrame:
    """Extract one timeseries block from a raw sheet using explicit row bounds.

    Applies the header at `header_row` to data rows [data_start, data_end),
    filters null key rows (empty separator rows in the sheet), casts year
    columns to Float64, and unpivots to long format.
    """
    col_names = _unique_headers(raw.row(header_row))
    data = raw.slice(data_start, data_end - data_start)
    data.columns = col_names
    year_cols = [c for c in col_names if _YEAR.match(str(c))]
    id_cols = [c for c in col_names if c not in year_cols]
    # Drop auto-named columns (_col{n}) that are entirely null — these arise when a
    # header row is shorter than the raw sheet width (e.g. ONSG regional table).
    id_cols = [c for c in id_cols if not (c.startswith("_col") and data[c].is_null().all())]
    data = data.filter(pl.col(id_cols[0]).is_not_null())
    data = data.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in year_cols])
    return (
        data.unpivot(index=id_cols, on=year_cols, variable_name="year", value_name="value")
        .pipe(_parse_year_columns)
        .pipe(_normalize_columns)
    )


# ── Generator registry ────────────────────────────────────────────────────────


@check_types
def existing_generators() -> DataFrame[schemas.ExistingGenerators]:
    raw = _open_isp().load_sheet_by_name("Existing Gen Data Summary", header_row=None).to_polars()
    df = _read_multiheader_block(raw, header_rows=[9, 10, 11], data_start=12)
    df = df.filter(pl.col("iasr_id").is_not_null())
    _sentinels = ["not found", "-", "Assumed sufficiently high"]
    return df.with_columns(pl.col(pl.String).replace(_sentinels, None))  # type: ignore


@check_types
def new_entrant_generators() -> DataFrame[schemas.NewEntrantGenerators]:
    raw = _open_isp().load_sheet_by_name("New Entrant Data Summary", header_row=None).to_polars()
    df = _read_multiheader_block(raw, header_rows=[8, 9, 10], data_start=11)
    df = df.filter(pl.col("iasr_id").is_not_null())
    _sentinels = [
        "not found",
        "-",
        "Assumed sufficiently high",
        "At the end of technical life",
        "Not Applicable",
    ]
    return df.with_columns(pl.col(pl.String).replace(_sentinels, None))  # type: ignore


@check_types
def new_electrolyser_data() -> DataFrame[schemas.NewElectrolyserData]:
    raw = (
        _open_isp().load_sheet_by_name("New Electrolyser Data Summary", header_row=None).to_polars()
    )
    df = _read_sheet_block(raw, 4, 7, 158)
    df = df.filter(pl.col("iasr_id").is_not_null())
    _sentinels = [
        "not found",
        "-",
        "Assumed sufficiently high",
        "At the end of technical life",
        "Not Applicable",
    ]
    return df.with_columns(pl.col(pl.String).replace(_sentinels, None))  # type: ignore


# ── Generator technical ───────────────────────────────────────────────────────


@check_types
def storage_battery_properties() -> DataFrame[schemas.StorageBatteryProperties]:
    raw = _open_isp().load_sheet_by_name("Storage properties", header_row=None).to_polars()
    df = _read_sheet_block(raw, 3, 5, 12)  # battery data rows 5-11; skip units row 4
    return df.rename(
        {
            "maximum_power1": "maximum_power",
            "energy_capacity2": "energy_capacity",
            "round_trip_efficiency3": "round_trip_efficiency",
            "annual_degradation4": "annual_degradation",
        }
    )  # type: ignore


@check_types
def storage_hydro_properties() -> DataFrame[schemas.StorageHydroProperties]:
    raw = _open_isp().load_sheet_by_name("Storage properties", header_row=None).to_polars()
    existing = _read_sheet_block(raw.select(raw.columns[:4]), 20, 21, 28).with_columns(
        pl.lit("existing").alias("type")
    )
    new_entrant = (
        _read_sheet_block(raw.select(raw.columns[5:9]), 20, 21, 25)
        .rename({"power_station_technology": "power_station"})
        .with_columns(pl.lit("new_entrant").alias("type"))
    )
    return pl.concat([existing, new_entrant], how="diagonal").select(
        [
            "type",
            "power_station",
            "installed_capacity_mw",
            "storage_capacity_hours",
            "pumping_efficiency_pct",
        ]
    )  # type: ignore


@check_types
def maximum_capacity() -> DataFrame[schemas.MaximumCapacity]:
    """Generator nameplate capacity (MW). Footnotes (rows 652+) are excluded.

    The sheet has a side technology-level lookup in cols 10-13; only the main cols 0-8 are
    returned. Use maximum_capacity_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Maximum capacity", header_row=None).to_polars()
    df = _read_sheet_block(raw, 8, 9, 652)
    df = df.select(df.columns[:9])
    return (
        df.rename({"status5": "status"})
        .with_columns(pl.col(pl.String).replace([""], None))
        .with_columns(
            pl.col("commissioning_date")
            .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
            .cast(pl.Date)
        )
    )  # type: ignore


@check_types
def maximum_capacity_by_technology() -> DataFrame[schemas.MaximumCapacityByTechnology]:
    """Technology-level unit size lookup table from the 'Maximum capacity' sheet.

    Side table (22 rows): Technology Type, Unit size (MW), Number of units, Total plant size (MW).
    """
    raw = _open_isp().load_sheet_by_name("Maximum capacity", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[10:]), 8, 9, 31)  # type: ignore


@check_types
def seasonal_ratings() -> DataFrame[schemas.SeasonalRatings]:
    """Seasonal ratings (% of nameplate) by generator and demand condition.

    The sheet holds two stacked tables with different schemas. A 'generator_group'
    column distinguishes them:
    - 'new_entrant': technology-type ratings (rows 11-31, header at row 10)
    - 'existing_committed_anticipated': unit-level ratings (rows 44-686, header at row 43)
    """
    raw = _open_isp().load_sheet_by_name("Seasonal ratings", header_row=None).to_polars()
    new_entrant = _read_sheet_block(raw, 10, 11, 32).with_columns(
        pl.lit("new_entrant").alias("generator_group")
    )
    existing = _read_sheet_block(raw, 43, 44, 687).with_columns(
        pl.lit("existing_committed_anticipated").alias("generator_group")
    )
    return pl.concat([new_entrant, existing], how="diagonal_relaxed")  # type: ignore


@check_types
def maintenance() -> DataFrame[schemas.Maintenance]:
    """Planned maintenance rates (% of year) by generator type. Footnotes (rows 23+) are excluded.

    The sheet has a side technology-type lookup in cols 5-7; only the main cols 0-2
    (Generator type, Proportion of time out, Equivalent average days) are returned.
    Use maintenance_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Maintenance", header_row=None).to_polars()
    df = _read_sheet_block(raw, 5, 6, 23)
    return df.select(df.columns[:3])  # type: ignore


@check_types
def maintenance_by_technology() -> DataFrame[schemas.MaintenanceByTechnology]:
    """Technology-type maintenance rate defaults from the 'Maintenance' sheet.

    Side table (26 rows): Technology Type, Proportion of time out (%), Equivalent average days.
    """
    raw = _open_isp().load_sheet_by_name("Maintenance", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 5, 6, 32)  # type: ignore


@check_types
def generator_reliability() -> DataFrame[schemas.GeneratorReliability]:
    return read_sheet("Generator Reliability Settings", 9)  # type: ignore


@check_types
def heat_rates() -> DataFrame[schemas.HeatRates]:
    """Heat rates (GJ/MWh) by generator. Footnotes (rows 651+) are excluded.

    The sheet has a side technology-level lookup in cols 6-7; only the main cols 0-3
    (IASR ID, Power Station, Technology, Heat rate) are returned.
    Use heat_rates_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Heat rates", header_row=None).to_polars()
    df = _read_sheet_block(raw, 7, 8, 651)
    return df.select(df.columns[:4])  # type: ignore


@check_types
def heat_rates_by_technology() -> DataFrame[schemas.HeatRatesByTechnology]:
    """Technology-level heat rate defaults from the 'Heat rates' sheet.

    Side table (23 rows): Technology, Heat rate (GJ/MWh).
    """
    raw = _open_isp().load_sheet_by_name("Heat rates", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[6:]), 7, 8, 31)  # type: ignore


@check_types
def auxiliary() -> DataFrame[schemas.Auxiliary]:
    """Auxiliary load (% of generation) by generator. Footnotes (rows 649+) are excluded.

    The sheet has a side generator-type lookup in cols 5-6; only the main cols 0-3
    (IASR ID, Power Station, Technology, Auxiliary load) are returned.
    Use auxiliary_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Auxiliary", header_row=None).to_polars()
    df = _read_sheet_block(raw, 5, 6, 649)
    return df.select(df.columns[:4])  # type: ignore


@check_types
def auxiliary_by_technology() -> DataFrame[schemas.AuxiliaryByTechnology]:
    """Generator-type auxiliary load defaults from the 'Auxiliary' sheet.

    Side table (23 rows): Generator, Auxiliary load (% of generation).
    """
    raw = _open_isp().load_sheet_by_name("Auxiliary", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 5, 6, 29)  # type: ignore


@check_types
def emissions_intensity() -> DataFrame[schemas.EmissionsIntensity]:
    """Scope 1 emissions intensity (kg/MWh as-gen) by generator. Footnotes (rows 655+) are excluded.

    The sheet has a side technology-level lookup in cols 5-6; only the main cols 0-3
    (IASR ID, Power Station, Technology, Scope 1 emissions intensity) are returned.
    Use emissions_intensity_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Emissions intensity", header_row=None).to_polars()
    df = _read_sheet_block(raw, 7, 8, 655)
    return df.select(df.columns[:4])  # type: ignore


@check_types
def emissions_intensity_by_technology() -> DataFrame[schemas.EmissionsIntensityByTechnology]:
    """Technology-level emissions intensity defaults from the 'Emissions intensity' sheet.

    Side table (21 rows): Technology, Scope 1 emissions intensity (kg/MWh as-gen).
    """
    raw = _open_isp().load_sheet_by_name("Emissions intensity", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 7, 8, 29)  # type: ignore


@check_types
def fixed_opex() -> DataFrame[schemas.FixedOpex]:
    """Fixed OPEX ($/kW/year) by generator. Footnotes (rows 650+) are excluded.

    The sheet has a side technology-type lookup in cols 5-7; only the main cols 0-3
    (IASR ID, Power Station, Technology, Fixed OPEX) are returned.
    Use fixed_opex_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Fixed OPEX", header_row=None).to_polars()
    df = _read_sheet_block(raw, 5, 6, 650)
    return df.select(df.columns[:4]).drop_nulls()  # type: ignore


@check_types
def fixed_opex_by_technology() -> DataFrame[schemas.FixedOpexByTechnology]:
    """Technology-type fixed OPEX defaults from the 'Fixed OPEX' sheet.

    Side table (26 rows): Technology Type, Base value ($/kW/year), Unit.
    """
    raw = _open_isp().load_sheet_by_name("Fixed OPEX", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 5, 6, 32)  # type: ignore


@check_types
def variable_opex() -> DataFrame[schemas.VariableOpex]:
    """Variable OPEX ($/MWh sent out) by generator. Footnotes (rows 650+) are excluded.

    The sheet has a side generator-type lookup in cols 5-6; only the main cols 0-3
    (IASR ID, Power Station/Technology, Technology, Variable OPEX) are returned.
    Use variable_opex_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Variable OPEX", header_row=None).to_polars()
    df = _read_sheet_block(raw, 5, 6, 650)
    return (
        df.select(df.columns[:4])
        .rename({"variable_opex_aud_mwh_sent_out1": "variable_opex_aud_mwh_sent_out"})
        .drop_nulls()
    )  # type: ignore


@check_types
def variable_opex_by_technology() -> DataFrame[schemas.VariableOpexByTechnology]:
    """Generator-type variable OPEX defaults from the 'Variable OPEX' sheet.

    Side table (25 rows): Generator, Base value.
    """
    raw = _open_isp().load_sheet_by_name("Variable OPEX", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 5, 6, 31)  # type: ignore


@check_types
def affine_heat_rates() -> DataFrame[schemas.AffineHeatRates]:
    """Affine heat rate parameters (no-load heat, slope). Footnotes (rows 188+) are excluded.

    The sheet has a side technology-level lookup in cols 6-9; only the main cols 0-4
    (IASR ID, Power Station, Technology Type, No load heat input, Marginal heat rate) are returned.
    Use affine_heat_rates_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Affine Heat rates", header_row=None).to_polars()
    df = _read_sheet_block(raw, 6, 7, 188)
    return df.select(df.columns[:5])  # type: ignore


@check_types
def affine_heat_rates_by_technology() -> DataFrame[schemas.AffineHeatRatesByTechnology]:
    """Technology-level affine heat rate defaults from the 'Affine Heat rates' sheet.

    Side table (22 rows): Technology, Unit nominal capacity (MW), No load heat input (GJ/h),
    Marginal heat rate (GJ/MWh).
    """
    raw = _open_isp().load_sheet_by_name("Affine Heat rates", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[6:]), 6, 7, 29).with_columns(
        pl.exclude(
            "technology",
        ).cast(float, strict=False)
    )  # type: ignore


@check_types
def max_ramp_rates() -> DataFrame[schemas.MaxRampRates]:
    """Maximum ramp rates (MW/min) by generator.

    The sheet has a side technology-level lookup in cols 6-8; only the main cols 0-4
    (IASR ID, Power Station, Technology Type, Max Ramp Up, Max Ramp Down) are returned.
    Use max_ramp_rates_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("Max Ramp Rates", header_row=None).to_polars()
    df = _read_sheet_block(raw, 7, 8, 189)
    return df.select(df.columns[:5]).with_columns(
        pl.col("max_ramp_up_mw_min").cast(pl.Float64, strict=False),
        pl.col("max_ramp_down_mw_min").cast(pl.Float64, strict=False),
    )  # type: ignore


@check_types
def max_ramp_rates_by_technology() -> DataFrame[schemas.MaxRampRatesByTechnology]:
    """Technology-level ramp rate defaults from the 'Max Ramp Rates' sheet.

    Side table (22 rows): Technology, Max Ramp Up (MW/min), Max Ramp Down (MW/min).
    """
    raw = _open_isp().load_sheet_by_name("Max Ramp Rates", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[6:]), 7, 8, 30).with_columns(
        pl.exclude(
            "technology",
        ).cast(float, strict=False)
    )  # type: ignore


@check_types
def coal_min_stable_level() -> DataFrame[schemas.CoalMinStableLevel]:
    """Coal generator minimum stable levels (MW) by unit. Footnotes (rows 58+) are excluded.

    The sheet has compound headers: row 11 is the main header, row 12 provides sub-labels
    for the three MW value columns:
    - 'IASR 2023 (Backcasting)': backcasted 2023 value
    - 'Typical Lowest Band': typical lowest operating band
    - 'Minimum Continuous Operating Level': minimum continuous operating level
    """
    raw = _open_isp().load_sheet_by_name("Coal Min Stable Level", header_row=None).to_polars()
    row11 = list(raw.row(11))
    row12 = list(raw.row(12))
    col_names = []
    for i, (h, sub) in enumerate(zip(row11, row12, strict=True)):
        if sub is not None:
            col_names.append(str(sub))
        elif h is not None:
            col_names.append(str(h))
        else:
            col_names.append(f"_col{i}")
    col_names = _unique_headers(tuple(col_names))
    data = raw.slice(13, 58 - 13)  # rows 13-57, before footnotes
    data.columns = col_names
    data = data.pipe(_normalize_columns)
    return data.filter(pl.col("iasr_id").is_not_null()).with_columns(
        pl.exclude(
            "iasr_id",
            "power_station",
            "technology_type",
        ).cast(float)
    )  # type: ignore


@check_types
def gpg_min_stable_level() -> DataFrame[schemas.GpgMinStableLevel]:
    """GPG minimum stable levels (MW) by unit.

    The sheet has a side technology-level lookup in cols 5-6; only the main cols 0-3
    (IASR ID, Power Station, Technology Type, Min Stable Level) are returned.
    Use gpg_min_stable_level_by_technology() for the side table.
    """
    raw = _open_isp().load_sheet_by_name("GPG Min Stable Level", header_row=None).to_polars()
    df = _read_sheet_block(raw, 10, 11, 148)
    return df.select(df.columns[:4]).with_columns(pl.col("min_stable_level_mw").cast(float))  # type: ignore


@check_types
def gpg_min_stable_level_by_technology() -> DataFrame[schemas.GpgMinStableLevelByTechnology]:
    """Technology-level minimum stable level defaults (% of nameplate) from the 'GPG Min Stable Level' sheet.

    Side table (24 rows): Technology, Min Stable Level (% of nameplate).
    """
    raw = _open_isp().load_sheet_by_name("GPG Min Stable Level", header_row=None).to_polars()
    return _read_sheet_block(raw.select(raw.columns[5:]), 10, 11, 35).with_columns(
        pl.col("min_stable_level_of_nameplate").cast(float)
    )  # type: ignore


@check_types
def capacity_factors() -> DataFrame[schemas.CapacityFactors]:
    return read_timeseries(
        "Capacity Factors ", 6
    )  # note trailing space in sheet name  # type: ignore


@check_types
def hydro_scheme_inflows() -> DataFrame[schemas.HydroSchemeInflows]:
    """Hydro scheme monthly inflows by reference year. Trailing blank columns are excluded."""
    df = read_sheet("Hydro Scheme Inflows", 9)
    df = df.select(df.columns[:14])
    # Keep only actual data rows: those where reference_year_fye starts with a digit (year)
    return df.filter(pl.col("reference_year_fye").str.contains(r"^\d"))  # type: ignore


# ── Costs / finance ───────────────────────────────────────────────────────────


@check_types
def lead_time_project_life() -> DataFrame[schemas.LeadTimeProjectLife]:
    df = read_sheet("Lead time and project life", 5)
    return df.rename(
        {
            "lead_time_for_development_years1_2": "lead_time_for_development_years",
            "lead_time_years3": "lead_time_years",
            "total_lead_time_years47": "total_lead_time_years",
            "economic_life_years5": "economic_life_years",
            "technical_life_years6": "technical_life_years",
        }
    )  # type: ignore


@check_types
def financial_parameters() -> DataFrame[schemas.FinancialParameters]:
    """Financial parameters (discount rate and WACC by technology/scenario).

    The first column (parameter/technology label) has no header; it is returned as 'parameter'.
    The trailing blank column is excluded.
    """
    df = read_sheet("Financial parameters", 5)
    return df.drop("unnamed_5").rename({"unnamed_0": "parameter"})  # type: ignore


@check_types
def locational_cost_factors() -> DataFrame[schemas.LocationalCostFactors]:
    """Locational cost factors for non-PHES technologies by cost zone / REZ."""
    raw = _open_isp().load_sheet_by_name("Locational Cost Factors", header_row=None).to_polars()
    df = _read_sheet_block(raw, 9, 10, 74)
    return df.select(df.columns[:7]).rename({"o&m_costs_3": "om_costs"})  # type: ignore


@check_types
def locational_cost_factors_phes() -> DataFrame[schemas.LocationalCostFactorsPhes]:
    """Locational cost factors for pumped hydro energy storage (PHES) by subregion and duration."""
    raw = _open_isp().load_sheet_by_name("Locational Cost Factors", header_row=None).to_polars()
    df = _read_sheet_block(raw, 82, 83, 128)
    return df.rename({"o&m_costs_2": "om_costs"})  # type: ignore


@check_types
def locational_cost_factors_technology_specific() -> DataFrame[
    schemas.LocationalCostFactorsTechnologySpecific
]:
    """Technology-specific locational cost factors by cost zone / REZ and technology."""
    raw = _open_isp().load_sheet_by_name("Locational Cost Factors", header_row=None).to_polars()
    df = _read_sheet_block(raw, 160, 161, 225)
    _not_applicable = ["Not Applicable", "Not applicable", "N/A"]
    return df.with_columns(pl.col(pl.String).replace(_not_applicable, None))  # type: ignore


@check_types
def connection_cost() -> DataFrame[schemas.ConnectionCost]:
    """Connection costs ($/kW) for REZ projects by technology and REZ ID.

    Footnotes and the other-generation table are excluded.
    Use connection_cost_other_generation() for non-REZ generator costs.
    """
    raw = _open_isp().load_sheet_by_name("Connection cost", header_row=None).to_polars()
    return _read_sheet_block(raw, 6, 7, 56)  # type: ignore


@check_types
def connection_cost_other_generation() -> DataFrame[schemas.ConnectionCostOtherGeneration]:
    """Connection costs ($/kW) for non-REZ generation by generator type and region.

    The table is stored wide (generator-type rows x region columns) and is
    returned in long format with a 'region' column.
    """
    raw = _open_isp().load_sheet_by_name("Connection cost", header_row=None).to_polars()
    data = _read_sheet_block(raw, 61, 63, 68)
    id_cols = [data.columns[0]]
    region_cols = [c for c in data.columns[1:] if not c.startswith("_col")]
    return data.unpivot(
        index=id_cols, on=region_cols, variable_name="generator_type", value_name="cost_aud_kw"
    )  # type: ignore


@check_types
def gas_system_properties() -> DataFrame[schemas.GasSystemProperties]:
    """Gas system properties by facility. Trailing blank columns are excluded."""
    df = read_sheet("Gas System Properties", 10)
    return df.select(df.columns[:5])  # type: ignore


# ── Network ───────────────────────────────────────────────────────────────────


@check_types
def renewable_energy_zones() -> DataFrame[schemas.RenewableEnergyZones]:
    """Renewable Energy Zones reference table. Trailing blank columns are excluded."""
    df = read_sheet("Renewable energy zones", 5)
    return df.select(df.columns[:4])  # type: ignore


@check_types
def network_capability() -> DataFrame[schemas.NetworkCapability]:
    """Transfer capability (MW) by flow path/season/direction.

    Compound column headers are built from two header rows (section label in
    row 5, sub-label in row 6). Two sub-tables are concatenated with a
    'flow_path_type' discriminator:
    - 'sub_regional': rows 7-24
    - 'interconnector': rows 35-41
    """
    COLS = [
        "Flow Paths",
        "Forward direction capability approximation (MW) - Notes 1,2&3 (Peak demand)",
        "Forward direction capability approximation (MW) - Notes 1,2&3 (Summer typical)",
        "Forward direction capability approximation (MW) - Notes 1,2&3 (Winter refernce)",
        "Reverse direction capability approximation (MW) - Notes 1,2&3 (Peak demand)",
        "Reverse direction capability approximation (MW) - Notes 1,2&3 (Summer typical)",
        "Reverse direction capability approximation (MW) - Notes 1,2&3 (Winter refernce)",
        "Dominant constraints (Forward direction)",
        "Dominant constraints (Reverse direction)",
        "Notes",
    ]
    raw = _open_isp().load_sheet_by_name("Network capability", header_row=None).to_polars()
    row5 = list(raw.row(5))
    row6 = list(raw.row(6))
    raw_names: list[str] = []
    current_section = ""
    for i, (s, sub) in enumerate(zip(row5, row6, strict=True)):
        if s is not None:
            current_section = str(s)
        sub_str = str(sub) if sub is not None else ""
        if sub_str:
            raw_names.append(f"{current_section} ({sub_str})" if current_section else sub_str)
        else:
            raw_names.append(current_section if current_section else f"_col{i}")
    col_names = _unique_headers(tuple(raw_names))

    def _extract(data_start: int, data_end: int, label: str) -> pl.DataFrame:
        block = raw.slice(data_start, data_end - data_start)
        block.columns = col_names
        block = block.filter(pl.col("Flow Paths").is_not_null())
        return block.select([c for c in COLS if c in block.columns]).with_columns(
            pl.lit(label).alias("flow_path_type")
        )

    sub_regional = _extract(7, 25, "sub_regional")
    interconnector = _extract(35, 42, "interconnector")
    return (
        pl.concat([sub_regional, interconnector], how="diagonal_relaxed")
        .pipe(_normalize_columns)
        .rename(
            {
                "forward_direction_capability_approximation_mw_notes_12&3_peak_demand": "forward_peak_demand_mw",
                "forward_direction_capability_approximation_mw_notes_12&3_summer_typical": "forward_summer_typical_mw",
                "forward_direction_capability_approximation_mw_notes_12&3_winter_refernce": "forward_winter_reference_mw",
                "reverse_direction_capability_approximation_mw_notes_12&3_peak_demand": "reverse_peak_demand_mw",
                "reverse_direction_capability_approximation_mw_notes_12&3_summer_typical": "reverse_summer_typical_mw",
                "reverse_direction_capability_approximation_mw_notes_12&3_winter_refernce": "reverse_winter_reference_mw",
                "dominant_constraints_forward_direction": "dominant_constraints_forward",
                "dominant_constraints_reverse_direction": "dominant_constraints_reverse",
            }
        )
    )  # type: ignore


@check_types
def network_losses() -> DataFrame[schemas.NetworkLosses]:
    """Loss factors by flow path/direction.

    Three stacked tables, distinguished by 'flow_path_type':
    - 'existing': rows 7-23
    - 'committed_anticipated': rows 33-34
    - 'development_option': rows 39-87
    """
    raw = _open_isp().load_sheet_by_name("Network losses", header_row=None).to_polars()
    existing = _read_sheet_block(raw, 6, 7, 24).with_columns(
        pl.lit("existing").alias("flow_path_type")
    )
    committed = _read_sheet_block(raw, 32, 33, 35).with_columns(
        pl.lit("committed_anticipated").alias("flow_path_type")
    )
    development = _read_sheet_block(raw, 38, 39, 88).with_columns(
        pl.lit("development_option").alias("flow_path_type")
    )
    return pl.concat([existing, committed, development], how="diagonal_relaxed")  # type: ignore


@check_types
def transmission_reliability() -> DataFrame[schemas.TransmissionReliability]:
    return read_sheet("Transmission Reliability", 6)  # type: ignore


@check_types
def flow_path_augmentation() -> DataFrame[schemas.FlowPathAugmentation]:
    """Flow path augmentation options with transfer level increase (MW) by direction.

    The sheet has compound sub-headers: the 'Notional transfer level increase' column
    splits into 'Forward direction' and 'Reverse direction' sub-headers (row 12).
    Repeated column-header rows and sub-header rows are filtered out, keeping only
    rows where 'Development path' is a real path name.
    """
    raw = (
        _open_isp()
        .load_sheet_by_name("Flow path augmentation options", header_row=None)
        .to_polars()
    )
    row11 = list(raw.row(11))
    row12 = list(raw.row(12))
    col_names: list[str] = []
    current_section = ""
    for i, (h, sub) in enumerate(zip(row11, row12, strict=True)):
        if h is not None:
            current_section = str(h)
        if h is not None and sub is not None:
            col_names.append(f"{current_section} ({sub})")
        elif h is not None:
            col_names.append(current_section)
        elif sub is not None:
            col_names.append(f"{current_section} ({sub})" if current_section else str(sub))
        else:
            col_names.append(f"_col{i}")
    col_names = _unique_headers(tuple(col_names))
    # Data starts at row 13 (skip main header row 11 and sub-header row 12)
    data = raw.slice(13)
    data.columns = col_names
    # Normalize column names first so we can filter on the normalized name
    data = data.pipe(_normalize_columns)
    # Filter: keep only rows with a real Development path value (drops sub-headers,
    # repeated header rows, section-label rows, and all-null separators)
    data = data.filter(
        pl.col("development_path").is_not_null()
        & (pl.col("development_path") != "development_path")
    )
    # Rename long compound column names to short schema-friendly names
    _fwd_long = next(
        c
        for c in data.columns
        if c.startswith("notional_transfer") and c.endswith("forward_direction")
    )
    _rev_long = next(
        c
        for c in data.columns
        if c.startswith("notional_transfer") and c.endswith("reverse_direction")
    )
    return data.rename(
        {
            _fwd_long: "notional_transfer_level_increase_forward_mw",
            _rev_long: "notional_transfer_level_increase_reverse_mw",
            "indicative_cost_estimate_aud2025_aud_million": "indicative_cost_estimate_2025_million",
        }
    )  # type: ignore


@check_types
def rez_augmentations() -> DataFrame[schemas.RezAugmentations]:
    return read_sheet("REZ augmentations options", 10)  # type: ignore


@functools.cache
def _build_limits_rez_raw() -> pl.DataFrame:
    return _open_isp().load_sheet_by_name("Build limits - REZs", header_row=None).to_polars()


@check_types
def build_limits_rez() -> DataFrame[schemas.BuildLimitsRez]:
    """Initial resource limits (wind, solar, land use) by REZ."""
    raw = _build_limits_rez_raw()
    return _read_multiheader_block(raw.slice(0, 58), header_rows=[5, 6], data_start=7)  # type: ignore


@check_types
def build_limits_rez_transmission() -> DataFrame[schemas.BuildLimitsRezTransmission]:
    """Initial transmission limits (network limits, expansion costs) by REZ."""
    raw = _build_limits_rez_raw()
    return _read_multiheader_block(raw.slice(0, 112), header_rows=[63, 64], data_start=65)  # type: ignore


@check_types
def build_limits_rez_modifiers() -> DataFrame[schemas.BuildLimitsRezModifiers]:
    """REZ transmission limit modifiers from committed/anticipated augmentations."""
    raw = _build_limits_rez_raw()
    return _read_sheet_block(raw, 119, 120, 128)  # type: ignore


@check_types
def build_limits_rez_group_constraints() -> DataFrame[schemas.RezGroupConstraints]:
    """REZ group constraint summary (transmission-limited build limits)."""
    raw = _build_limits_rez_raw()
    df = _read_multiheader_block(raw.slice(0, 216), header_rows=[133, 134], data_start=135)
    return df.with_columns(pl.col(pl.String).replace("-", None))  # type: ignore


@check_types
def build_limits_rez_transmission_limit_constraints() -> DataFrame[schemas.RezGroupConstraints]:
    """REZ transmission limit constraint summary."""
    raw = _build_limits_rez_raw()
    df = _read_multiheader_block(raw.slice(0, 239), header_rows=[219, 220], data_start=221)
    return df.with_columns(pl.col(pl.String).replace("-", None))  # type: ignore


@check_types
def build_limits_rez_secondary_transmission_limits() -> DataFrame[schemas.RezGroupConstraints]:
    """REZ secondary transmission limit summary."""
    raw = _build_limits_rez_raw()
    df = _read_multiheader_block(raw.slice(0, 257), header_rows=[242, 243], data_start=244)
    return df.with_columns(pl.col(pl.String).replace("-", None))  # type: ignore


@check_types
def build_limits_phes() -> DataFrame[schemas.BuildLimitsPhes]:
    """Pumped hydro build limits by site. Trailing blank columns are excluded."""
    df = read_sheet("Build limits - PHES", 7)
    return df.select(df.columns[:4])  # type: ignore


# ── Retirement — main table (cols 0-5) + side table (cols 7-8) ───────────────


@check_types
def retirement() -> DataFrame[schemas.Retirement]:
    raw = _open_isp().load_sheet_by_name("Retirement", header_row=None).to_polars()
    header_row = 12
    header = _unique_headers(raw.row(header_row))
    data = raw.slice(header_row + 1)
    data.columns = header
    data = data.select(data.columns[:6]).pipe(_normalize_columns)
    return data.filter(pl.col(data.columns[0]).is_not_null())  # type: ignore


@check_types
def retirement_costs() -> DataFrame[schemas.RetirementCosts]:
    """Technology-level retirement cost lookup ($/MW). Side table on 'Retirement' sheet."""
    raw = _open_isp().load_sheet_by_name("Retirement", header_row=None).to_polars()
    header_row = 12
    header = _unique_headers(raw.row(header_row))
    data = raw.slice(header_row + 1)
    data.columns = header
    side = data.select([data.columns[7], data.columns[8]])
    side.columns = ["technology_type", "retirement_mw"]
    return side.pipe(_normalize_columns).filter(pl.col("technology_type").is_not_null())  # type: ignore


# ── Marginal Loss Factors — existing (cols 0-11) + new entrant (cols 12-17) ──


@check_types
def marginal_loss_factors() -> DataFrame[schemas.MarginalLossFactors]:
    """MLFs for existing, new entrant, and new entrant electrolyser generators.

    Three groups sit side-by-side in the sheet. Each is extracted separately
    and concatenated with a 'generator_group' label column.
    """
    raw = _open_isp().load_sheet_by_name("Marginal Loss Factors", header_row=None).to_polars()
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
    return (
        pl.concat([existing, new_entrant, electrolysers], how="diagonal_relaxed")
        .pipe(_normalize_columns)
        .with_columns(pl.col("mlf").cast(pl.Float64, strict=False))
    )  # type: ignore


# ── Build / capital costs ─────────────────────────────────────────────────────


@check_types
def build_costs() -> DataFrame[schemas.BuildCosts]:
    return (
        read_timeseries("Build costs", 9)
        .drop_nulls("gencost_scenario")
        .rename({"technology1": "technology"})
    )  # type: ignore


@check_types
def connection_cost_forecasts_wind_and_solar() -> DataFrame[
    schemas.ConnectionCostForecastsWindSolar
]:
    """Connection cost forecasts (real 2025 $) by technology/scenario/year.

    The sheet holds two stacked tables with different key columns:
    - 'wind_solar': keyed by REZ ID and REZ names
    - 'other_generation': keyed by Generator Type and Region

    A 'cost_type' column distinguishes them. The Notes column (all null for
    data rows) is dropped. Rows with null first key column (blank region
    separators) are filtered out by _parse_timeseries_block.
    """
    raw = _open_isp().load_sheet_by_name("Connection cost forecasts", header_row=None).to_polars()
    rez = (
        _parse_timeseries_block(raw, 8, 9, 141)
        .drop("notes")
        .with_columns(pl.lit("wind_solar").alias("cost_type"))
    )
    return rez


@check_types
def connection_cost_forecasts_other() -> DataFrame[schemas.ConnectionCostForecastsOther]:
    """Connection cost forecasts (real 2025 $) by technology/scenario/year.

    The sheet holds two stacked tables with different key columns:
    - 'wind_solar': keyed by REZ ID and REZ names
    - 'other_generation': keyed by Generator Type and Region

    A 'cost_type' column distinguishes them. The Notes column (all null for
    data rows) is dropped. Rows with null first key column (blank region
    separators) are filtered out by _parse_timeseries_block.
    """
    raw = _open_isp().load_sheet_by_name("Connection cost forecasts", header_row=None).to_polars()
    other = (
        _parse_timeseries_block(raw, 144, 145, 385)
        .drop("notes")
        .with_columns(pl.lit("other_generation").alias("cost_type"))
    )
    return other


@check_types
def flow_path_cost_forecasts() -> DataFrame[schemas.FlowPathCostForecasts]:
    return read_timeseries("Flow path cost forecasts", 11)  # type: ignore


@check_types
def rez_cost_forecasts() -> DataFrame[schemas.RezCostForecasts]:
    return read_timeseries("REZ cost forecasts", 12)  # type: ignore


@check_types
def distribution_cost_forecasts() -> DataFrame[schemas.DistributionCostForecasts]:
    return read_timeseries("Distribution cost forecasts", 5)  # type: ignore


@check_types
def build_cost_hydrogen_pipeline() -> DataFrame[schemas.BuildCostHydrogenPipeline]:
    return read_timeseries("Build Cost - Hydrogen pipeline", 5)  # type: ignore


# ── Fuel prices ───────────────────────────────────────────────────────────────


@check_types
def coal_biomass_price() -> DataFrame[schemas.CoalBiomassPrice]:
    """Coal and biomass fuel prices ($/GJ) by generator/scenario/year.

    The sheet holds two stacked tables. A 'fuel_type' column distinguishes them:
    - 'coal': per-generator prices (rows 9-53)
    - 'biomass': single biomass price (rows 58-60)

    Both tables share the same column structure (Generator, Scenario, years).
    """
    raw = _open_isp().load_sheet_by_name("Coal and Biomass price", header_row=None).to_polars()
    coal = _parse_timeseries_block(raw, 8, 9, 54).with_columns(pl.lit("coal").alias("fuel_type"))
    # Rename the biomass header col ("biomass_price") to match the coal schema
    biomass = (
        _parse_timeseries_block(raw, 57, 58, 61)
        .rename({"biomass_price": "generator"})
        .with_columns(pl.lit("biomass").alias("fuel_type"))
    )
    return pl.concat([coal, biomass], how="diagonal_relaxed")  # type: ignore


@check_types
def gas_liquid_h2_price() -> DataFrame[schemas.GasLiquidH2Price]:
    """Gas, liquid fuel, and H2/biomethane prices ($/GJ) by entity/scenario/year.

    Eight sub-tables are concatenated with a 'price_category' discriminator.
    The model-inclusion flag column (0.0/1.0 in col 0) is dropped.

    Categories:
    - 'gas_existing':          existing generator gas prices (rows 9-125)
    - 'gas_new_entrant':       new entrant gas prices (rows 131-160)
    - 'gas_industrial':        industrial gas costs by state (rows 168-185)
    - 'gas_residential':       residential/commercial gas costs by state (rows 193-210)
    - 'liquid_existing':       liquid fuel prices for existing generators (rows 218-238)
    - 'liquid_gpg_secondary':  GPG secondary liquid fuel prices (rows 246-362)
    - 'hydrogen':              hydrogen prices (rows 369-371)
    - 'biomethane':            biomethane prices (rows 376-384)
    """
    raw = _open_isp().load_sheet_by_name("Gas, Liquid fuel, H2 price", header_row=None).to_polars()
    BLOCKS = [
        ("gas_existing", 8, 9, 126),
        ("gas_new_entrant", 130, 131, 161),
        ("gas_industrial", 167, 168, 186),
        ("gas_residential", 192, 193, 211),
        ("liquid_existing", 217, 218, 239),
        ("liquid_gpg_secondary", 245, 246, 363),
        ("hydrogen", 368, 369, 372),
        ("biomethane", 375, 376, 385),
    ]
    parts = []
    for category, header_row, data_start, data_end in BLOCKS:
        block = _parse_timeseries_block(raw, header_row, data_start, data_end)
        # Drop col 0 (model-inclusion flag: 0.0 or 1.0); rename entity/scenario
        # cols positionally — each sub-table uses different header text
        block = block.select(block.columns[1:])
        entity_col, scenario_col = block.columns[0], block.columns[1]
        block = block.rename(
            {
                entity_col: "generator",
                scenario_col: "price_scenario",
            }
        ).with_columns(pl.lit(category).alias("price_category"))
        parts.append(block)
    return pl.concat(parts, how="vertical")  # type: ignore


@check_types
def gpg_emissions_reduction() -> DataFrame[schemas.GpgEmissionsReduction]:
    """GPG emissions reduction factor (fraction) by scenario/year due to biomethane blending.

    The footnote row (row 11) is excluded. The unnamed scenario column is
    renamed to 'scenario'.
    """
    raw = (
        _open_isp()
        .load_sheet_by_name("GPG emissions reduction - BioM", header_row=None)
        .to_polars()
    )
    return _parse_timeseries_block(raw, 7, 8, 11).rename({"col0": "scenario"})  # type: ignore


# ── DER / demand forecasts (Scenario col already embedded in data) ────────────


@check_types
def rooftop_pv() -> DataFrame[schemas.RooftopPv]:
    """Rooftop PV capacity (MW degraded) and energy (GWh) by subregion/scenario/year.

    The sheet holds two stacked tables. A 'metric' column distinguishes them:
    'capacity_mw' and 'energy_gwh'.
    """
    raw = _open_isp().load_sheet_by_name("Rooftop PV", header_row=None).to_polars()
    capacity = _parse_timeseries_block(raw, 14, 15, 63).with_columns(
        pl.lit("capacity_mw").alias("metric")
    )
    energy = _parse_timeseries_block(raw, 67, 68, 116).with_columns(
        pl.lit("energy_gwh").alias("metric")
    )
    return pl.concat([capacity, energy], how="diagonal_relaxed")  # type: ignore


@check_types
def pvnsg() -> DataFrame[schemas.Pvnsg]:
    """PVNSG capacity (MW degraded) and energy (GWh) by subregion/scenario/year.

    The sheet holds two stacked tables. A 'metric' column distinguishes them:
    'capacity_mw' and 'energy_gwh'.
    """
    raw = _open_isp().load_sheet_by_name("PVNSG", header_row=None).to_polars()
    capacity = _parse_timeseries_block(raw, 14, 15, 63).with_columns(
        pl.lit("capacity_mw").alias("metric")
    )
    energy = _parse_timeseries_block(raw, 67, 68, 116).with_columns(
        pl.lit("energy_gwh").alias("metric")
    )
    return pl.concat([capacity, energy], how="diagonal_relaxed")  # type: ignore


@check_types
def onsg() -> DataFrame[schemas.Onsg]:
    """Other Non-Scheduled Generators installed capacity (MW) by scenario/year.

    Returns both sub-regional and regional granularities in a single DataFrame.
    A 'granularity' column ('subregional' / 'regional') distinguishes the two
    stacked tables, which have different key columns:
    - subregional: Subregion, Region, Scenario
    - regional:    Region, Scenario  (Subregion will be null)
    """
    raw = _open_isp().load_sheet_by_name("ONSG", header_row=None).to_polars()
    subregional = _parse_timeseries_block(raw, 9, 10, 55).with_columns(
        pl.lit("subregional").alias("granularity")
    )
    regional = _parse_timeseries_block(raw, 58, 59, 74).with_columns(
        pl.lit("regional").alias("granularity")
    )
    return pl.concat([subregional, regional], how="diagonal_relaxed")  # type: ignore


@check_types
def battery_plugin_evs() -> DataFrame[schemas.BatteryPluginEvs]:
    """Aggregated BEV and PHEV energy consumption (GWh) and uptake (vehicle count)
    by region/subregion/scenario/year.

    A 'metric' column distinguishes the two stacked tables:
    'energy_gwh' and 'uptake_vehicles'.
    """
    raw = _open_isp().load_sheet_by_name("Battery & Plug-in EVs", header_row=None).to_polars()
    energy = _parse_timeseries_block(raw, 13, 14, 62).with_columns(
        pl.lit("energy_gwh").alias("metric")
    )
    uptake = _parse_timeseries_block(raw, 66, 67, 115).with_columns(
        pl.lit("uptake_vehicles").alias("metric")
    )
    return pl.concat([energy, uptake], how="diagonal_relaxed")  # type: ignore


@check_types
def fuel_cell_evs() -> DataFrame[schemas.FuelCellEvs]:
    return read_timeseries("Fuel cell EVs", 13)  # type: ignore


@check_types
def ev_v2g() -> DataFrame[schemas.EvV2g]:
    """EV Vehicle-to-Grid battery capacity (MW) and depth (MWh) by subregion/scenario/year.

    A 'metric' column distinguishes the two stacked tables:
    'capacity_mw' and 'depth_mwh'.
    """
    raw = _open_isp().load_sheet_by_name("EV V2G", header_row=None).to_polars()
    capacity = _parse_timeseries_block(raw, 13, 14, 62).with_columns(
        pl.lit("capacity_mw").alias("metric")
    )
    depth = _parse_timeseries_block(raw, 66, 67, 115).with_columns(
        pl.lit("depth_mwh").alias("metric")
    )
    return pl.concat([capacity, depth], how="diagonal_relaxed")  # type: ignore


@check_types
def dsp() -> DataFrame[schemas.Dsp]:
    """Demand Side Participation (MW) by region/price-band/scenario/season/year.

    The sheet has Summer and Winter tables stacked. Both use the same column
    structure; however the Winter header row erroneously uses calendar years
    (2026, 2027, ...) instead of FY format (2025-26, 2026-27, ...). Both
    blocks are therefore read using the Summer header (row 8) to ensure
    consistent year labels throughout.
    """
    raw = _open_isp().load_sheet_by_name("DSP", header_row=None).to_polars()
    summer = _parse_timeseries_block(raw, 8, 9, 84)
    winter = _parse_timeseries_block(raw, 8, 89, 164)
    return pl.concat([summer, winter], how="diagonal_relaxed")  # type: ignore


@check_types
def embedded_energy_storages() -> DataFrame[schemas.EmbeddedEnergyStorages]:
    """Embedded (small-scale) battery storage capacity (MW) and degraded energy (MWh)
    by subregion/scenario/year.

    A 'metric' column distinguishes the two stacked tables:
    'capacity_mw' and 'energy_mwh_degraded'.
    """
    raw = _open_isp().load_sheet_by_name("Embedded energy storages", header_row=None).to_polars()
    capacity = _parse_timeseries_block(raw, 13, 14, 62).with_columns(
        pl.lit("capacity_mw").alias("metric")
    )
    energy = _parse_timeseries_block(raw, 67, 68, 116).with_columns(
        pl.lit("energy_mwh_degraded").alias("metric")
    )
    return pl.concat([capacity, energy], how="diagonal_relaxed")  # type: ignore


@check_types
def aggregated_energy_storages() -> DataFrame[schemas.AggregatedEnergyStorages]:
    """Aggregated (VPP) battery storage capacity (MW) and degraded energy (MWh)
    by subregion/scenario/year.

    A 'metric' column distinguishes the two stacked tables:
    'capacity_mw' and 'energy_mwh_degraded'.
    """
    raw = _open_isp().load_sheet_by_name("Aggregated energy storages", header_row=None).to_polars()
    capacity = _parse_timeseries_block(raw, 12, 13, 61).with_columns(
        pl.lit("capacity_mw").alias("metric")
    )
    energy = _parse_timeseries_block(raw, 66, 67, 115).with_columns(
        pl.lit("energy_mwh_degraded").alias("metric")
    )
    return pl.concat([capacity, energy], how="diagonal_relaxed")  # type: ignore


@check_types
def elec_retail_price_indices() -> DataFrame[schemas.ElecRetailPriceIndices]:
    """Electricity retail price index by scenario/year. Footnote row (row 12) is excluded.

    The scenario column (col 0) has no header in the sheet; it is returned as 'scenario'.
    """
    raw = _open_isp().load_sheet_by_name("Elec. Retail Price Indices", header_row=None).to_polars()
    return _parse_timeseries_block(raw, 8, 9, 12).rename({"col0": "scenario"})  # type: ignore


@check_types
def hydrogen_demand_domestic() -> DataFrame[schemas.HydrogenDemandDomestic]:
    return read_timeseries("Hydrogen demand - Domestic", 7)  # type: ignore


@check_types
def hydrogen_demand_export() -> DataFrame[schemas.HydrogenDemandExport]:
    return read_timeseries("Hydrogen demand-Export&Commod", 6)  # type: ignore


def _stacked_blocks(sheet_name: str, blocks: list[tuple]) -> pl.DataFrame:
    """Extract stacked multi-scenario blocks (Pattern F sheets).

    Each block: (scenario, year_row, data_start, data_end_exclusive, key_col_name)
    Year row has [None-or-label, year1, year2, ...] — col0 may be ignored.
    """
    raw = _open_isp().load_sheet_by_name(sheet_name, header_row=None).to_polars()
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
    return pl.DataFrame(parts).pipe(_parse_year_columns).pipe(_normalize_columns)


@check_types
def data_centre_forecasts() -> DataFrame[schemas.DataCentreForecasts]:
    """Data centre electricity demand by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Data Centre Forecasts",
        [
            ("Slower Growth", 10, 11, 16, "state"),
            ("Step Change", 18, 19, 24, "state"),
            ("Accelerated Transition", 26, 27, 32, "state"),
        ],
    )  # type: ignore


@check_types
def electrification() -> DataFrame[schemas.Electrification]:
    """Electrification of end-use energy by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Electrification",
        [
            ("Slower Growth", 12, 13, 19, "state"),
            ("Step Change", 21, 22, 28, "state"),
            ("Accelerated Transition", 30, 31, 37, "state"),
        ],
    )  # type: ignore


@check_types
def appliance_uptake() -> DataFrame[schemas.ApplianceUptake]:
    """Residential appliance uptake by scenario/state/year (TWh)."""
    return _stacked_blocks(
        "Appliance Uptake Forecasts",
        [
            ("Slower Growth", 13, 14, 20, "state"),
            ("Step Change", 22, 23, 29, "state"),
            ("Accelerated Transition", 31, 32, 38, "state"),
        ],
    )  # type: ignore


@check_types
def connections_forecasts() -> DataFrame[schemas.ConnectionsForecasts]:
    """Residential connections by scenario/state/year."""
    return _stacked_blocks(
        "Connections Forecasts",
        [
            ("Slower Growth", 15, 16, 22, "state"),
            ("Step Change", 24, 25, 31, "state"),
            ("Accelerated Transition", 33, 34, 40, "state"),
        ],
    )  # type: ignore


@check_types
def end_use_fuel_consumption() -> DataFrame[schemas.EndUseFuelConsumption]:
    """End-use fuel consumption by scenario/fuel sector/year (PJ).

    Note: scenario name is embedded in col0 of the year-header row for each block.
    """
    raw = (
        _open_isp().load_sheet_by_name("End use fuel consumption data", header_row=None).to_polars()
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
    return pl.DataFrame(parts).pipe(_parse_year_columns).pipe(_normalize_columns)  # type: ignore


@check_types
def energy_efficiency() -> DataFrame[schemas.EnergyEfficiency]:
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
    raw = _open_isp().load_sheet_by_name("Energy Efficiency", header_row=None).to_polars()
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
    return pl.DataFrame(parts).pipe(_parse_year_columns).pipe(_normalize_columns)  # type: ignore


@check_types
def hydrogen_monthly_profiles() -> DataFrame[schemas.HydrogenMonthlyProfiles]:
    return read_timeseries("Hydrogen monthly profiles", 7)  # type: ignore


@check_types
def water_for_hydrogen() -> DataFrame[schemas.WaterForHydrogen]:
    return read_timeseries("Water for Hydrogen", 6)  # type: ignore


@check_types
def desalination_demand_h2() -> DataFrame[schemas.DesalinationDemandH2]:
    return read_timeseries("Desalination demand for H2", 6)  # type: ignore


@check_types
def h2_gpg_limit() -> DataFrame[schemas.H2GpgLimit]:
    return read_timeseries("H2 as fuel for GPG Limit", 5)  # type: ignore


# ── Bespoke: Fuel Price Summary — two vertically-stacked tables ───────────────


@check_types
def fuel_price_summary() -> DataFrame[schemas.FuelPriceSummary]:
    """Fuel prices for all generators. Adds 'generator_status' column."""
    raw = _open_isp().load_sheet_by_name("Fuel Price Summary", header_row=None).to_polars()

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
    return (
        pl.concat([existing, new_entrant], how="diagonal_relaxed")
        .pipe(_normalize_columns)
        .pipe(_parse_year_columns)
    )  # type: ignore


# ── Bespoke: Economic Growth Forecasts — null in col0 on year-header row ──────


@check_types
def economic_growth_forecasts() -> DataFrame[schemas.EconomicGrowthForecasts]:
    """GSP and HDI per state/scenario/year. Hardcoded block boundaries."""
    raw = _open_isp().load_sheet_by_name("Economic Growth Forecasts", header_row=None).to_polars()

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

    return pl.DataFrame(parts).pipe(_normalize_columns).pipe(_parse_year_columns)  # type: ignore


# ── Bespoke: Power System Security — cols 0-2 only ───────────────────────────


@check_types
def power_system_security() -> DataFrame[schemas.PowerSystemSecurity]:
    """Coal generator fault-level replacement costs ($M)."""
    raw = _open_isp().load_sheet_by_name("Power System Security", header_row=None).to_polars()
    header_row = 4
    data = raw.slice(header_row + 1).select(raw.columns[:3])
    data.columns = ["duid", "generator_name", "fault_level_replacement_cost_m"]
    return data.filter(pl.col("duid").is_not_null())  # type: ignore


@check_types
def summary_mapping() -> DataFrame[schemas.SummaryMapping]:
    """Assumption mapping keys for all generators (existing, committed, anticipated, new entrant).

    Each row maps one generator unit to the lookup keys used to find its assumptions
    in other sheets (heat rates, outage rates, fuel costs, connection costs, etc.).

    The sheet has two labelled sections ('Existing, Committed, Anticipated' and
    'New Entrant') separated by blank rows; both are returned in one DataFrame.
    'Not Applicable' sentinel values are replaced with null.
    """
    raw = _open_isp().load_sheet_by_name("Summary Mapping", header_row=None).to_polars()
    df = _read_multiheader_block(raw, header_rows=[3, 4, 5], data_start=6)
    df = df.filter(pl.col("rowid").is_not_null())
    return df.with_columns(
        pl.col(pl.String).replace(["Not Applicable", "Not applicable", "N/A"], None)
    )  # type: ignore


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

    @property
    def maximum_capacity(self):
        return self._get("maximum_capacity", maximum_capacity)

    @property
    def maximum_capacity_by_technology(self):
        return self._get("maximum_capacity_by_technology", maximum_capacity_by_technology)

    @property
    def maintenance(self):
        return self._get("maintenance", maintenance)

    @property
    def maintenance_by_technology(self):
        return self._get("maintenance_by_technology", maintenance_by_technology)

    @property
    def heat_rates_by_technology(self):
        return self._get("heat_rates_by_technology", heat_rates_by_technology)

    @property
    def auxiliary(self):
        return self._get("auxiliary", auxiliary)

    @property
    def auxiliary_by_technology(self):
        return self._get("auxiliary_by_technology", auxiliary_by_technology)

    @property
    def emissions_intensity(self):
        return self._get("emissions_intensity", emissions_intensity)

    @property
    def emissions_intensity_by_technology(self):
        return self._get("emissions_intensity_by_technology", emissions_intensity_by_technology)

    @property
    def fixed_opex(self):
        return self._get("fixed_opex", fixed_opex)

    @property
    def fixed_opex_by_technology(self):
        return self._get("fixed_opex_by_technology", fixed_opex_by_technology)

    @property
    def variable_opex(self):
        return self._get("variable_opex", variable_opex)

    @property
    def variable_opex_by_technology(self):
        return self._get("variable_opex_by_technology", variable_opex_by_technology)

    @property
    def affine_heat_rates(self):
        return self._get("affine_heat_rates", affine_heat_rates)

    @property
    def affine_heat_rates_by_technology(self):
        return self._get("affine_heat_rates_by_technology", affine_heat_rates_by_technology)

    @property
    def max_ramp_rates(self):
        return self._get("max_ramp_rates", max_ramp_rates)

    @property
    def max_ramp_rates_by_technology(self):
        return self._get("max_ramp_rates_by_technology", max_ramp_rates_by_technology)

    @property
    def coal_min_stable_level(self):
        return self._get("coal_min_stable_level", coal_min_stable_level)

    @property
    def gpg_min_stable_level(self):
        return self._get("gpg_min_stable_level", gpg_min_stable_level)

    @property
    def gpg_min_stable_level_by_technology(self):
        return self._get("gpg_min_stable_level_by_technology", gpg_min_stable_level_by_technology)

    @property
    def hydro_scheme_inflows(self):
        return self._get("hydro_scheme_inflows", hydro_scheme_inflows)

    @property
    def renewable_energy_zones(self):
        return self._get("renewable_energy_zones", renewable_energy_zones)

    @property
    def gas_system_properties(self):
        return self._get("gas_system_properties", gas_system_properties)

    @property
    def build_limits_phes(self):
        return self._get("build_limits_phes", build_limits_phes)

    @property
    def build_limits_rez(self):
        return self._get("build_limits_rez", build_limits_rez)

    @property
    def build_limits_rez_transmission(self):
        return self._get("build_limits_rez_transmission", build_limits_rez_transmission)

    @property
    def build_limits_rez_modifiers(self):
        return self._get("build_limits_rez_modifiers", build_limits_rez_modifiers)

    @property
    def build_limits_rez_group_constraints(self):
        return self._get("build_limits_rez_group_constraints", build_limits_rez_group_constraints)

    @property
    def build_limits_rez_transmission_limit_constraints(self):
        return self._get(
            "build_limits_rez_transmission_limit_constraints",
            build_limits_rez_transmission_limit_constraints,
        )

    @property
    def build_limits_rez_secondary_transmission_limits(self):
        return self._get(
            "build_limits_rez_secondary_transmission_limits",
            build_limits_rez_secondary_transmission_limits,
        )

    @property
    def locational_cost_factors(self):
        return self._get("locational_cost_factors", locational_cost_factors)

    @property
    def locational_cost_factors_phes(self):
        return self._get("locational_cost_factors_phes", locational_cost_factors_phes)

    @property
    def locational_cost_factors_technology_specific(self):
        return self._get(
            "locational_cost_factors_technology_specific",
            locational_cost_factors_technology_specific,
        )

    @property
    def financial_parameters(self):
        return self._get("financial_parameters", financial_parameters)

    @property
    def flow_path_augmentation(self):
        return self._get("flow_path_augmentation", flow_path_augmentation)

    @property
    def elec_retail_price_indices(self):
        return self._get("elec_retail_price_indices", elec_retail_price_indices)
