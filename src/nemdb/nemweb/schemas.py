"""Pandera schemas for NEMWEB database tables.

These schemas document the expected column types and structure for each AEMO MMS
table as returned by the corresponding DataSource.get_data() method.

All schemas use pandera.polars.DataFrameModel for native Polars support and IDE
type hinting. Fields are marked Optional since the _archive_to_df function fills
missing columns with null values.
"""

from typing import Optional  # noqa: F401

import pandera.polars as pa
import polars as pl

# Dispatch Tables
# ===============


class DispatchRegionSumSchema(pa.DataFrameModel):
    """Daily region dispatch summary with demand and supply data."""

    SETTLEMENTDATE: pl.Datetime
    REGIONID: pl.Categorical
    TOTALDEMAND: pl.Float32 | None
    DEMANDFORECAST: pl.Float32 | None
    DISPATCHABLELOAD: pl.Float32 | None
    INITIALSUPPLY: pl.Float32 | None
    SS_SOLAR_AVAILABILITY: pl.Float32 | None
    SS_WIND_AVAILABILITY: pl.Float32 | None
    AVAILABLEGENERATION: pl.Float32 | None
    AVAILABLELOAD: pl.Float32 | None


class DispatchLoadSchema(pa.DataFrameModel):
    """Dispatch load and availability data for generators."""

    SETTLEMENTDATE: pl.Datetime
    DUID: pl.Categorical
    DISPATCHMODE: pl.Int8 | None
    AGCSTATUS: pl.Int8 | None
    INITIALMW: pl.Float32 | None
    TOTALCLEARED: pl.Float32 | None
    RAMPDOWNRATE: pl.Float32 | None
    RAMPUPRATE: pl.Float32 | None
    AVAILABILITY: pl.Float32 | None
    RAISEREGENABLEMENTMAX: pl.Float32 | None
    RAISEREGENABLEMENTMIN: pl.Float32 | None
    LOWERREGENABLEMENTMAX: pl.Float32 | None
    LOWERREGENABLEMENTMIN: pl.Float32 | None
    SEMIDISPATCHCAP: pl.Float32 | None
    LOWER5MIN: pl.Float32 | None
    LOWER60SEC: pl.Float32 | None
    LOWER6SEC: pl.Float32 | None
    LOWER1SEC: pl.Float32 | None
    RAISE5MIN: pl.Float32 | None
    RAISE60SEC: pl.Float32 | None
    RAISE6SEC: pl.Float32 | None
    RAISE1SEC: pl.Float32 | None
    LOWERREG: pl.Float32 | None
    RAISEREG: pl.Float32 | None
    RAISEREGAVAILABILITY: pl.Float32 | None
    RAISE6SECACTUALAVAILABILITY: pl.Float32 | None
    RAISE1SECACTUALAVAILABILITY: pl.Float32 | None
    RAISE60SECACTUALAVAILABILITY: pl.Float32 | None
    RAISE5MINACTUALAVAILABILITY: pl.Float32 | None
    RAISEREGACTUALAVAILABILITY: pl.Float32 | None
    LOWER6SECACTUALAVAILABILITY: pl.Float32 | None
    LOWER1SECACTUALAVAILABILITY: pl.Float32 | None
    UIGF: pl.Float32 | None


class DispatchPriceSchema(pa.DataFrameModel):
    """Regional dispatch pricing for energy and reserve products."""

    SETTLEMENTDATE: pl.Datetime
    REGIONID: pl.Categorical
    RRP: pl.Float32 | None
    ROP: pl.Float32 | None
    RAISE6SECROP: pl.Float32 | None
    RAISE1SECROP: pl.Float32 | None
    RAISE60SECROP: pl.Float32 | None
    RAISE5MINROP: pl.Float32 | None
    RAISEREGROP: pl.Float32 | None
    LOWER6SECROP: pl.Float32 | None
    LOWER1SECROP: pl.Float32 | None
    LOWER60SECROP: pl.Float32 | None
    LOWER5MINROP: pl.Float32 | None
    LOWERREGROP: pl.Float32 | None


class DispatchConstraintSchema(pa.DataFrameModel):
    """Dispatch constraint violations and marginal values."""

    SETTLEMENTDATE: pl.Datetime
    CONSTRAINTID: pl.Categorical
    DUID: pl.Categorical | None
    RHS: pl.Float32 | None
    GENCONID_EFFECTIVEDATE: pl.Date | None
    GENCONID_VERSIONNO: pl.Int32 | None
    LHS: pl.Float32 | None
    VIOLATIONDEGREE: pl.Float32 | None
    MARGINALVALUE: pl.Float32 | None


class DispatchInterconnectorResSchema(pa.DataFrameModel):
    """Interconnector flow and losses during dispatch."""

    INTERCONNECTORID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    MWFLOW: pl.Float32 | None
    MWLOSSES: pl.Float32 | None
