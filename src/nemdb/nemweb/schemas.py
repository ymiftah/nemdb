"""Pandera schemas for NEMWEB database tables.

These schemas document the expected column types and structure for each AEMO MMS
table as returned by the corresponding DataSource.get_data() method.

All schemas use pandera.polars.DataFrameModel for native Polars support and IDE
type hinting. Fields are marked Optional since the _archive_to_df function fills
missing columns with null values.

## Schema Discovery

Each schema is indexed in SCHEMA_MAP for programmatic access:

    from nemdb.nemweb.schemas import SCHEMA_MAP
    schema = SCHEMA_MAP["DISPATCHREGIONSUM"]

## IDE Type Inference

DataSource instances store their schema class for IDE inference:

    from nemdb.nemweb import NEMWEBManager
    manager = NEMWEBManager(config)
    df = manager.DISPATCHREGIONSUM.get_data("2024/01/01 12:00:00")
    # IDE knows df conforms to DispatchRegionSumSchema via manager.DISPATCHREGIONSUM.schema_class

## Optional Runtime Validation

Validation is provided as an opt-in utility (not in critical path):

    from nemdb.nemweb.schemas import validate_against_schema
    result = validate_against_schema(df, DispatchRegionSumSchema)
    if not result:
        logger.warning("Data does not conform to schema")
"""

import logging
import types
import typing
from typing import Optional  # noqa: F401

import pandera.polars as pa
import polars as pl
from pandera import Field


# Dispatch Tables
# ===============
class BasePartitionedSchema(pa.DataFrameModel):
    """Base schema for partitioned tables with common columns."""

    archive_month: pl.Date = Field(nullable=False)


class DispatchRegionSumSchema(BasePartitionedSchema):
    """Daily region dispatch summary with demand and supply data."""

    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    REGIONID: pl.Categorical = Field(nullable=False)
    TOTALDEMAND: pl.Float32 | None = Field(nullable=True)
    DEMANDFORECAST: pl.Float32 | None = Field(nullable=True)
    DISPATCHABLELOAD: pl.Float32 | None = Field(nullable=True)
    INITIALSUPPLY: pl.Float32 | None = Field(nullable=True)
    SS_SOLAR_AVAILABILITY: pl.Float32 | None = Field(nullable=True)
    SS_WIND_AVAILABILITY: pl.Float32 | None = Field(nullable=True)
    AVAILABLEGENERATION: pl.Float32 | None = Field(nullable=True)
    AVAILABLELOAD: pl.Float32 | None = Field(nullable=True)


class DispatchLoadSchema(BasePartitionedSchema):
    """Dispatch load and availability data for generators."""

    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    DUID: pl.Categorical = Field(nullable=False)
    DISPATCHMODE: pl.Int8 | None = Field(nullable=True)
    AGCSTATUS: pl.Int8 | None = Field(nullable=True)
    INTERVENTION: pl.Float32 | None = Field(nullable=True)
    INITIALMW: pl.Float32 | None = Field(nullable=True)
    TOTALCLEARED: pl.Float32 | None = Field(nullable=True)
    RAMPDOWNRATE: pl.Float32 | None = Field(nullable=True)
    RAMPUPRATE: pl.Float32 | None = Field(nullable=True)
    AVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISEREGENABLEMENTMAX: pl.Float32 | None = Field(nullable=True)
    RAISEREGENABLEMENTMIN: pl.Float32 | None = Field(nullable=True)
    LOWERREGENABLEMENTMAX: pl.Float32 | None = Field(nullable=True)
    LOWERREGENABLEMENTMIN: pl.Float32 | None = Field(nullable=True)
    SEMIDISPATCHCAP: pl.Float32 | None = Field(nullable=True)
    LOWER5MIN: pl.Float32 | None = Field(nullable=True)
    LOWER60SEC: pl.Float32 | None = Field(nullable=True)
    LOWER6SEC: pl.Float32 | None = Field(nullable=True)
    LOWER1SEC: pl.Float32 | None = Field(nullable=True)
    RAISE5MIN: pl.Float32 | None = Field(nullable=True)
    RAISE60SEC: pl.Float32 | None = Field(nullable=True)
    RAISE6SEC: pl.Float32 | None = Field(nullable=True)
    RAISE1SEC: pl.Float32 | None = Field(nullable=True)
    LOWERREG: pl.Float32 | None = Field(nullable=True)
    RAISEREG: pl.Float32 | None = Field(nullable=True)
    RAISEREGAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISE6SECACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISE1SECACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISE60SECACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISE5MINACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    RAISEREGACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    LOWER6SECACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    LOWER1SECACTUALAVAILABILITY: pl.Float32 | None = Field(nullable=True)
    UIGF: pl.Float32 | None = Field(nullable=True)


class DispatchPriceSchema(BasePartitionedSchema):
    """Regional dispatch pricing for energy and reserve products."""

    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    REGIONID: pl.Categorical = Field(nullable=False)
    RRP: pl.Float32 | None = Field(nullable=True)
    ROP: pl.Float32 | None = Field(nullable=True)
    RAISE6SECROP: pl.Float32 | None = Field(nullable=True)
    RAISE1SECROP: pl.Float32 | None = Field(nullable=True)
    RAISE60SECROP: pl.Float32 | None = Field(nullable=True)
    RAISE5MINROP: pl.Float32 | None = Field(nullable=True)
    RAISEREGROP: pl.Float32 | None = Field(nullable=True)
    LOWER6SECROP: pl.Float32 | None = Field(nullable=True)
    LOWER1SECROP: pl.Float32 | None = Field(nullable=True)
    LOWER60SECROP: pl.Float32 | None = Field(nullable=True)
    LOWER5MINROP: pl.Float32 | None = Field(nullable=True)
    LOWERREGROP: pl.Float32 | None = Field(nullable=True)
    # FCAS Regional Reference Prices (clearing prices per service)
    RAISE6SECRRP: pl.Float32 | None = Field(nullable=True)
    RAISE1SECRRP: pl.Float32 | None = Field(nullable=True)
    RAISE60SECRRP: pl.Float32 | None = Field(nullable=True)
    RAISE5MINRRP: pl.Float32 | None = Field(nullable=True)
    RAISEREGRRP: pl.Float32 | None = Field(nullable=True)
    LOWER6SECRRP: pl.Float32 | None = Field(nullable=True)
    LOWER1SECRRP: pl.Float32 | None = Field(nullable=True)
    LOWER60SECRRP: pl.Float32 | None = Field(nullable=True)
    LOWER5MINRRP: pl.Float32 | None = Field(nullable=True)
    LOWERREGRRP: pl.Float32 | None = Field(nullable=True)
    # Intervention flag (0/1)
    INTERVENTION: pl.Float32 | None = Field(nullable=True)


class DispatchConstraintSchema(BasePartitionedSchema):
    """Dispatch constraint violations and marginal values."""

    CONSTRAINTID: pl.Categorical = Field(nullable=False)
    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    DUID: pl.Categorical | None = Field(nullable=True)
    INTERVENTION: pl.Float32 | None = Field(nullable=True)
    LASTCHANGED: pl.Date = Field(nullable=False)
    GENCONID_EFFECTIVEDATE: pl.Date | None = Field(nullable=True)
    GENCONID_VERSIONNO: pl.Int32 | None = Field(nullable=True)
    RHS: pl.Float32 | None = Field(nullable=True)
    LHS: pl.Float32 | None = Field(nullable=True)
    VIOLATIONDEGREE: pl.Float32 | None = Field(nullable=True)
    MARGINALVALUE: pl.Float32 | None = Field(nullable=True)


class DispatchInterconnectorResSchema(BasePartitionedSchema):
    """Interconnector flow and losses during dispatch."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    MWFLOW: pl.Float32 | None = Field(nullable=True)
    MWLOSSES: pl.Float32 | None = Field(nullable=True)
    EXPORTLIMIT: pl.Float32 | None = Field(nullable=True)
    IMPORTLIMIT: pl.Float32 | None = Field(nullable=True)


class DispatchUnitScadaSchema(BasePartitionedSchema):
    """Actual SCADA MW readings per DUID per dispatch interval."""

    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    DUID: pl.Categorical = Field(nullable=False)
    SCADAVALUE: pl.Float32 | None = Field(nullable=True)


# Bid Tables
# ==========


class BidDayOfferDSchema(BasePartitionedSchema):
    """Daily energy bid offers by generators."""

    DUID: pl.Categorical = Field(nullable=False)
    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    BIDTYPE: pl.Categorical = Field(nullable=False)
    DIRECTION: pl.Categorical = Field(nullable=False)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)
    PARTICIPANTID: pl.Categorical | None = Field(nullable=True)
    DAILYENERGYCONSTRAINT: pl.Float32 | None = Field(nullable=True)
    PRICEBAND1: pl.Float32 | None = Field(nullable=True)
    PRICEBAND2: pl.Float32 | None = Field(nullable=True)
    PRICEBAND3: pl.Float32 | None = Field(nullable=True)
    PRICEBAND4: pl.Float32 | None = Field(nullable=True)
    PRICEBAND5: pl.Float32 | None = Field(nullable=True)
    PRICEBAND6: pl.Float32 | None = Field(nullable=True)
    PRICEBAND7: pl.Float32 | None = Field(nullable=True)
    PRICEBAND8: pl.Float32 | None = Field(nullable=True)
    PRICEBAND9: pl.Float32 | None = Field(nullable=True)
    PRICEBAND10: pl.Float32 | None = Field(nullable=True)
    MINIMUMLOAD: pl.Float32 | None = Field(nullable=True)
    T1: pl.Float32 | None = Field(nullable=True)
    T2: pl.Float32 | None = Field(nullable=True)
    T3: pl.Float32 | None = Field(nullable=True)
    T4: pl.Float32 | None = Field(nullable=True)
    NORMALSTATUS: pl.String | None = Field(nullable=True)
    ENTRYTYPE: pl.Categorical | None = Field(nullable=True)


class BidPerOfferDSchema(BasePartitionedSchema):
    """Interval-level bid offers with availability and constraints."""

    DUID: pl.Categorical = Field(nullable=False)
    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    BIDTYPE: pl.Categorical = Field(nullable=False)
    DIRECTION: pl.Categorical = Field(nullable=False)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)
    INTERVAL_DATETIME: pl.Datetime = Field(nullable=False)
    MAXAVAIL: pl.Float32 | None = Field(nullable=True)
    FIXEDLOAD: pl.Float32 | None = Field(nullable=True)
    ROCUP: pl.Float32 | None = Field(nullable=True)
    ROCDOWN: pl.Float32 | None = Field(nullable=True)
    ENABLEMENTMIN: pl.Float32 | None = Field(nullable=True)
    ENABLEMENTMAX: pl.Float32 | None = Field(nullable=True)
    LOWBREAKPOINT: pl.Float32 | None = Field(nullable=True)
    HIGHBREAKPOINT: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL1: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL2: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL3: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL4: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL5: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL6: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL7: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL8: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL9: pl.Float32 | None = Field(nullable=True)
    BANDAVAIL10: pl.Float32 | None = Field(nullable=True)
    ENERGYLIMIT: pl.Float32 | None = Field(nullable=True)
    LASTCHANGED: pl.Datetime | None = Field(nullable=True)


# Generation Unit Tables
# ======================


class DUALLOCSchema(BasePartitionedSchema):
    """Dispatch unit to generation set allocation."""

    DUID: pl.Categorical = Field(nullable=False)
    GENSETID: pl.Categorical = Field(nullable=False)
    LASTCHANGED: pl.Datetime | None = Field(nullable=True)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)


class GENUNITSSchema(BasePartitionedSchema):
    """Generation unit characteristics and capabilities."""

    GENSETID: pl.Categorical = Field(nullable=False)
    STATIONID: pl.String = Field(nullable=False)
    VOLTLEVEL: pl.Float32 | None = Field(nullable=True)
    DISPATCHTYPE: pl.Categorical = Field(nullable=False)
    STARTTYPE: pl.String | None = Field(nullable=True)
    NORMALSTATUS: pl.String | None = Field(nullable=True)
    MAXCAPACITY: pl.Float32 | None = Field(nullable=True)
    GENSETTYPE: pl.String | None = Field(nullable=True)
    GENSETNAME: pl.String | None = Field(nullable=True)
    LOWERREG: pl.Float32 | None = Field(nullable=True)
    CO2E_EMISSIONS_FACTOR: pl.Float32 | None = Field(nullable=True)
    CO2E_ENERGY_SOURCE: pl.String | None = Field(nullable=True)
    CO2E_DATA_SOURCE: pl.String | None = Field(nullable=True)
    MINCAPACITY: pl.Float32 | None = Field(nullable=True)
    REGISTEREDMINCAPACITY: pl.Float32 | None = Field(nullable=True)
    LASTCHANGED: pl.Datetime | None = Field(nullable=True)


class DUDETAILSUMMARYSchema(BasePartitionedSchema):
    """Dispatch unit summary with operational dates and limits."""

    DUID: pl.Categorical = Field(nullable=False)
    START_DATE: pl.Date = Field(nullable=False)
    END_DATE: pl.Date = Field(nullable=False)
    DISPATCHTYPE: pl.Categorical | None = Field(nullable=True)
    CONNECTIONPOINTID: pl.Categorical | None = Field(nullable=True)
    REGIONID: pl.Categorical | None = Field(nullable=True)
    STATIONID: pl.String | None = Field(nullable=True)
    TRANSMISSIONLOSSFACTOR: pl.Float32 | None = Field(nullable=True)
    STARTTYPE: pl.String | None = Field(nullable=True)
    DISTRIBUTIONLOSSFACTOR: pl.Float32 | None = Field(nullable=True)
    MINIMUM_ENERGY_PRICE: pl.Float32 | None = Field(nullable=True)
    MAXIMUM_ENERGY_PRICE: pl.Float32 | None = Field(nullable=True)
    SCHEDULE_TYPE: pl.Categorical | None = Field(nullable=True)
    MIN_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    MIN_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    MAX_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    MAX_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    IS_AGGREGATED: pl.Boolean | None = Field(nullable=True)
    LOAD_MINIMUM_ENERGY_PRICE: pl.Float32 | None = Field(nullable=True)
    LOAD_MAXIMUM_ENERGY_PRICE: pl.Float32 | None = Field(nullable=True)
    LOAD_MIN_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    LOAD_MIN_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    LOAD_MAX_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    LOAD_MAX_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    SECONDARY_TLF: pl.Float32 | None = Field(nullable=True)


class DUDETAILSchema(BasePartitionedSchema):
    """Dispatch unit detailed technical specifications."""

    DUID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    CONNECTIONPOINTID: pl.Categorical | None = Field(nullable=True)
    VOLTLEVEL: pl.Float32 | None = Field(nullable=True)
    REGISTEREDCAPACITY: pl.Float32 | None = Field(nullable=True)
    AGCCAPABILITY: pl.String | None = Field(nullable=True)
    DISPATCHTYPE: pl.Categorical | None = Field(nullable=True)
    MAXCAPACITY: pl.Float32 | None = Field(nullable=True)
    STARTTYPE: pl.String | None = Field(nullable=True)
    NORMALLYONFLAG: pl.String | None = Field(nullable=True)
    SPINNINGRESERVEFLAG: pl.String | None = Field(nullable=True)
    INTERMITTENTFLAG: pl.String | None = Field(nullable=True)
    SEMISCHEDULE_FLAG: pl.String | None = Field(nullable=True)
    MAXRATEOFCHANGEUP: pl.Float32 | None = Field(nullable=True)
    MAXRATEOFCHANGEDOWN: pl.Float32 | None = Field(nullable=True)
    ADG_ID: pl.String | None = Field(nullable=True)
    MINCAPACITY: pl.Float32 | None = Field(nullable=True)
    REGISTEREDMINCAPACITY: pl.Float32 | None = Field(nullable=True)
    MAXRATEOFCHANGEUP_LOAD: pl.Float32 | None = Field(nullable=True)
    MAXRATEOFCHANGEDOWN_LOAD: pl.Float32 | None = Field(nullable=True)
    MAXSTORAGECAPACITY: pl.Float32 | None = Field(nullable=True)
    STORAGEIMPORTEFFICIENCYFACTOR: pl.Float32 | None = Field(nullable=True)
    STORAGEEXPORTEFFICIENCYFACTOR: pl.Float32 | None = Field(nullable=True)
    MIN_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    MIN_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    LOAD_MIN_RAMP_RATE_UP: pl.Float32 | None = Field(nullable=True)
    LOAD_MIN_RAMP_RATE_DOWN: pl.Float32 | None = Field(nullable=True)
    AGGREGATED: pl.String | None = Field(nullable=True)


class RESERVESchema(BasePartitionedSchema):
    """Regional reserve requirements and availability."""

    SETTLEMENTDATE: pl.Datetime = Field(nullable=False)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)
    REGIONID: pl.Categorical = Field(nullable=False)
    PERIODID: pl.Int32 | None = Field(nullable=True)
    LOWER5MIN: pl.Float32 | None = Field(nullable=True)
    RAISE5MIN: pl.Float32 | None = Field(nullable=True)
    RAISEREG: pl.Float32 | None = Field(nullable=True)
    LOWERREG: pl.Float32 | None = Field(nullable=True)


# Station Tables
# ==============


class PARTICIPANTSchema(pa.DataFrameModel):
    """Participant ID, name and class for all registered NEM participants."""

    PARTICIPANTID: pl.String = Field(nullable=False)
    PARTICIPANTCLASSID: pl.String | None = Field(nullable=True)
    NAME: pl.String | None = Field(nullable=True)
    DESCRIPTION: pl.String | None = Field(nullable=True)
    ACN: pl.String | None = Field(nullable=True)
    PRIMARYBUSINESS: pl.String | None = Field(nullable=True)
    LASTCHANGED: pl.Datetime | None = Field(nullable=True)


class STATIONSchema(pa.DataFrameModel):
    """Power station location and contact information."""

    STATIONID: pl.String = Field(nullable=False)
    STATIONNAME: pl.String | None = Field(nullable=True)
    ADDRESS1: pl.String | None = Field(nullable=True)
    ADDRESS2: pl.String | None = Field(nullable=True)
    ADDRESS3: pl.String | None = Field(nullable=True)
    ADDRESS4: pl.String | None = Field(nullable=True)
    CITY: pl.String | None = Field(nullable=True)
    STATE: pl.String | None = Field(nullable=True)
    POSTCODE: pl.String | None = Field(nullable=True)


class STATIONOPERATINGSTATUSSchema(BasePartitionedSchema):
    """Station operating status over time."""

    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    STATIONID: pl.String = Field(nullable=False)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)
    STATUS: pl.String | None = Field(nullable=True)


class STATIONOWNERSchema(BasePartitionedSchema):
    """Station ownership and participant information."""

    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    PARTICIPANTID: pl.Categorical = Field(nullable=False)
    STATIONID: pl.String = Field(nullable=False)
    VERSIONNO: pl.Int32 | None = Field(nullable=True)


class STADUALLOCSchema(BasePartitionedSchema):
    """Station to dispatch unit allocation."""

    DUID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    STATIONID: pl.String = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)


# Interconnector Tables
# =====================


class INTERCONNECTORSchema(BasePartitionedSchema):
    """Interconnector corridor definitions with region endpoints."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    REGIONFROM: pl.Categorical = Field(nullable=False)
    REGIONTO: pl.Categorical = Field(nullable=False)


class INTERCONNECTORCONSTRAINTSchema(BasePartitionedSchema):
    """Interconnector technical constraints and limits."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    FROMREGIONLOSSSHARE: pl.Float32 | None = Field(nullable=True)
    ICTYPE: pl.Categorical | None = Field(nullable=True)
    LOSSCONSTANT: pl.Float32 | None = Field(nullable=True)
    LOSSFLOWCOEFFICIENT: pl.Float32 | None = Field(nullable=True)
    IMPORTLIMIT: pl.Float32 | None = Field(nullable=True)
    EXPORTLIMIT: pl.Float32 | None = Field(nullable=True)
    MAXMWIN: pl.Float32 | None = Field(nullable=True)
    MAXMWOUT: pl.Float32 | None = Field(nullable=True)


class LOSSMODELSchema(BasePartitionedSchema):
    """Loss model segments for interconnectors."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    LOSSSEGMENT: pl.Int32 | None = Field(nullable=True)
    MWBREAKPOINT: pl.Float32 | None = Field(nullable=True)


class LOSSFACTORMODELSchema(BasePartitionedSchema):
    """Loss factors by region on interconnectors."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    REGIONID: pl.Categorical | None = Field(nullable=True)
    DEMANDCOEFFICIENT: pl.Float32 | None = Field(nullable=True)


class MNSP_INTERCONNECTORSchema(BasePartitionedSchema):
    """Market Network Service Provider interconnector details."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    LINKID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    FROMREGION: pl.Categorical | None = Field(nullable=True)
    TOREGION: pl.Categorical | None = Field(nullable=True)
    FROM_REGION_TLF: pl.Float32 | None = Field(nullable=True)
    TO_REGION_TLF: pl.Float32 | None = Field(nullable=True)
    LHSFACTOR: pl.Float32 | None = Field(nullable=True)
    MAXCAPACITY: pl.Float32 | None = Field(nullable=True)


# Constraint Tables
# =================


class GENCONDATASchema(BasePartitionedSchema):
    """Generic constraint definitions and weighting."""

    GENCONID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    CONSTRAINTTYPE: pl.Categorical | None = Field(nullable=True)
    GENERICCONSTRAINTWEIGHT: pl.Float32 | None = Field(nullable=True)
    DESCRIPTION: pl.String | None = Field(nullable=True)


class SPDREGIONCONSTRAINTSchema(BasePartitionedSchema):
    """Regional constraints on specific dispatch unit types."""

    REGIONID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    GENCONID: pl.Categorical = Field(nullable=False)
    BIDTYPE: pl.Categorical = Field(nullable=False)
    FACTOR: pl.Float32 | None = Field(nullable=True)


class SPDCONNECTIONPOINTCONSTRAINTSchema(BasePartitionedSchema):
    """Connection point constraints on specific dispatch unit types."""

    CONNECTIONPOINTID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    GENCONID: pl.Categorical = Field(nullable=False)
    BIDTYPE: pl.Categorical = Field(nullable=False)
    FACTOR: pl.Float32 | None = Field(nullable=True)


class SPDINTERCONNECTORCONSTRAINTSchema(BasePartitionedSchema):
    """Interconnector constraints on specific dispatch unit types."""

    INTERCONNECTORID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    GENCONID: pl.Categorical = Field(nullable=False)
    FACTOR: pl.Float32 | None = Field(nullable=True)


class GENCONSETSchema(BasePartitionedSchema):
    """Maps generic constraint sets to individual constraint equations (GENCONID)."""

    GENCONSETID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    GENCONID: pl.Categorical = Field(nullable=False)
    LASTCHANGED: pl.Date | None = Field(nullable=True)


class GENCONSETINVOKESchema(BasePartitionedSchema):
    """Active constraint set invocations with start/end interval datetime windows.

    THE key table for determining which constraints are active per dispatch interval.
    Filter: STARTAUTHORISEDBY IS NOT NULL (non-null = active invocation).
    """

    INVOCATION_ID: pl.Int64 = Field(nullable=False)
    STARTDATE: pl.Date | None = Field(nullable=True)
    STARTPERIOD: pl.Int32 | None = Field(nullable=True)
    GENCONSETID: pl.Categorical | None = Field(nullable=True)
    ENDDATE: pl.Date | None = Field(nullable=True)
    ENDPERIOD: pl.Int32 | None = Field(nullable=True)
    STARTAUTHORISEDBY: pl.Utf8 | None = Field(nullable=True)
    ENDAUTHORISEDBY: pl.Utf8 | None = Field(nullable=True)
    INTERVENTION: pl.Utf8 | None = Field(nullable=True)
    ASCONSTRAINTTYPE: pl.Utf8 | None = Field(nullable=True)
    LASTCHANGED: pl.Datetime | None = Field(nullable=True)
    STARTINTERVALDATETIME: pl.Datetime | None = Field(nullable=True)
    ENDINTERVALDATETIME: pl.Datetime | None = Field(nullable=True)
    SYSTEMNORMAL: pl.Utf8 | None = Field(nullable=True)


class GENCONSETTRKSchema(BasePartitionedSchema):
    """Constraint set version tracking — helps resolve the correct version in GENCONSETINVOKE."""

    GENCONSETID: pl.Categorical = Field(nullable=False)
    EFFECTIVEDATE: pl.Date = Field(nullable=False)
    VERSIONNO: pl.Int32 = Field(nullable=False)
    DESCRIPTION: pl.Utf8 | None = Field(nullable=True)
    AUTHORISEDBY: pl.Utf8 | None = Field(nullable=True)
    AUTHORISEDDATE: pl.Date | None = Field(nullable=True)
    LASTCHANGED: pl.Date | None = Field(nullable=True)
    COVERAGE: pl.Utf8 | None = Field(nullable=True)
    SYSTEMNORMAL: pl.Utf8 | None = Field(nullable=True)
    OUTAGE: pl.Utf8 | None = Field(nullable=True)


# DNSP Tables (Not in standard DTYPES)
# ====================================
# Note: ZONE_SUBSTATION uses DNSP-specific columns not in DTYPES.
# Types derived from usage in DNSPDataSource.


class ZONESUBSTATIONSchema(pa.DataFrameModel):
    """Distribution network zone substation data from DNSP operators."""

    time: pl.String | None = Field(nullable=True)  # DNSP-specific, not in standard DTYPES
    zss: pl.String | None = Field(nullable=True)  # DNSP-specific, not in standard DTYPES
    MW: pl.Float32 | None = Field(nullable=True)
    network: pl.String | None = Field(nullable=True)


# SCHEMA_MAP Registry
# ===================
# Maps table names to their corresponding Pandera schemas for discovery and validation

SCHEMA_MAP: dict[str, type[pa.DataFrameModel]] = {
    # Dispatch Tables
    "DISPATCHREGIONSUM": DispatchRegionSumSchema,
    "DISPATCHLOAD": DispatchLoadSchema,
    "DISPATCHPRICE": DispatchPriceSchema,
    "DISPATCHCONSTRAINT": DispatchConstraintSchema,
    "DISPATCHINTERCONNECTORRES": DispatchInterconnectorResSchema,
    "DISPATCH_UNIT_SCADA": DispatchUnitScadaSchema,
    # Bid Tables
    "BIDDAYOFFER_D": BidDayOfferDSchema,
    "BIDPEROFFER_D": BidPerOfferDSchema,
    # Generation Unit Tables
    "DUALLOC": DUALLOCSchema,
    "GENUNITS": GENUNITSSchema,
    "DUDETAILSUMMARY": DUDETAILSUMMARYSchema,
    "DUDETAIL": DUDETAILSchema,
    "RESERVE": RESERVESchema,
    # Participant Tables
    "PARTICIPANT": PARTICIPANTSchema,
    # Station Tables
    "STATION": STATIONSchema,
    "STATIONOPERATINGSTATUS": STATIONOPERATINGSTATUSSchema,
    "STATIONOWNER": STATIONOWNERSchema,
    "STADUALLOC": STADUALLOCSchema,
    # Interconnector Tables
    "INTERCONNECTOR": INTERCONNECTORSchema,
    "INTERCONNECTORCONSTRAINT": INTERCONNECTORCONSTRAINTSchema,
    "LOSSMODEL": LOSSMODELSchema,
    "LOSSFACTORMODEL": LOSSFACTORMODELSchema,
    "MNSP_INTERCONNECTOR": MNSP_INTERCONNECTORSchema,
    # Constraint Tables
    "GENCONDATA": GENCONDATASchema,
    "SPDREGIONCONSTRAINT": SPDREGIONCONSTRAINTSchema,
    "SPDCONNECTIONPOINTCONSTRAINT": SPDCONNECTIONPOINTCONSTRAINTSchema,
    "SPDINTERCONNECTORCONSTRAINT": SPDINTERCONNECTORCONSTRAINTSchema,
    "GENCONSET": GENCONSETSchema,
    "GENCONSETINVOKE": GENCONSETINVOKESchema,
    "GENCONSETTRK": GENCONSETTRKSchema,
    # DNSP Tables
    "ZONE_SUBSTATION": ZONESUBSTATIONSchema,
}


# Type Extraction
# ==============


def _schema_to_dtypes(schema_class: type[BasePartitionedSchema]) -> dict[str, type]:
    """Extract Polars column types from a Pandera schema, unwrapping optional unions.

    Pandera schemas use `pl.X | None = Field(nullable=True)` for optional fields. This function extracts
    the bare Polars type (e.g., `pl.Float32`) for use in `.cast()` operations.
    Required fields like `pl.Datetime` are returned as-is.

    Args:
        schema_class: A Pandera DataFrameModel subclass

    Returns:
        dict mapping column name -> bare Polars type (without | None = Field(nullable=True) wrapper)

    Example:
        >>> dtypes = _schema_to_dtypes(DispatchRegionSumSchema)
        >>> dtypes['TOTALDEMAND']  # returns pl.Float32, not pl.Float32 | None = Field(nullable=True)
        <polars.datatypes.Float32 object>
    """
    result = {}
    type_hints = typing.get_type_hints(schema_class)
    # Only process actual schema fields
    for field_name in schema_class.__fields__:
        annotation = type_hints.get(field_name)
        if annotation is None:
            continue
        origin = typing.get_origin(annotation)
        # Handle X | None = Field(nullable=True) (types.UnionType) or typing.Union[X, None]
        if origin is types.UnionType or origin is typing.Union:
            # Get union args and filter out NoneType
            args = [a for a in typing.get_args(annotation) if a is not type(None)]
            result[field_name] = args[0] if args else annotation
        else:
            # Not a union - bare type like pl.Datetime
            result[field_name] = annotation
    return result


# Validation Utilities
# ====================


def validate_against_schema(
    df: pl.DataFrame,
    schema_class: type[BasePartitionedSchema],
    raise_on_error: bool = False,
) -> bool:
    """
    Validate a Polars DataFrame against a Pandera schema.

    This is an opt-in utility for schema validation. It's not called automatically
    to preserve the performance characteristics of the main data pipeline. Use this
    in tests, CI/CD, or specific validation scenarios.

    Args:
        df: DataFrame to validate
        schema_class: Pandera DataFrameModel schema class to validate against
        raise_on_error: If True, raise SchemaError on validation failure.
                       If False, return False and log warning.

    Returns:
        True if validation passes, False if validation fails (and raise_on_error=False)

    Raises:
        pandera.errors.SchemaError: If validation fails and raise_on_error=True
    """
    try:
        schema_class.validate(df)
        return True
    except pa.errors.SchemaError as e:
        if raise_on_error:
            raise
        logger = logging.getLogger(__name__)
        logger.warning(f"Schema validation failed for {schema_class.__name__}: {e}")
        return False
