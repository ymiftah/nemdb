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


# Dispatch Tables
# ===============
class BasePartitionedSchema(pa.DataFrameModel):
    """Base schema for partitioned tables with common columns."""

    archive_month: pl.Date


class DispatchRegionSumSchema(BasePartitionedSchema):
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


class DispatchLoadSchema(BasePartitionedSchema):
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


class DispatchPriceSchema(BasePartitionedSchema):
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


class DispatchConstraintSchema(BasePartitionedSchema):
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


class DispatchInterconnectorResSchema(BasePartitionedSchema):
    """Interconnector flow and losses during dispatch."""

    INTERCONNECTORID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    MWFLOW: pl.Float32 | None
    MWLOSSES: pl.Float32 | None
    EXPORTLIMIT: pl.Float32 | None
    IMPORTLIMIT: pl.Float32 | None


# Bid Tables
# ==========


class BidDayOfferDSchema(BasePartitionedSchema):
    """Daily energy bid offers by generators."""

    DUID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    BIDTYPE: pl.Categorical
    DIRECTION: pl.Categorical
    VERSIONNO: pl.Int32 | None
    PARTICIPANTID: pl.Categorical | None
    DAILYENERGYCONSTRAINT: pl.Float32 | None
    PRICEBAND1: pl.Float32 | None
    PRICEBAND2: pl.Float32 | None
    PRICEBAND3: pl.Float32 | None
    PRICEBAND4: pl.Float32 | None
    PRICEBAND5: pl.Float32 | None
    PRICEBAND6: pl.Float32 | None
    PRICEBAND7: pl.Float32 | None
    PRICEBAND8: pl.Float32 | None
    PRICEBAND9: pl.Float32 | None
    PRICEBAND10: pl.Float32 | None
    MINIMUMLOAD: pl.Float32 | None
    T1: pl.Float32 | None
    T2: pl.Float32 | None
    T3: pl.Float32 | None
    T4: pl.Float32 | None
    NORMALSTATUS: pl.String | None
    ENTRYTYPE: pl.Categorical | None


class BidPerOfferDSchema(BasePartitionedSchema):
    """Interval-level bid offers with availability and constraints."""

    DUID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    BIDTYPE: pl.Categorical
    DIRECTION: pl.Categorical
    VERSIONNO: pl.Int32 | None
    INTERVAL_DATETIME: pl.Datetime
    MAXAVAIL: pl.Float32 | None
    FIXEDLOAD: pl.Float32 | None
    ROCUP: pl.Float32 | None
    ROCDOWN: pl.Float32 | None
    ENABLEMENTMIN: pl.Float32 | None
    ENABLEMENTMAX: pl.Float32 | None
    LOWBREAKPOINT: pl.Float32 | None
    HIGHBREAKPOINT: pl.Float32 | None
    BANDAVAIL1: pl.Float32 | None
    BANDAVAIL2: pl.Float32 | None
    BANDAVAIL3: pl.Float32 | None
    BANDAVAIL4: pl.Float32 | None
    BANDAVAIL5: pl.Float32 | None
    BANDAVAIL6: pl.Float32 | None
    BANDAVAIL7: pl.Float32 | None
    BANDAVAIL8: pl.Float32 | None
    BANDAVAIL9: pl.Float32 | None
    BANDAVAIL10: pl.Float32 | None
    ENERGYLIMIT: pl.Float32 | None
    LASTCHANGED: pl.Datetime | None


# Generation Unit Tables
# ======================


class DUALLOCSchema(BasePartitionedSchema):
    """Dispatch unit to generation set allocation."""

    DUID: pl.Categorical
    GENSETID: pl.Categorical
    LASTCHANGED: pl.Datetime | None
    VERSIONNO: pl.Int32 | None


class GENUNITSSchema(BasePartitionedSchema):
    """Generation unit characteristics and capabilities."""

    GENSETID: pl.Categorical
    STATIONID: pl.String
    VOLTLEVEL: pl.Float32 | None
    DISPATCHTYPE: pl.Categorical
    STARTTYPE: pl.String | None
    NORMALSTATUS: pl.String | None
    MAXCAPACITY: pl.Float32 | None
    GENSETTYPE: pl.String | None
    GENSETNAME: pl.String | None
    LOWERREG: pl.Float32 | None
    CO2E_EMISSIONS_FACTOR: pl.Float32 | None
    CO2E_ENERGY_SOURCE: pl.String | None
    CO2E_DATA_SOURCE: pl.String | None
    MINCAPACITY: pl.Float32 | None
    REGISTEREDMINCAPACITY: pl.Float32 | None
    LASTCHANGED: pl.Datetime | None


class DUDETAILSUMMARYSchema(BasePartitionedSchema):
    """Dispatch unit summary with operational dates and limits."""

    DUID: pl.Categorical
    START_DATE: pl.Date
    END_DATE: pl.Date
    DISPATCHTYPE: pl.Categorical | None
    CONNECTIONPOINTID: pl.Categorical | None
    REGIONID: pl.Categorical | None
    STATIONID: pl.String | None
    TRANSMISSIONLOSSFACTOR: pl.Float32 | None
    STARTTYPE: pl.String | None
    DISTRIBUTIONLOSSFACTOR: pl.Float32 | None
    MINIMUM_ENERGY_PRICE: pl.Float32 | None
    MAXIMUM_ENERGY_PRICE: pl.Float32 | None
    SCHEDULE_TYPE: pl.Categorical | None
    MIN_RAMP_RATE_UP: pl.Float32 | None
    MIN_RAMP_RATE_DOWN: pl.Float32 | None
    MAX_RAMP_RATE_UP: pl.Float32 | None
    MAX_RAMP_RATE_DOWN: pl.Float32 | None
    IS_AGGREGATED: pl.Boolean | None
    LOAD_MINIMUM_ENERGY_PRICE: pl.Float32 | None
    LOAD_MAXIMUM_ENERGY_PRICE: pl.Float32 | None
    LOAD_MIN_RAMP_RATE_UP: pl.Float32 | None
    LOAD_MIN_RAMP_RATE_DOWN: pl.Float32 | None
    LOAD_MAX_RAMP_RATE_UP: pl.Float32 | None
    LOAD_MAX_RAMP_RATE_DOWN: pl.Float32 | None
    SECONDARY_TLF: pl.Float32 | None


class DUDETAILSchema(BasePartitionedSchema):
    """Dispatch unit detailed technical specifications."""

    DUID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    CONNECTIONPOINTID: pl.Categorical | None
    VOLTLEVEL: pl.Float32 | None
    REGISTEREDCAPACITY: pl.Float32 | None
    AGCCAPABILITY: pl.String | None
    DISPATCHTYPE: pl.Categorical | None
    MAXCAPACITY: pl.Float32 | None
    STARTTYPE: pl.String | None
    NORMALLYONFLAG: pl.String | None
    SPINNINGRESERVEFLAG: pl.String | None
    INTERMITTENTFLAG: pl.String | None
    SEMISCHEDULE_FLAG: pl.String | None
    MAXRATEOFCHANGEUP: pl.Float32 | None
    MAXRATEOFCHANGEDOWN: pl.Float32 | None
    ADG_ID: pl.String | None
    MINCAPACITY: pl.Float32 | None
    REGISTEREDMINCAPACITY: pl.Float32 | None
    MAXRATEOFCHANGEUP_LOAD: pl.Float32 | None
    MAXRATEOFCHANGEDOWN_LOAD: pl.Float32 | None
    MAXSTORAGECAPACITY: pl.Float32 | None
    STORAGEIMPORTEFFICIENCYFACTOR: pl.Float32 | None
    STORAGEEXPORTEFFICIENCYFACTOR: pl.Float32 | None
    MIN_RAMP_RATE_UP: pl.Float32 | None
    MIN_RAMP_RATE_DOWN: pl.Float32 | None
    LOAD_MIN_RAMP_RATE_UP: pl.Float32 | None
    LOAD_MIN_RAMP_RATE_DOWN: pl.Float32 | None
    AGGREGATED: pl.String | None


class RESERVESchema(BasePartitionedSchema):
    """Regional reserve requirements and availability."""

    SETTLEMENTDATE: pl.Datetime
    VERSIONNO: pl.Int32 | None
    REGIONID: pl.Categorical
    PERIODID: pl.Int32 | None
    LOWER5MIN: pl.Float32 | None
    RAISE5MIN: pl.Float32 | None
    RAISEREG: pl.Float32 | None
    LOWERREG: pl.Float32 | None


# Station Tables
# ==============


class PARTICIPANTSchema(pa.DataFrameModel):
    """Participant ID, name and class for all registered NEM participants."""

    PARTICIPANTID: pl.String
    PARTICIPANTCLASSID: pl.String | None
    NAME: pl.String | None
    DESCRIPTION: pl.String | None
    ACN: pl.String | None
    PRIMARYBUSINESS: pl.String | None
    LASTCHANGED: pl.Datetime | None


class STATIONSchema(pa.DataFrameModel):
    """Power station location and contact information."""

    STATIONID: pl.String
    STATIONNAME: pl.String | None
    ADDRESS1: pl.String | None
    ADDRESS2: pl.String | None
    ADDRESS3: pl.String | None
    ADDRESS4: pl.String | None
    CITY: pl.String | None
    STATE: pl.String | None
    POSTCODE: pl.String | None


class STATIONOPERATINGSTATUSSchema(BasePartitionedSchema):
    """Station operating status over time."""

    EFFECTIVEDATE: pl.Date
    STATIONID: pl.String
    VERSIONNO: pl.Int32 | None
    STATUS: pl.String | None


class STATIONOWNERSchema(BasePartitionedSchema):
    """Station ownership and participant information."""

    EFFECTIVEDATE: pl.Date
    PARTICIPANTID: pl.Categorical
    STATIONID: pl.String
    VERSIONNO: pl.Int32 | None


class STADUALLOCSchema(BasePartitionedSchema):
    """Station to dispatch unit allocation."""

    DUID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    STATIONID: pl.String
    VERSIONNO: pl.Int32


# Interconnector Tables
# =====================


class INTERCONNECTORSchema(BasePartitionedSchema):
    """Interconnector corridor definitions with region endpoints."""

    INTERCONNECTORID: pl.Categorical
    REGIONFROM: pl.Categorical
    REGIONTO: pl.Categorical


class INTERCONNECTORCONSTRAINTSchema(BasePartitionedSchema):
    """Interconnector technical constraints and limits."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    FROMREGIONLOSSSHARE: pl.Float32 | None
    ICTYPE: pl.Categorical | None
    LOSSCONSTANT: pl.Float32 | None
    LOSSFLOWCOEFFICIENT: pl.Float32 | None
    IMPORTLIMIT: pl.Float32 | None
    EXPORTLIMIT: pl.Float32 | None
    MAXMWIN: pl.Float32 | None
    MAXMWOUT: pl.Float32 | None


class LOSSMODELSchema(BasePartitionedSchema):
    """Loss model segments for interconnectors."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    LOSSSEGMENT: pl.Int32 | None
    MWBREAKPOINT: pl.Float32 | None


class LOSSFACTORMODELSchema(BasePartitionedSchema):
    """Loss factors by region on interconnectors."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    REGIONID: pl.Categorical | None
    DEMANDCOEFFICIENT: pl.Float32 | None


class MNSP_INTERCONNECTORSchema(BasePartitionedSchema):
    """Market Network Service Provider interconnector details."""

    INTERCONNECTORID: pl.Categorical
    LINKID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    FROMREGION: pl.Categorical | None
    TOREGION: pl.Categorical | None
    FROM_REGION_TLF: pl.Float32 | None
    TO_REGION_TLF: pl.Float32 | None
    LHSFACTOR: pl.Float32 | None
    MAXCAPACITY: pl.Float32 | None


# Constraint Tables
# =================


class GENCONDATASchema(BasePartitionedSchema):
    """Generic constraint definitions and weighting."""

    GENCONID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    CONSTRAINTTYPE: pl.Categorical | None
    GENERICCONSTRAINTWEIGHT: pl.Float32 | None


class SPDREGIONCONSTRAINTSchema(BasePartitionedSchema):
    """Regional constraints on specific dispatch unit types."""

    REGIONID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    BIDTYPE: pl.Categorical
    FACTOR: pl.Float32 | None


class SPDCONNECTIONPOINTCONSTRAINTSchema(BasePartitionedSchema):
    """Connection point constraints on specific dispatch unit types."""

    CONNECTIONPOINTID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    BIDTYPE: pl.Categorical
    FACTOR: pl.Float32 | None


class SPDINTERCONNECTORCONSTRAINTSchema(BasePartitionedSchema):
    """Interconnector constraints on specific dispatch unit types."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    FACTOR: pl.Float32 | None


# DNSP Tables (Not in standard DTYPES)
# ====================================
# Note: ZONE_SUBSTATION uses DNSP-specific columns not in DTYPES.
# Types derived from usage in DNSPDataSource.


class ZONESUBSTATIONSchema(BasePartitionedSchema):
    """Distribution network zone substation data from DNSP operators."""

    time: pl.String | None  # DNSP-specific, not in standard DTYPES
    zss: pl.String | None  # DNSP-specific, not in standard DTYPES
    MW: pl.Float32 | None
    network: pl.String | None


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
    # DNSP Tables
    "ZONE_SUBSTATION": ZONESUBSTATIONSchema,
}


# Type Extraction
# ==============


def _schema_to_dtypes(schema_class: type[BasePartitionedSchema]) -> dict[str, type]:
    """Extract Polars column types from a Pandera schema, unwrapping optional unions.

    Pandera schemas use `pl.X | None` for optional fields. This function extracts
    the bare Polars type (e.g., `pl.Float32`) for use in `.cast()` operations.
    Required fields like `pl.Datetime` are returned as-is.

    Args:
        schema_class: A Pandera DataFrameModel subclass

    Returns:
        dict mapping column name -> bare Polars type (without | None wrapper)

    Example:
        >>> dtypes = _schema_to_dtypes(DispatchRegionSumSchema)
        >>> dtypes['TOTALDEMAND']  # returns pl.Float32, not pl.Float32 | None
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
        # Handle X | None (types.UnionType) or typing.Union[X, None]
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
