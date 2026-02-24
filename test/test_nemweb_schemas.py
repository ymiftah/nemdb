"""Tests for NEMWEB Pandera schemas."""

import tempfile
from datetime import datetime
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

from nemdb import Config
from nemdb.nemweb.dbloader import DataSource, NEMWEBManager
from nemdb.nemweb.schemas import (
    SCHEMA_MAP,
    BidDayOfferDSchema,
    BidPerOfferDSchema,
    DispatchConstraintSchema,
    DispatchInterconnectorResSchema,
    DispatchLoadSchema,
    DispatchPriceSchema,
    DispatchRegionSumSchema,
    DUALLOCSchema,
    DUDETAILSchema,
    DUDETAILSUMMARYSchema,
    GENCONDATASchema,
    GENUNITSSchema,
    INTERCONNECTORCONSTRAINTSchema,
    INTERCONNECTORSchema,
    LOSSFACTORMODELSchema,
    LOSSMODELSchema,
    MNSP_INTERCONNECTORSchema,
    RESERVESchema,
    SPDCONNECTIONPOINTCONSTRAINTSchema,
    SPDINTERCONNECTORCONSTRAINTSchema,
    SPDREGIONCONSTRAINTSchema,
    STADUALLOCSchema,
    STATIONOPERATINGSTATUSSchema,
    STATIONOWNERSchema,
    STATIONSchema,
    ZONESUBSTATIONSchema,
    _schema_to_dtypes,
    validate_against_schema,
)


def test_dispatch_region_sum_schema_fields():
    """Verify DispatchRegionSumSchema has expected fields."""
    fields = DispatchRegionSumSchema.__fields__
    assert "SETTLEMENTDATE" in fields
    assert "REGIONID" in fields
    assert "TOTALDEMAND" in fields


def test_dispatch_load_schema_fields():
    """Verify DispatchLoadSchema has expected fields."""
    fields = DispatchLoadSchema.__fields__
    assert "SETTLEMENTDATE" in fields
    assert "DUID" in fields
    assert "UIGF" in fields


def test_bid_day_offer_schema_fields():
    """Verify BidDayOfferDSchema has expected fields."""
    fields = BidDayOfferDSchema.__fields__
    assert "DUID" in fields
    assert "SETTLEMENTDATE" in fields
    assert "PRICEBAND10" in fields


def test_generation_schemas_fields():
    """Verify generation-related schemas have expected fields."""
    genunits_fields = GENUNITSSchema.__fields__
    dudetail_fields = DUDETAILSchema.__fields__

    assert "GENSETID" in genunits_fields
    assert "DUID" in dudetail_fields
    assert "MAXCAPACITY" in genunits_fields
    assert "MAXCAPACITY" in dudetail_fields


def test_station_schemas_fields():
    """Verify station-related schemas have expected fields."""
    station_fields = STATIONSchema.__fields__
    stadualloc_fields = STADUALLOCSchema.__fields__

    assert "STATIONID" in station_fields
    assert "STATIONNAME" in station_fields
    assert "DUID" in stadualloc_fields
    assert "VERSIONNO" in stadualloc_fields


def test_interconnector_schemas_fields():
    """Verify interconnector-related schemas have expected fields."""
    ic_fields = INTERCONNECTORSchema.__fields__
    loss_fields = LOSSMODELSchema.__fields__

    assert "INTERCONNECTORID" in ic_fields
    assert "REGIONFROM" in ic_fields
    assert "LOSSSEGMENT" in loss_fields


def test_constraint_schemas_fields():
    """Verify constraint-related schemas have expected fields."""
    gencon_fields = GENCONDATASchema.__fields__
    spdregion_fields = SPDREGIONCONSTRAINTSchema.__fields__

    assert "GENCONID" in gencon_fields
    assert "CONSTRAINTTYPE" in gencon_fields
    assert "BIDTYPE" in spdregion_fields
    assert "FACTOR" in spdregion_fields


def test_all_schemas_have_fields():
    """Test that all 26 schema classes have defined fields."""
    schemas = [
        DispatchRegionSumSchema,
        DispatchLoadSchema,
        DispatchPriceSchema,
        DispatchConstraintSchema,
        DispatchInterconnectorResSchema,
        BidDayOfferDSchema,
        BidPerOfferDSchema,
        DUALLOCSchema,
        GENUNITSSchema,
        DUDETAILSUMMARYSchema,
        DUDETAILSchema,
        RESERVESchema,
        STATIONSchema,
        STATIONOPERATINGSTATUSSchema,
        STATIONOWNERSchema,
        STADUALLOCSchema,
        INTERCONNECTORSchema,
        INTERCONNECTORCONSTRAINTSchema,
        LOSSMODELSchema,
        LOSSFACTORMODELSchema,
        MNSP_INTERCONNECTORSchema,
        GENCONDATASchema,
        SPDREGIONCONSTRAINTSchema,
        SPDCONNECTIONPOINTCONSTRAINTSchema,
        SPDINTERCONNECTORCONSTRAINTSchema,
        ZONESUBSTATIONSchema,
    ]
    assert len(schemas) == 26
    for schema_class in schemas:
        assert hasattr(schema_class, "__fields__")
        assert len(schema_class.__fields__) > 0


def test_sample_dispatch_region_sum_data():
    """Test validation with sample DISPATCHREGIONSUM data."""
    df = pl.DataFrame(
        {
            "SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)],
            "REGIONID": ["NSW1"],
            "TOTALDEMAND": [10000.0],
            "DEMANDFORECAST": [10100.0],
            "DISPATCHABLELOAD": [8000.0],
            "INITIALSUPPLY": [9800.0],
            "SS_SOLAR_AVAILABILITY": [2000.0],
            "SS_WIND_AVAILABILITY": [1500.0],
            "AVAILABLEGENERATION": [9500.0],
            "AVAILABLELOAD": [8500.0],
        }
    )
    # Just verify it's a valid DataFrame for the schema
    assert df is not None
    assert "SETTLEMENTDATE" in df.columns
    assert len(df) == 1


def test_schema_map_exists_and_is_complete():
    """Test that SCHEMA_MAP registry exists and contains all 26 schemas."""
    # Should have 26 entries
    assert len(SCHEMA_MAP) == 26, f"SCHEMA_MAP has {len(SCHEMA_MAP)} entries, expected 26"

    # All values should be schema classes
    for table_name, schema_class in SCHEMA_MAP.items():
        assert isinstance(table_name, str)
        assert issubclass(schema_class, pa.DataFrameModel), (
            f"{table_name}: schema_class {schema_class} is not a DataFrameModel"
        )


def test_schema_fields_match_table_columns():
    """Verify table_columns are correctly derived from schema in DataSource."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config()
        config.CACHE_DIR = Path(tmp_dir)
        config.TEMP_DIR = Path(tmp_dir)
        manager = NEMWEBManager(config)

        # For each DataSource with a schema, verify table_columns match schema fields
        for ds_attr in dir(manager):
            if ds_attr.startswith("_"):
                continue
            obj = getattr(manager, ds_attr)
            if not hasattr(obj, "table_name") or not hasattr(obj, "table_columns"):
                continue

            table_name = obj.table_name
            if table_name not in SCHEMA_MAP:
                continue

            # Get schema fields
            schema_class = SCHEMA_MAP[table_name]
            schema_fields = set(schema_class.__fields__.keys())
            table_columns = set(obj.table_columns)

            # Should be identical now (table_columns derived from schema)
            assert schema_fields == table_columns, (
                f"{table_name}: Schema fields {schema_fields} do not match "
                f"table columns {table_columns}"
            )


def test_datasource_schema_class_attribute():
    """Verify all NEMWEBManager DataSources have schema_class attribute (now required)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config()
        config.CACHE_DIR = Path(tmp_dir)
        config.TEMP_DIR = Path(tmp_dir)
        manager = NEMWEBManager(config)

        # Check all DataSources in manager
        checked_count = 0
        for ds_attr in dir(manager):
            if ds_attr.startswith("_"):
                continue
            obj = getattr(manager, ds_attr)
            # Only check DataSource instances (skip DNSPDataSource, etc)
            if not isinstance(obj, DataSource):
                continue

            # DataSources should have schema_class attribute (now required)
            assert hasattr(obj, "schema_class"), (
                f"{obj.table_name} DataSource missing schema_class attribute"
            )
            # Since schema_class is now required, check it's a type
            assert isinstance(obj.schema_class, type), (
                f"{obj.table_name} schema_class should be a type"
            )
            checked_count += 1

        # Should have checked at least 25 DataSources (excludes ZONE_SUBSTATION which is DNSPDataSource)
        assert checked_count >= 25, (
            f"Only checked {checked_count} DataSources, expected at least 25"
        )


def test_validate_against_schema_valid_data():
    """Test validate_against_schema with valid data."""
    # Create valid DataFrame matching schema
    df = pl.DataFrame(
        {
            "SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)],
            "REGIONID": ["NSW1"],
            "TOTALDEMAND": [1000.0],
            "DEMANDFORECAST": [1050.0],
            "DISPATCHABLELOAD": [900.0],
            "INITIALSUPPLY": [950.0],
            "SS_SOLAR_AVAILABILITY": [100.0],
            "SS_WIND_AVAILABILITY": [50.0],
            "AVAILABLEGENERATION": [1100.0],
            "AVAILABLELOAD": [950.0],
        }
    ).cast(
        {
            "SETTLEMENTDATE": pl.Datetime,
            "REGIONID": pl.Categorical,
            "TOTALDEMAND": pl.Float32,
            "DEMANDFORECAST": pl.Float32,
            "DISPATCHABLELOAD": pl.Float32,
            "INITIALSUPPLY": pl.Float32,
            "SS_SOLAR_AVAILABILITY": pl.Float32,
            "SS_WIND_AVAILABILITY": pl.Float32,
            "AVAILABLEGENERATION": pl.Float32,
            "AVAILABLELOAD": pl.Float32,
        }
    )

    result = validate_against_schema(df, DispatchRegionSumSchema)
    assert result is True


def test_validate_against_schema_invalid_data():
    """Test validate_against_schema with invalid data (missing required field)."""
    # Create invalid DataFrame (missing REGIONID)
    df = pl.DataFrame(
        {
            "SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)],
            # Missing REGIONID - required field
            "TOTALDEMAND": [1000.0],
        }
    ).cast(
        {
            "SETTLEMENTDATE": pl.Datetime,
            "TOTALDEMAND": pl.Float32,
        }
    )

    result = validate_against_schema(df, DispatchRegionSumSchema, raise_on_error=False)
    assert result is False


def test_validate_against_schema_raises_on_error():
    """Test validate_against_schema raises exception when requested."""
    df = pl.DataFrame(
        {
            "SETTLEMENTDATE": [],
            "TOTALDEMAND": [],
        }
    ).cast(
        {
            "SETTLEMENTDATE": pl.Datetime,
            "TOTALDEMAND": pl.Float32,
        }
    )

    with pytest.raises(pa.errors.SchemaError):
        validate_against_schema(df, DispatchRegionSumSchema, raise_on_error=True)


def test_schema_to_dtypes():
    """Test that _schema_to_dtypes correctly extracts Polars types from schema annotations."""
    dtypes = _schema_to_dtypes(DispatchRegionSumSchema)

    # Check that we get Polars types (not unions with None)
    assert isinstance(dtypes, dict)
    assert "SETTLEMENTDATE" in dtypes
    assert dtypes["SETTLEMENTDATE"] == pl.Datetime  # required field, no union
    assert "TOTALDEMAND" in dtypes
    assert dtypes["TOTALDEMAND"] == pl.Float32  # optional field, union unwrapped
    assert dtypes["REGIONID"] == pl.Categorical

    # Should have exactly the expected schema fields
    expected_fields = {
        "SETTLEMENTDATE",
        "REGIONID",
        "TOTALDEMAND",
        "DEMANDFORECAST",
        "DISPATCHABLELOAD",
        "INITIALSUPPLY",
        "SS_SOLAR_AVAILABILITY",
        "SS_WIND_AVAILABILITY",
        "AVAILABLEGENERATION",
        "AVAILABLELOAD",
    }
    assert set(dtypes.keys()) == expected_fields
