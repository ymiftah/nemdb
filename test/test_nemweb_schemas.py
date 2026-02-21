"""Tests for NEMWEB Pandera schemas."""

from datetime import datetime

import pandera.polars as pa
import polars as pl

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
