from datetime import datetime
from pathlib import Path

import pandas as pd
import pandera as pa
import polars as pl
import pytest

from nemdb import Config
from nemdb.nemweb.dbloader import (
    ByEffectiveDateVersionNo,
    ByIntervalDate,
    BySettlementDate,
    BySettlementDay,
    ByStartEnd,
    DataSource,
    NEMWEBManager,
    _archive_to_df,
    _get_archive,
    _MissingData,
)
from nemdb.nemweb.schemas import (
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
)


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    Config.CACHE_DIR = Path(tmp_path)
    Config.TEMP_DIR = Path(tmp_path)
    return tmp_path


@pytest.fixture
def mock_config(temp_dir):
    """Mock the Config object."""
    config = Config()
    config.CACHE_DIR = Path(temp_dir)
    config.TEMP_DIR = Path(temp_dir)
    return config


def test_nemweb_manager_init(mock_config):
    """Test NEMWEBManager initialization."""
    manager = NEMWEBManager(mock_config)
    assert manager.config == mock_config
    assert "DISPATCHREGIONSUM" in manager.active_tables()


def test_nemweb_manager_datasources_have_schemas(mock_config):
    """Test that NEMWEBManager DataSources are wired to their schemas."""
    manager = NEMWEBManager(mock_config)

    # Test all 26 DataSources
    assert manager.DUALLOC.schema_class == DUALLOCSchema
    assert manager.GENUNITS.schema_class == GENUNITSSchema
    assert manager.RESERVE.schema_class == RESERVESchema
    assert manager.DISPATCHREGIONSUM.schema_class == DispatchRegionSumSchema
    assert manager.DISPATCHLOAD.schema_class == DispatchLoadSchema
    assert manager.DISPATCHPRICE.schema_class == DispatchPriceSchema
    assert manager.DUDETAILSUMMARY.schema_class == DUDETAILSUMMARYSchema
    assert manager.DUDETAIL.schema_class == DUDETAILSchema
    assert manager.STATION.schema_class == STATIONSchema
    assert manager.STATIONOPERATINGSTATUS.schema_class == STATIONOPERATINGSTATUSSchema
    assert manager.STATIONOWNER.schema_class == STATIONOWNERSchema
    assert manager.STADUALLOC.schema_class == STADUALLOCSchema
    assert manager.BIDDAYOFFER_D.schema_class == BidDayOfferDSchema
    assert manager.BIDPEROFFER_D.schema_class == BidPerOfferDSchema
    assert manager.DISPATCHCONSTRAINT.schema_class == DispatchConstraintSchema
    assert manager.GENCONDATA.schema_class == GENCONDATASchema
    assert manager.SPDREGIONCONSTRAINT.schema_class == SPDREGIONCONSTRAINTSchema
    assert manager.SPDCONNECTIONPOINTCONSTRAINT.schema_class == SPDCONNECTIONPOINTCONSTRAINTSchema
    assert manager.SPDINTERCONNECTORCONSTRAINT.schema_class == SPDINTERCONNECTORCONSTRAINTSchema
    assert manager.INTERCONNECTOR.schema_class == INTERCONNECTORSchema
    assert manager.INTERCONNECTORCONSTRAINT.schema_class == INTERCONNECTORCONSTRAINTSchema
    assert manager.LOSSMODEL.schema_class == LOSSMODELSchema
    assert manager.LOSSFACTORMODEL.schema_class == LOSSFACTORMODELSchema
    assert manager.DISPATCHINTERCONNECTORRES.schema_class == DispatchInterconnectorResSchema
    assert manager.MNSP_INTERCONNECTOR.schema_class == MNSP_INTERCONNECTORSchema


def test_get_unit_volume_bids(mocker, mock_config):
    """Test get_unit_volume_bids method."""
    manager = NEMWEBManager(mock_config)
    manager.read_bids.cache_clear()
    mocker.patch(
        "nemdb.nemweb.dbloader.read_bids",
        return_value=(
            None,
            pl.DataFrame(
                {
                    "ROCUP": [1],
                    "ROCDOWN": [1],
                    "INTERVAL_DATETIME": [datetime(2024, 1, 1, 12, 0)],
                    "DUID": ["A"],
                    "BIDTYPE": ["B"],
                    "MAXAVAIL": [1],
                    "FIXEDLOAD": [1],
                    "ENABLEMENTMIN": [1],
                    "ENABLEMENTMAX": [1],
                    "LOWBREAKPOINT": [1],
                    "HIGHBREAKPOINT": [1],
                    "BANDAVAIL1": [1],
                    "BANDAVAIL2": [1],
                    "BANDAVAIL3": [1],
                    "BANDAVAIL4": [1],
                    "BANDAVAIL5": [1],
                    "BANDAVAIL6": [1],
                    "BANDAVAIL7": [1],
                    "BANDAVAIL8": [1],
                    "BANDAVAIL9": [1],
                    "BANDAVAIL10": [1],
                }
            ),
        ),
    )
    df = manager.get_unit_volume_bids("2024/01/01 12:00:00")
    assert "RAMPUPRATE" in df.columns
    assert "RAMPDOWNRATE" in df.columns


def test_get_unit_price_bids(mocker, mock_config):
    """Test get_unit_price_bids method."""
    manager = NEMWEBManager(mock_config)
    manager.read_bids.cache_clear()
    mocker.patch(
        "nemdb.nemweb.dbloader.read_bids",
        return_value=(
            pl.DataFrame(
                {
                    "SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)],
                    "DUID": ["A"],
                    "BIDTYPE": ["B"],
                    "PRICEBAND1": [1],
                    "PRICEBAND2": [1],
                    "PRICEBAND3": [1],
                    "PRICEBAND4": [1],
                    "PRICEBAND5": [1],
                    "PRICEBAND6": [1],
                    "PRICEBAND7": [1],
                    "PRICEBAND8": [1],
                    "PRICEBAND9": [1],
                    "PRICEBAND10": [1],
                }
            ),
            None,
        ),
    )
    df = manager.get_unit_price_bids("2024/01/01 12:00:00")
    assert "PRICEBAND1" in df.columns


def test_get_archive_success(mocker):
    """Test _get_archive on success."""
    mocker.patch("nemdb.nemweb.dbloader.cache_response_zip", return_value="path/to/file.zip")
    path = _get_archive("TABLE", 2024, 1)
    assert path == "path/to/file.zip"


def test_get_archive_failure(mocker):
    """Test _get_archive on failure."""
    mocker.patch("nemdb.nemweb.dbloader.cache_response_zip", side_effect=ValueError)
    with pytest.raises(_MissingData):
        _get_archive("TABLE", 2024, 1)


def test_archive_to_df(mocker, tmp_path):
    """Test _archive_to_df function."""
    mocker.patch("nemdb.nemweb.dbloader.read_header", return_value={"a", "b"})
    mocker.patch("pandas.read_csv", return_value=pd.DataFrame({"a": [1], "b": [2]}))
    mocker.patch.dict("nemdb.nemweb.dbloader.DTYPES", {"a": int, "b": int})
    df = _archive_to_df(str(tmp_path / "test.zip"), ["a", "b"], 2024, 1)
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["a", "b"]


def test_data_source_init(mock_config):
    """Test DataSource init derives table_columns and types from schema_class."""
    ds = DataSource(
        mock_config,
        "DISPATCHREGIONSUM",
        schema_class=DispatchRegionSumSchema,
        table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
    )
    assert ds.table_name == "DISPATCHREGIONSUM"
    # table_columns should be derived from schema
    assert set(ds.table_columns) == set(DispatchRegionSumSchema.__fields__.keys())
    assert ds.table_primary_keys == ["SETTLEMENTDATE", "REGIONID"]
    assert ds.schema_class == DispatchRegionSumSchema


def test_data_source_populate(mocker, mock_config):
    """Test DataSource populate method."""

    class SimpleSchema(pa.DataFrameModel):
        a: pl.Int64
        b: pl.Int64

    mock_add_data = mocker.patch("nemdb.nemweb.dbloader.DataSource.add_data")
    ds = DataSource(mock_config, "TABLE", schema_class=SimpleSchema, table_primary_keys=["a"])
    ds.populate(slice("2024-01-01", "2024-01-31"))
    mock_add_data.assert_called_once_with(year=2024, month=1)


def test_by_settlement_date(mocker, mock_config):
    """Test BySettlementDate get_data method."""

    class SimpleSettlementSchema(pa.DataFrameModel):
        SETTLEMENTDATE: pl.Datetime
        a: pl.Int64

    ds = BySettlementDate(
        mock_config, "TABLE", schema_class=SimpleSettlementSchema, table_primary_keys=["a"]
    )
    mock_scan = mocker.patch.object(ds, "scan")
    mock_scan.return_value.filter.return_value.collect.return_value = pl.DataFrame(
        {"SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)], "a": [1]}
    )
    df = ds.get_data("2024/01/01 12:00:00")
    assert df.shape == (1, 2)


def test_by_interval_date(mocker, mock_config):
    """Test ByIntervalDate get_data method."""

    class SimpleIntervalSchema(pa.DataFrameModel):
        INTERVAL_DATETIME: pl.Datetime
        a: pl.Int64

    ds = ByIntervalDate(
        mock_config, "TABLE", schema_class=SimpleIntervalSchema, table_primary_keys=["a"]
    )
    mock_scan = mocker.patch.object(ds, "scan")
    mock_scan.return_value.filter.return_value.collect.return_value = pl.DataFrame(
        {"INTERVAL_DATETIME": [datetime(2024, 1, 1, 12, 0)], "a": [1]}
    )
    df = ds.get_data("2024/01/01 12:00:00")
    assert df.shape == (1, 2)


def test_by_settlement_day(mocker, mock_config):
    """Test BySettlementDay get_data method."""

    class SimpleSettlementDaySchema(pa.DataFrameModel):
        SETTLEMENTDATE: pl.Date
        a: pl.Int64

    ds = BySettlementDay(
        mock_config, "TABLE", schema_class=SimpleSettlementDaySchema, table_primary_keys=["a"]
    )
    mock_scan = mocker.patch.object(ds, "scan")
    mock_scan.return_value.filter.return_value.collect.return_value = pl.DataFrame(
        {"SETTLEMENTDATE": [datetime(2024, 1, 1)], "a": [1]}
    )
    df = ds.get_data("2024/01/01")
    assert df.shape == (1, 2)


def test_by_start_end(mocker, mock_config):
    """Test ByStartEnd get_data method."""

    class SimpleStartEndSchema(pa.DataFrameModel):
        START_DATE: pl.Date
        END_DATE: pl.Date
        a: pl.Int64

    ds = ByStartEnd(
        mock_config, "TABLE", schema_class=SimpleStartEndSchema, table_primary_keys=["a"]
    )
    mock_scan = mocker.patch.object(ds, "scan")
    mock_scan.return_value.filter.return_value.collect.return_value = pl.DataFrame(
        {
            "START_DATE": [datetime(2024, 1, 1)],
            "END_DATE": [datetime(2024, 1, 31)],
            "a": [1],
        }
    )
    df = ds.get_data("2024/01/15")
    assert df.shape == (1, 3)


def test_by_effective_date_version_no(mocker, mock_config):
    """Test ByEffectiveDateVersionNo get_data method."""

    class SimpleEffectiveDateSchema(pa.DataFrameModel):
        EFFECTIVEDATE: pl.Date
        VERSIONNO: pl.Int64
        a: pl.Int64

    ds = ByEffectiveDateVersionNo(
        mock_config, "TABLE", schema_class=SimpleEffectiveDateSchema, table_primary_keys=["a"]
    )
    mock_scan = mocker.patch.object(ds, "scan")
    (
        mock_scan.return_value.filter.return_value.sort.return_value.unique.return_value.collect.return_value
    ) = pl.DataFrame({"EFFECTIVEDATE": [datetime(2024, 1, 1)], "VERSIONNO": [1], "a": [1]})
    df = ds.get_data("2024/01/15")
    assert df.shape == (1, 3)


def test_data_source_with_schema_class(mock_config):
    """Test DataSource accepts and stores schema_class parameter."""
    ds = DataSource(
        mock_config,
        "DISPATCHREGIONSUM",
        schema_class=DispatchRegionSumSchema,
        table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
    )
    assert ds.schema_class == DispatchRegionSumSchema
