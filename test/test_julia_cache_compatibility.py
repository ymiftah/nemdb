"""Exercise the Julia/DuckDB storage contract without a Julia install or live cache."""

import json
from datetime import date, datetime
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from nemdb.config import config
from nemdb.nemweb.dbloader import NEMWEBManager, _archive_to_df
from nemdb.nemweb.schemas import SCHEMA_MAP, BasePartitionedSchema, _schema_to_dtypes

JULIA_TABLES = json.loads(
    (Path(__file__).parent / "fixtures" / "julia_nemweb_schemas.json").read_text()
)["tables"]


def julia_frame(table):
    """Build typed input from an independent snapshot of the Julia cache schemas."""
    values = {
        "String": "NSW1",
        "Int8": 0,
        "Int32": 1,
        "Float32": 1.25,
        "Boolean": True,
        "Date": date(2025, 1, 1),
        "Datetime": datetime(2025, 1, 1, 12, 5),
    }
    columns = JULIA_TABLES[table]["columns"]
    data = {column: [values[dtype]] for column, dtype in columns.items()}
    if table == "GENUNITS":
        # Julia's cached GENUNITS includes units with no station association.
        data["STATIONID"] = [None]
    return pl.DataFrame(data, schema={c: getattr(pl, t) for c, t in columns.items()})


@pytest.mark.parametrize("table", JULIA_TABLES)
def test_julia_cache_rows_validate_without_casting(table):
    """Identifier strings and nullable station associations must validate as stored."""
    df = julia_frame(table).with_columns(pl.lit(date(2025, 1, 1)).alias("archive_month"))
    assert_frame_equal(SCHEMA_MAP[table].validate(df), df)


@pytest.mark.parametrize("table", JULIA_TABLES)
def test_python_archive_types_match_julia_cache(table, tmp_path):
    """Python must emit the same string/numeric/date types when parsing AEMO CSVs."""
    expected = julia_frame(table)
    archive = tmp_path / "archive.csv"
    archive.write_text(
        "C,NEMWEB\n"
        + expected.write_csv(datetime_format="%Y/%m/%d %H:%M:%S", date_format="%Y/%m/%d 00:00:00")
        + "C,END OF REPORT\n"
    )
    schema = SCHEMA_MAP[table]
    result = _archive_to_df(
        str(archive), list(expected.columns), _schema_to_dtypes(schema), 2025, 1
    )
    assert_frame_equal(result.select(expected.columns), expected)


@pytest.mark.parametrize("legacy_first", [False, True])
def test_scan_mixed_julia_and_legacy_python_identifiers(tmp_path, legacy_first):
    """A categorical partition must not prevent reading string partitions."""
    config.cache_dir = tmp_path
    ds = NEMWEBManager().DISPATCHREGIONSUM
    df = julia_frame("DISPATCHREGIONSUM")
    legacy = df.cast({"REGIONID": pl.Categorical})
    frames = [legacy, df] if legacy_first else [df, legacy]
    for month, frame in enumerate(frames, start=1):
        partition = Path(ds.path) / f"archive_month=2025-{month:02d}-01"
        partition.mkdir()
        frame.write_parquet(partition / "data_0.parquet", use_pyarrow=True)
    result = ds.scan().collect()
    assert result["REGIONID"].to_list() == ["NSW1", "NSW1"]
    assert result.schema["REGIONID"] == pl.String
    ds.schema_class.validate(result)


@pytest.mark.parametrize(
    ("table", "missing_columns"),
    [
        ("DISPATCHREGIONSUM", ["RUNNO", "INTERVENTION"]),
        ("GENCONDATA", ["CONSTRAINTVALUE", "AUTHORISEDDATE", "SSM_GROUPID"]),
        ("GENCONSET", ["GENCONEFFDATE", "GENCONVERSIONNO"]),
        ("GENCONSETTRK", ["MODIFICATIONS"]),
    ],
)
def test_scan_julia_source_subset_inserts_typed_columns(tmp_path, table, missing_columns):
    """Older Julia table definitions must not hide columns in later partitions."""
    config.cache_dir = tmp_path
    ds = getattr(NEMWEBManager(), table)
    df = julia_frame(table)
    for month, frame in [
        (1, df.select(JULIA_TABLES[table]["source_columns"])),
        (2, df),
    ]:
        partition = Path(ds.path) / f"archive_month=2025-{month:02d}-01"
        partition.mkdir()
        frame.write_parquet(partition / "data_0.parquet")
    result = ds.scan().collect().sort("archive_month")
    for column in missing_columns:
        assert result[column].to_list() == [None, df[column][0]]
    ds.schema_class.validate(result)


@pytest.mark.parametrize(
    "table", ["DISPATCH_FCAS_REQ", "DISPATCH_FCAS_REQ_CONSTRAINT", "DISPATCH_FCAS_REQ_RUN"]
)
def test_fcas_tables_read_julia_parquet(tmp_path, table):
    """All three registered FCAS sources must return actual cached rows."""
    config.cache_dir = tmp_path
    ds = getattr(NEMWEBManager(), table)
    expected = julia_frame(table).with_columns(pl.lit(date(2025, 1, 1)).alias("archive_month"))
    partition = Path(ds.path) / "archive_month=2025-01-01"
    partition.mkdir()
    expected.drop("archive_month").write_parquet(partition / "data_0.parquet")
    if table == "DISPATCH_FCAS_REQ_RUN":
        result = ds.get_data()
    else:
        result = ds.get_data("2025/01/01 12:05:00")
    assert_frame_equal(result.select(expected.columns), expected)
    ds.schema_class.validate(result)


def test_dtype_extraction_before_schema_initialization():
    """A fresh subclass must expose its own fields, not only cached base fields."""

    class FreshSchema(BasePartitionedSchema):
        DUID: pl.String = pa.Field()
        INITIALMW: pl.Float32 | None = pa.Field(nullable=True)

    assert _schema_to_dtypes(FreshSchema) == {
        "archive_month": pl.Date,
        "DUID": pl.String,
        "INITIALMW": pl.Float32,
    }
