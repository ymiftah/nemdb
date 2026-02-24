# Schema Single Source of Truth Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Remove duplication of `table_columns` and types by deriving them from `schema_class` annotations, eliminating the DTYPES dictionary.

**Architecture:** Add `_schema_to_dtypes()` helper to extract column types from Pandera schema annotations (unwrapping `X | None` unions). Modify `DataSource.__init__` to derive `table_columns` and `_dtypes` from `schema_class`. Update `_archive_to_df` and `_archive_to_df_low_memory` to accept `dtypes` parameter instead of looking up DTYPES. Simplify all 26 NEMWEBManager DataSource instantiations by removing `table_columns=` arguments.

**Tech Stack:** Pandera 0.26.1+, Polars, Python 3.10+ union syntax, `typing.get_type_hints()`, `types.UnionType`

---

## Task 1: Add `_schema_to_dtypes()` Helper to schemas.py

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (add after SCHEMA_MAP definition, before or after validate_against_schema)

### Step 1: Write test for _schema_to_dtypes()

Add to `test/test_nemweb_schemas.py`:

```python
def test_schema_to_dtypes():
    """Test that _schema_to_dtypes correctly extracts Polars types from schema annotations."""
    from nemdb.nemweb.schemas import _schema_to_dtypes, DispatchRegionSumSchema
    import polars as pl

    dtypes = _schema_to_dtypes(DispatchRegionSumSchema)

    # Check that we get Polars types (not unions with None)
    assert isinstance(dtypes, dict)
    assert "SETTLEMENTDATE" in dtypes
    assert dtypes["SETTLEMENTDATE"] == pl.Datetime  # required field, no union
    assert "TOTALDEMAND" in dtypes
    assert dtypes["TOTALDEMAND"] == pl.Float32  # optional field, union unwrapped
    assert dtypes["REGIONID"] == pl.Categorical

    # Should have all schema fields
    assert set(dtypes.keys()) == set(DispatchRegionSumSchema.__fields__.keys())
```

### Step 2: Run test to verify it fails

```bash
cd /home/simba/workspace/nemdb
pytest test/test_nemweb_schemas.py::test_schema_to_dtypes -v
```

Expected: FAIL with "ImportError: cannot import name '_schema_to_dtypes'"

### Step 3: Write _schema_to_dtypes() implementation

Add to `src/nemdb/nemweb/schemas.py` (after SCHEMA_MAP, before validate_against_schema function):

```python
# Type Extraction
# ==============


def _schema_to_dtypes(schema_class: type[pa.DataFrameModel]) -> dict[str, type]:
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
    import types
    import typing

    result = {}
    for name, annotation in typing.get_type_hints(schema_class).items():
        origin = typing.get_origin(annotation)
        # Handle X | None (types.UnionType) or typing.Union[X, None]
        if origin is types.UnionType or origin is typing.Union:
            # Get union args and filter out NoneType
            args = [a for a in typing.get_args(annotation) if a is not type(None)]
            result[name] = args[0] if args else annotation
        else:
            # Not a union - bare type like pl.Datetime
            result[name] = annotation
    return result
```

### Step 4: Run test to verify it passes

```bash
pytest test/test_nemweb_schemas.py::test_schema_to_dtypes -v
```

Expected: PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
git commit -m "feat: add _schema_to_dtypes() helper to extract types from schema annotations"
```

---

## Task 2: Modify DataSource.**init** to Derive table_columns and Types from schema_class

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:1145-1182` (DataSource.**init**)
- Modify: `test/test_nemweb_dbloader.py:201-206` (test_data_source_init)

### Step 1: Update test_data_source_init to use real schema

In `test/test_nemweb_dbloader.py`, replace lines 201-206:

```python
def test_data_source_init(mock_config):
    """Test DataSource init derives table_columns and types from schema_class."""
    from nemdb.nemweb.schemas import DispatchRegionSumSchema

    ds = DataSource(mock_config, "DISPATCHREGIONSUM", schema_class=DispatchRegionSumSchema, table_primary_keys=["SETTLEMENTDATE", "REGIONID"])
    assert ds.table_name == "DISPATCHREGIONSUM"
    # table_columns should be derived from schema
    assert set(ds.table_columns) == set(DispatchRegionSumSchema.__fields__.keys())
    assert ds.table_primary_keys == ["SETTLEMENTDATE", "REGIONID"]
    assert ds.schema_class == DispatchRegionSumSchema
```

### Step 2: Run test to verify it fails

```bash
pytest test/test_nemweb_dbloader.py::test_data_source_init -v
```

Expected: FAIL with "TypeError: **init**() missing required keyword-only argument 'schema_class'" or similar

**Step 3: Modify DataSource.**init****

In `src/nemdb/nemweb/dbloader.py`, replace the `DataSource.__init__` method (lines 1145-1181):

```python
def __init__(
    self,
    config: type[Config],
    table_name: str,
    schema_class: type[pa.DataFrameModel],  # Now required, moved before optional params
    table_primary_keys: list[str] | None = None,
    add_partitions: list[str] | None = None,
    low_memory: bool = False,
):
    """Creates a parquet dataset.

    Args:
        config: Configuration class for cache and filesystem settings
        table_name: Name of the table (used as subdirectory in cache)
        schema_class: Pandera DataFrameModel schema for this table (defines columns and types)
        table_primary_keys: Optional list of primary key column names
        add_partitions: Optional list of additional partition columns
        low_memory: Whether to use lower memory mode for reading
    """
    from nemdb.nemweb.schemas import _schema_to_dtypes

    self.config = config
    self.table_name = table_name
    self.schema_class = schema_class
    # Derive table_columns from schema
    self.table_columns = list(schema_class.__fields__.keys())
    # Extract types from schema (unwrapping X | None unions to bare types)
    self._dtypes = _schema_to_dtypes(schema_class)
    self.table_primary_keys = table_primary_keys if table_primary_keys is not None else []
    self.partitions = (
        [*add_partitions, "archive_month"] if add_partitions is not None else ["archive_month"]
    )
    self.low_memory = low_memory

    self.path = f"{config.CACHE_DIR}/{table_name}/"
    self.fs = fsspec.filesystem(config.FILESYSTEM)
    self.fs.makedirs(f"{config.CACHE_DIR}/{table_name}", exist_ok=True)
```

### Step 4: Run test to verify it passes

```bash
pytest test/test_nemweb_dbloader.py::test_data_source_init -v
```

Expected: PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/dbloader.py test/test_nemweb_dbloader.py
git commit -m "feat: derive table_columns and types from schema_class in DataSource.__init__"
```

---

## Task 3: Update DataSource.fetch_data to Pass self._dtypes to _archive_to_df

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:1364-1376` (DataSource.fetch_data)
- Modify: `src/nemdb/nemweb/dbloader.py:1030-1036` (_archive_to_df signature)

### Step 1: Modify _archive_to_df signature

In `src/nemdb/nemweb/dbloader.py`, replace the `_archive_to_df` function signature (line 1030-1036):

```python
def _archive_to_df(
    archive: str,
    table_columns: list[str],
    dtypes: dict[str, type],  # NEW: replaces DTYPES lookups
    year: int,
    month: int,
    _low_memory: bool = False,
) -> pl.DataFrame:
    """Downloads a zipped csv file and converts it to a polars DataFrame.

    Args:
        archive: Path to CSV file (from ZIP)
        table_columns: List of column names to read
        dtypes: Dict mapping column names to Polars types
        year: Year of data
        month: Month of data
        _low_memory: Unused (for compatibility); low-memory handling is in add_data()

    Returns:
        Polars DataFrame with data cast to correct types
    """
```

### Step 2: Update _archive_to_df body to use dtypes parameter

In `src/nemdb/nemweb/dbloader.py`, replace lines 1093 and 1115:

Line 1093 (old: `table_dtypes = {k: DTYPES[k] for k in set(table_columns).intersection(available_cols)}`):

```python
    table_dtypes = {k: dtypes[k] for k in set(table_columns).intersection(available_cols)}
```

Line 1115 (old: `return pl.from_dataframe(data).cast({k: DTYPES[k] for k in set(table_columns)})`):

```python
    return pl.from_dataframe(data).cast({k: dtypes[k] for k in set(table_columns)})
```

### Step 3: Update DataSource.fetch_data to pass self._dtypes

In `src/nemdb/nemweb/dbloader.py`, line 1376 (in `fetch_data` method), change:

Old:

```python
        return _archive_to_df(archive, self.table_columns, year, month, _low_memory=self.low_memory)
```

New:

```python
        return _archive_to_df(archive, self.table_columns, self._dtypes, year, month, _low_memory=self.low_memory)
```

### Step 4: Run tests

```bash
cd /home/simba/workspace/nemdb
pytest test/test_nemweb_dbloader.py -v -k "test_data_source" --tb=short
```

Expected: Tests pass (test_data_source_init should still pass, others may vary)

### Step 5: Commit

```bash
git add src/nemdb/nemweb/dbloader.py
git commit -m "feat: update _archive_to_df to accept dtypes parameter instead of using DTYPES"
```

---

## Task 4: Update _archive_to_df_low_memory to Accept dtypes Parameter

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:1309-1362` (_archive_to_df_low_memory)
- Modify: `src/nemdb/nemweb/dbloader.py:1297-1307` (_add_data_low_memory)

### Step 1: Modify _archive_to_df_low_memory signature

In `src/nemdb/nemweb/dbloader.py`, find the `_archive_to_df_low_memory` method definition (line ~1309) and add `dtypes` parameter:

```python
def _archive_to_df_low_memory(self, archive, name, table_columns, dtypes, year, month, path, **kwargs):
    """Read CSV in chunks and write parquet, handling low-memory scenarios.

    Args:
        archive: Path to CSV file (from ZIP)
        name: Table name
        table_columns: List of column names
        dtypes: Dict mapping column names to Polars types
        year: Year of data
        month: Month of data
        path: Output parquet path
        **kwargs: Additional args for write_parquet
    """
```

### Step 2: Update _archive_to_df_low_memory body

Replace line 1314 (old: `table_dtypes = {k: DTYPES[k] for k in set(table_columns).intersection(available_cols)}`):

```python
    table_dtypes = {k: dtypes[k] for k in set(table_columns).intersection(available_cols)}
```

Replace line 1339 (old: `.cast({k: DTYPES[k] for k in set(table_columns)})`):

```python
    .cast({k: dtypes[k] for k in set(table_columns)})
```

### Step 3: Update _add_data_low_memory to pass self._dtypes

In `src/nemdb/nemweb/dbloader.py`, line ~1301 in `_add_data_low_memory` method, change:

Old:

```python
            self._archive_to_df_low_memory(
                archive, name, self.table_columns, year, month, self.path, **kwargs
            )
```

New:

```python
            self._archive_to_df_low_memory(
                archive, name, self.table_columns, self._dtypes, year, month, self.path, **kwargs
            )
```

### Step 4: Run tests

```bash
pytest test/test_nemweb_dbloader.py -v --tb=short
```

Expected: Tests pass

### Step 5: Commit

```bash
git add src/nemdb/nemweb/dbloader.py
git commit -m "feat: update _archive_to_df_low_memory to accept dtypes parameter"
```

---

## Task 5: Remove DTYPES Dictionary

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:57-265` (remove DTYPES dict)

### Step 1: Remove DTYPES

In `src/nemdb/nemweb/dbloader.py`, delete lines 57-265 (the entire DTYPES dictionary definition and its comment).

Lines to delete:

```python
DTYPES = {
    "DISPATCHABLELOAD": pl.Float32,
    # ... 200+ lines ...
    "AGGREGATED": pl.Int32,
}
```

### Step 2: Verify no other references to DTYPES in dbloader.py

```bash
grep -n "DTYPES" /home/simba/workspace/nemdb/src/nemdb/nemweb/dbloader.py
```

Expected: No matches (all references replaced in previous tasks)

### Step 3: Run tests

```bash
pytest test/test_nemweb_dbloader.py test/test_nemweb_schemas.py -v --tb=short
```

Expected: Tests pass

### Step 4: Commit

```bash
git add src/nemdb/nemweb/dbloader.py
git commit -m "feat: remove DTYPES dictionary (now derived from schemas)"
```

---

## Task 6: Simplify NEMWEBManager Instantiations (Remove table_columns Arguments)

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:397-883` (NEMWEBManager.**init**)

### Step 1: Edit all 26 DataSource instantiations

For each DataSource instance in NEMWEBManager.**init** (lines ~397-883), remove the `table_columns=[...]` argument. Keep `schema_class=`.

**Examples:**

Old DUALLOC (lines ~397-402):

```python
self.DUALLOC = DataSource(
    config=config,
    table_name="DUALLOC",
    table_columns=["DUID", "GENSETID", "LASTCHANGED", "VERSIONNO"],
    schema_class=DUALLOCSchema,
)
```

New DUALLOC:

```python
self.DUALLOC = DataSource(
    config=config,
    table_name="DUALLOC",
    schema_class=DUALLOCSchema,
)
```

Old DISPATCHREGIONSUM (lines ~443-460):

```python
self.DISPATCHREGIONSUM = BySettlementDate(
    config=config,
    table_name="DISPATCHREGIONSUM",
    table_columns=[
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
    ],
    table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
    schema_class=DispatchRegionSumSchema,
)
```

New DISPATCHREGIONSUM:

```python
self.DISPATCHREGIONSUM = BySettlementDate(
    config=config,
    table_name="DISPATCHREGIONSUM",
    table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
    schema_class=DispatchRegionSumSchema,
)
```

**Apply this pattern to all 26 DataSource instances** (lines 397-883):

- DUALLOC, GENUNITS, RESERVE, DISPATCHREGIONSUM, DISPATCHLOAD, DISPATCHPRICE, DUDETAILSUMMARY, DUDETAIL, STATION, STATIONOPERATINGSTATUS, STATIONOWNER, STADUALLOC, BIDDAYOFFER_D, BIDPEROFFER_D, DISPATCHCONSTRAINT, GENCONDATA, SPDREGIONCONSTRAINT, SPDCONNECTIONPOINTCONSTRAINT, SPDINTERCONNECTORCONSTRAINT, INTERCONNECTOR, INTERCONNECTORCONSTRAINT, LOSSMODEL, LOSSFACTORMODEL, DISPATCHINTERCONNECTORRES, MNSP_INTERCONNECTOR

### Step 2: Run tests to verify

```bash
pytest test/test_nemweb_dbloader.py::test_nemweb_manager_datasources_have_schemas -v
```

Expected: PASS (tests that schema_class is set correctly)

### Step 3: Verify NEMWEBManager still instantiates

```bash
python3 -c "from nemdb import Config; from nemdb.nemweb.dbloader import NEMWEBManager; m = NEMWEBManager(Config()); print('✓ NEMWEBManager instantiated'); print(f'✓ DISPATCHREGIONSUM has {len(m.DISPATCHREGIONSUM.table_columns)} columns')"
```

Expected: Output shows instantiation success and column count

### Step 4: Commit

```bash
git add src/nemdb/nemweb/dbloader.py
git commit -m "refactor: remove table_columns arguments from 26 NEMWEBManager DataSources (now derived from schemas)"
```

---

## Task 7: Update test_archive_to_df Test

**Files:**

- Modify: `test/test_nemweb_dbloader.py:191-198` (test_archive_to_df)

### Step 1: Update test_archive_to_df signature and mocks

In `test/test_nemweb_dbloader.py`, replace lines 191-198:

```python
def test_archive_to_df(mocker, tmp_path):
    """Test _archive_to_df function with schema-derived dtypes."""
    mocker.patch("nemdb.nemweb.dbloader.read_header", return_value={"a", "b"})
    mocker.patch("pandas.read_csv", return_value=pd.DataFrame({"a": [1], "b": [2]}))
    # Mock dtypes dict instead of DTYPES
    dtypes = {"a": pl.Int64, "b": pl.Int64}
    df = _archive_to_df(str(tmp_path / "test.zip"), ["a", "b"], dtypes, 2024, 1)
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["a", "b"]
```

### Step 2: Run test

```bash
pytest test/test_nemweb_dbloader.py::test_archive_to_df -v
```

Expected: PASS

### Step 3: Commit

```bash
git add test/test_nemweb_dbloader.py
git commit -m "test: update test_archive_to_df to use dtypes parameter instead of DTYPES"
```

---

## Task 8: Update test_datasource_schema_class_attribute Test

**Files:**

- Modify: `test/test_nemweb_schemas.py:217-245` (test_datasource_schema_class_attribute)

### Step 1: Update test assertions

In `test/test_nemweb_schemas.py`, update lines 217-245 to remove the "is not None" checks (since schema_class is now required):

```python
def test_datasource_schema_class_attribute():
    """Verify all NEMWEBManager DataSources have schema_class attribute (now required)."""
    from nemdb import Config
    from nemdb.nemweb.dbloader import NEMWEBManager
    from pathlib import Path
    import tempfile

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
            if not hasattr(obj, "table_name") or not hasattr(obj, "table_columns"):
                continue

            # DataSources should have schema_class attribute (now required)
            assert hasattr(obj, "schema_class"), (
                f"{obj.table_name} DataSource missing schema_class attribute"
            )
            # Since schema_class is now required, don't need to check if not None
            assert isinstance(obj.schema_class, type), (
                f"{obj.table_name} schema_class should be a type"
            )
            checked_count += 1

        # Should have checked at least 25 DataSources (excludes ZONE_SUBSTATION which is DNSPDataSource)
        assert checked_count >= 25, (
            f"Only checked {checked_count} DataSources, expected at least 25"
        )
```

### Step 2: Run test

```bash
pytest test/test_nemweb_schemas.py::test_datasource_schema_class_attribute -v
```

Expected: PASS

### Step 3: Commit

```bash
git add test/test_nemweb_schemas.py
git commit -m "test: update test_datasource_schema_class_attribute since schema_class is now required"
```

---

## Task 9: Update test_schema_fields_match_table_columns Test

**Files:**

- Modify: `test/test_nemweb_schemas.py:183-214` (test_schema_fields_match_table_columns)

### Step 1: Rephrase test (no longer checking against separately-declared columns)

Since table_columns are now derived from schema, the original test is circular. Rephrase to verify the derivation works:

```python
def test_schema_fields_match_table_columns():
    """Verify table_columns are correctly derived from schema in DataSource."""
    from nemdb import Config
    from nemdb.nemweb.dbloader import NEMWEBManager
    from nemdb.nemweb.schemas import SCHEMA_MAP
    from pathlib import Path
    import tempfile

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
```

### Step 2: Run test

```bash
pytest test/test_nemweb_schemas.py::test_schema_fields_match_table_columns -v
```

Expected: PASS

### Step 3: Commit

```bash
git add test/test_nemweb_schemas.py
git commit -m "test: repurpose test_schema_fields_match_table_columns to verify schema-derived table_columns"
```

---

## Task 10: Run Full Test Suite and Verify

**Files:**

- Test: Run full nemweb test suite

### Step 1: Run all nemweb tests

```bash
cd /home/simba/workspace/nemdb
pytest test/test_nemweb_dbloader.py test/test_nemweb_schemas.py -v --tb=short
```

Expected: All tests PASS (should be ~32+ tests)

### Step 2: Run linting

```bash
ruff check src/nemdb/nemweb/
ruff format --check src/nemdb/nemweb/
mypy src/nemdb/nemweb/dbloader.py src/nemdb/nemweb/schemas.py
```

Expected: All pass (no formatting or type issues)

### Step 3: Verify DTYPES is completely removed

```bash
grep -r "DTYPES" /home/simba/workspace/nemdb/src/nemdb/nemweb/dbloader.py /home/simba/workspace/nemdb/src/nemdb/nemweb/schemas.py
```

Expected: No matches

### Step 4: Verify schema derivation works end-to-end

```bash
python3 << 'EOF'
from nemdb import Config
from nemdb.nemweb.dbloader import NEMWEBManager
from pathlib import Path
import tempfile

with tempfile.TemporaryDirectory() as tmp_dir:
    config = Config()
    config.CACHE_DIR = Path(tmp_dir)
    config.TEMP_DIR = Path(tmp_dir)
    manager = NEMWEBManager(config)

    # Spot check a few DataSources
    ds = manager.DISPATCHREGIONSUM
    print(f"✓ DISPATCHREGIONSUM columns derived: {len(ds.table_columns)} columns")
    print(f"  Sample: {ds.table_columns[:3]}")
    print(f"✓ DISPATCHREGIONSUM dtypes derived: {len(ds._dtypes)} types")
    print(f"  Sample: {list(ds._dtypes.items())[:2]}")

    ds2 = manager.DUALLOC
    print(f"✓ DUALLOC columns derived: {len(ds2.table_columns)} columns")
    print(f"✓ DUALLOC dtypes derived: {len(ds2._dtypes)} types")

    print("\n✓✓✓ All DataSources correctly derive columns and types from schemas")
EOF
```

Expected: Output shows successful derivation for multiple DataSources

### Step 5: Final commit

```bash
git add -A
git commit -m "refactor: complete schema single-source-of-truth implementation - remove table_columns duplication and DTYPES"
```

---

## Summary

**What was achieved:**

1. ✅ Added `_schema_to_dtypes()` helper to extract Polars types from Pandera schema annotations
2. ✅ Modified `DataSource.__init__` to derive `table_columns` and `_dtypes` from `schema_class`
3. ✅ Updated `_archive_to_df` and `_archive_to_df_low_memory` to accept `dtypes` parameter
4. ✅ Removed 200+ line DTYPES dictionary entirely
5. ✅ Simplified all 26 NEMWEBManager DataSource instantiations (removed `table_columns=` arguments)
6. ✅ Updated tests to reflect new architecture
7. ✅ Verified full test suite passes

**Single source of truth achieved:**

- Column names: derived from `schema_class.__fields__.keys()`
- Column types: derived from schema annotations via `_schema_to_dtypes()`
- No duplication: DTYPES removed, table_columns parameter removed

**Backward compatibility:**

- `table_columns` still exists as an instance attribute (derived, not passed)
- Existing code using `ds.table_columns` continues to work
- `_dtypes` is an internal attribute (prefixed with `_`)
- Public API unchanged from user perspective

---

## Execution

Plan complete and saved to `docs/plans/2026-02-22-schema-single-source-of-truth.md`.

**Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
