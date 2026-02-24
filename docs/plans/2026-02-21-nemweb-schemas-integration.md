# DataSource-Schema Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Connect Pandera schemas to DataSources for IDE type inference, schema validation, and consistency verification.

**Architecture:** Add optional `schema_class` parameter to DataSource, create SCHEMA_MAP registry mapping DataSources to schemas, add non-invasive tests for schema-column consistency, and provide opt-in validation utilities. All changes are backward-compatible (existing code continues to work unchanged).

**Tech Stack:** Pandera 0.26.1+, Polars, pytest, Python 3.10+ type hints

---

## Task 1: Modify DataSource to Accept Schema Class

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:1093-1115` (DataSource.**init**)
- Test: `test/test_nemweb_dbloader.py` (add test)

### Step 1: Write the failing test

```python
def test_data_source_with_schema_class(mock_config):
    """Test DataSource accepts and stores schema_class parameter."""
    from nemdb.nemweb.schemas import DispatchRegionSumSchema

    ds = DataSource(
        mock_config,
        "TABLE",
        ["a", "b"],
        ["a"],
        schema_class=DispatchRegionSumSchema
    )
    assert ds.schema_class == DispatchRegionSumSchema
```

### Step 2: Run test to verify it fails

```bash
cd /home/simba/workspace/nemdb
pytest test/test_nemweb_dbloader.py::test_data_source_with_schema_class -v
```

Expected: FAIL with "TypeError: **init**() got an unexpected keyword argument 'schema_class'"

### Step 3: Write minimal implementation

Modify `src/nemdb/nemweb/dbloader.py` DataSource.**init**:

```python
def __init__(
    self,
    config: type[Config],
    table_name: str,
    table_columns: list[str],
    table_primary_keys: list[str] | None = None,
    add_partitions: list[str] | None = None,
    low_memory: bool = False,
    schema_class: type | None = None,  # NEW
):
    """Creates a parquet dataset."""
    self.config = config
    self.table_name = table_name
    self.table_columns = table_columns
    self.table_primary_keys = table_primary_keys if table_primary_keys is not None else []
    self.partitions = (
        [*add_partitions, "archive_month"] if add_partitions is not None else ["archive_month"]
    )
    self.low_memory = low_memory
    self.schema_class = schema_class  # NEW

    self.path = f"{config.CACHE_DIR}/{table_name}/"
    self.fs = fsspec.filesystem(config.FILESYSTEM)
    self.fs.makedirs(f"{config.CACHE_DIR}/{table_name}", exist_ok=True)
```

### Step 4: Run test to verify it passes

```bash
pytest test/test_nemweb_dbloader.py::test_data_source_with_schema_class -v
```

Expected: PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/dbloader.py test/test_nemweb_dbloader.py
git commit -m "feat: add schema_class parameter to DataSource"
```

---

## Task 2: Wire Schema Classes to NEMWEBManager DataSources

**Files:**

- Modify: `src/nemdb/nemweb/dbloader.py:370-831` (NEMWEBManager.**init**)
- Test: `test/test_nemweb_dbloader.py` (add test)

### Step 1: Write the failing test

```python
def test_nemweb_manager_datasources_have_schemas(mock_config):
    """Test that NEMWEBManager DataSources are wired to their schemas."""
    from nemdb.nemweb.schemas import (
        DispatchRegionSumSchema,
        DispatchLoadSchema,
        DUALLOCSchema,
    )

    manager = NEMWEBManager(mock_config)

    assert manager.DISPATCHREGIONSUM.schema_class == DispatchRegionSumSchema
    assert manager.DISPATCHLOAD.schema_class == DispatchLoadSchema
    assert manager.DUALLOC.schema_class == DUALLOCSchema
```

### Step 2: Run test to verify it fails

```bash
pytest test/test_nemweb_dbloader.py::test_nemweb_manager_datasources_have_schemas -v
```

Expected: FAIL (assertion errors - schema_class is None)

### Step 3: Write minimal implementation

Add import at top of dbloader.py:

```python
# Near other imports at top (line ~24)
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
    ZONESUBSTATIONSchema,
)
```

Modify NEMWEBManager.**init** (line ~370-831) to add `schema_class=` parameter to each DataSource. Example pattern:

```python
# Around line 370-375
self.DUALLOC = DataSource(
    config=config,
    table_name="DUALLOC",
    table_columns=["DUID", "GENSETID", "LASTCHANGED", "VERSIONNO"],
    schema_class=DUALLOCSchema,  # NEW
)

# Around line 375-397
self.GENUNITS = DataSource(
    config=config,
    table_name="GENUNITS",
    table_columns=[...],
    table_primary_keys=["STATIONID", "LASTCHANGED"],
    schema_class=GENUNITSSchema,  # NEW
)

# Continue for all 26 DataSources...
# RESERVE (line ~398-412): schema_class=RESERVESchema
# DISPATCHREGIONSUM (line ~413-429): schema_class=DispatchRegionSumSchema
# DISPATCHLOAD (line ~430-469): schema_class=DispatchLoadSchema
# DISPATCHPRICE (line ~470-490): schema_class=DispatchPriceSchema
# DUDETAILSUMMARY (line ~491-522): schema_class=DUDETAILSUMMARYSchema
# DUDETAIL (line ~523-558): schema_class=DUDETAILSchema
# STATION (line ~559-574): schema_class=STATIONSchema
# STATIONOPERATINGSTATUS (line ~575-585): schema_class=STATIONOPERATINGSTATUSSchema
# STATIONOWNER (line ~586-596): schema_class=STATIONOWNERSchema
# STADUALLOC (line ~597-607): schema_class=STADUALLOCSchema
# BIDDAYOFFER_D (line ~608-638): schema_class=BidDayOfferDSchema
# BIDPEROFFER_D (line ~639-672): schema_class=BidPerOfferDSchema
# DISPATCHCONSTRAINT (line ~673-688): schema_class=DispatchConstraintSchema
# GENCONDATA (line ~689-700): schema_class=GENCONDATASchema
# SPDREGIONCONSTRAINT (line ~701-719): schema_class=SPDREGIONCONSTRAINTSchema
# SPDCONNECTIONPOINTCONSTRAINT (line ~720-738): schema_class=SPDCONNECTIONPOINTCONSTRAINTSchema
# SPDINTERCONNECTORCONSTRAINT (line ~739-755): schema_class=SPDINTERCONNECTORCONSTRAINTSchema
# INTERCONNECTOR (line ~756-761): schema_class=INTERCONNECTORSchema
# INTERCONNECTORCONSTRAINT (line ~762-779): schema_class=INTERCONNECTORCONSTRAINTSchema
# LOSSMODEL (line ~780-791): schema_class=LOSSMODELSchema
# LOSSFACTORMODEL (line ~792-803): schema_class=LOSSFACTORMODELSchema
# DISPATCHINTERCONNECTORRES (line ~804-809): schema_class=DispatchInterconnectorResSchema
# MNSP_INTERCONNECTOR (line ~810-831): schema_class=MNSP_INTERCONNECTORSchema
```

### Step 4: Run test to verify it passes

```bash
pytest test/test_nemweb_dbloader.py::test_nemweb_manager_datasources_have_schemas -v
```

Expected: PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/dbloader.py test/test_nemweb_dbloader.py
git commit -m "feat: wire schema classes to all 26 NEMWEBManager DataSources"
```

---

## Task 3: Create SCHEMA_MAP Registry

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (add to end)
- Test: `test/test_nemweb_schemas.py` (add test)

### Step 1: Write the failing test

```python
def test_schema_map_exists_and_is_complete():
    """Test that SCHEMA_MAP registry exists and contains all 26 schemas."""
    from nemdb.nemweb.schemas import SCHEMA_MAP

    # Should have 26 entries
    assert len(SCHEMA_MAP) == 26

    # All values should be schema classes
    import pandera.polars as pa
    for table_name, schema_class in SCHEMA_MAP.items():
        assert isinstance(table_name, str)
        assert issubclass(schema_class, pa.DataFrameModel)
```

### Step 2: Run test to verify it fails

```bash
pytest test/test_nemweb_schemas.py::test_schema_map_exists_and_is_complete -v
```

Expected: FAIL with "ImportError: cannot import name 'SCHEMA_MAP'"

### Step 3: Write minimal implementation

Add to end of `src/nemdb/nemweb/schemas.py` (after ZONESUBSTATIONSchema definition):

```python
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
```

### Step 4: Run test to verify it passes

```bash
pytest test/test_nemweb_schemas.py::test_schema_map_exists_and_is_complete -v
```

Expected: PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
git commit -m "feat: add SCHEMA_MAP registry for table-schema discovery"
```

---

## Task 4: Add Schema-Consistency Tests

**Files:**

- Modify: `test/test_nemweb_schemas.py` (add tests)

### Step 1: Write the failing tests

Add these test functions to `test/test_nemweb_schemas.py`:

```python
def test_schema_fields_match_table_columns():
    """Verify that schema fields match DataSource table_columns for each table."""
    from nemdb import Config
    from nemdb.nemweb.dbloader import NEMWEBManager
    from nemdb.nemweb.schemas import SCHEMA_MAP
    from pathlib import Path
    import tempfile

    # Create temp config
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config()
        config.CACHE_DIR = Path(tmp_dir)
        config.TEMP_DIR = Path(tmp_dir)
        manager = NEMWEBManager(config)

        # For each DataSource with a schema, verify columns match schema fields
        for ds_attr in dir(manager):
            if ds_attr.startswith("_"):
                continue
            obj = getattr(manager, ds_attr)
            if not hasattr(obj, "table_name") or not hasattr(obj, "table_columns"):
                continue

            table_name = obj.table_name
            if table_name not in SCHEMA_MAP:
                continue

            # Get schema fields (excluding inherited pydantic fields)
            schema_class = SCHEMA_MAP[table_name]
            schema_fields = set(schema_class.__pydantic_model__.model_fields.keys())
            table_columns = set(obj.table_columns)

            assert schema_fields == table_columns, (
                f"{table_name}: Schema fields {schema_fields} do not match "
                f"table columns {table_columns}. Mismatch: "
                f"Missing from schema: {table_columns - schema_fields}, "
                f"Extra in schema: {schema_fields - table_columns}"
            )


def test_datasource_schema_class_attribute():
    """Verify all NEMWEBManager DataSources have schema_class attribute."""
    from nemdb import Config
    from nemdb.nemweb.dbloader import NEMWEBManager
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config()
        config.CACHE_DIR = Path(tmp_dir)
        config.TEMP_DIR = Path(tmp_dir)
        manager = NEMWEBManager(config)

        # Check all DataSources in manager (except ZONE_SUBSTATION which is DNSP)
        checked_count = 0
        for ds_attr in dir(manager):
            if ds_attr.startswith("_"):
                continue
            obj = getattr(manager, ds_attr)
            if not hasattr(obj, "table_name") or not hasattr(obj, "table_columns"):
                continue

            # DataSources should have schema_class attribute
            assert hasattr(obj, "schema_class"), (
                f"{obj.table_name} DataSource missing schema_class attribute"
            )
            assert obj.schema_class is not None, (
                f"{obj.table_name} schema_class is None"
            )
            checked_count += 1

        # Should have checked at least 26 DataSources
        assert checked_count >= 26
```

### Step 2: Run tests to verify they fail

```bash
pytest test/test_nemweb_schemas.py::test_schema_fields_match_table_columns -v
pytest test/test_nemweb_schemas.py::test_datasource_schema_class_attribute -v
```

Expected: Both FAIL (schema_class is None or not set)

### Step 3: Run tests to verify they pass

(Tests should pass after Tasks 1-2 are complete)

```bash
pytest test/test_nemweb_schemas.py::test_schema_fields_match_table_columns -v
pytest test/test_nemweb_schemas.py::test_datasource_schema_class_attribute -v
```

Expected: Both PASS

### Step 4: Commit

```bash
git add test/test_nemweb_schemas.py
git commit -m "test: add schema-consistency validation tests"
```

---

## Task 5: Create Optional Validation Utility

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (add utility function)
- Test: `test/test_nemweb_schemas.py` (add tests)

### Step 1: Write the failing test

Add to `test/test_nemweb_schemas.py`:

```python
def test_validate_against_schema_valid_data():
    """Test validate_against_schema with valid data."""
    from nemdb.nemweb.schemas import validate_against_schema, DispatchRegionSumSchema
    from datetime import datetime
    import polars as pl

    # Create valid DataFrame matching schema
    df = pl.DataFrame({
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
    }).cast({
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
    })

    result = validate_against_schema(df, DispatchRegionSumSchema)
    assert result is True


def test_validate_against_schema_invalid_data():
    """Test validate_against_schema with invalid data (missing required field)."""
    from nemdb.nemweb.schemas import validate_against_schema, DispatchRegionSumSchema
    from datetime import datetime
    import polars as pl

    # Create invalid DataFrame (missing REGIONID)
    df = pl.DataFrame({
        "SETTLEMENTDATE": [datetime(2024, 1, 1, 12, 0)],
        # Missing REGIONID - required field
        "TOTALDEMAND": [1000.0],
    }).cast({
        "SETTLEMENTDATE": pl.Datetime,
        "TOTALDEMAND": pl.Float32,
    })

    result = validate_against_schema(df, DispatchRegionSumSchema, raise_on_error=False)
    assert result is False


def test_validate_against_schema_raises_on_error():
    """Test validate_against_schema raises exception when requested."""
    from nemdb.nemweb.schemas import validate_against_schema, DispatchRegionSumSchema
    import polars as pl
    import pytest

    df = pl.DataFrame({
        "SETTLEMENTDATE": [],
        "TOTALDEMAND": [],
    }).cast({
        "SETTLEMENTDATE": pl.Datetime,
        "TOTALDEMAND": pl.Float32,
    })

    with pytest.raises(Exception):  # Pandera SchemaError
        validate_against_schema(df, DispatchRegionSumSchema, raise_on_error=True)
```

### Step 2: Run tests to verify they fail

```bash
pytest test/test_nemweb_schemas.py::test_validate_against_schema_valid_data -v
pytest test/test_nemweb_schemas.py::test_validate_against_schema_invalid_data -v
pytest test/test_nemweb_schemas.py::test_validate_against_schema_raises_on_error -v
```

Expected: All FAIL with "ImportError: cannot import name 'validate_against_schema'"

### Step 3: Write minimal implementation

Add to `src/nemdb/nemweb/schemas.py` (after SCHEMA_MAP definition):

```python
# Validation Utilities
# ====================


def validate_against_schema(
    df: pl.DataFrame,
    schema_class: type[pa.DataFrameModel],
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
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Schema validation failed for {schema_class.__name__}: {e}")
        return False
```

### Step 4: Run tests to verify they pass

```bash
pytest test/test_nemweb_schemas.py::test_validate_against_schema_valid_data -v
pytest test/test_nemweb_schemas.py::test_validate_against_schema_invalid_data -v
pytest test/test_nemweb_schemas.py::test_validate_against_schema_raises_on_error -v
```

Expected: All PASS

### Step 5: Commit

```bash
git add src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
git commit -m "feat: add optional validate_against_schema() utility"
```

---

## Task 6: Add Documentation and Verification

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (update module docstring)
- Modify: `src/nemdb/nemweb/dbloader.py` (add docstring notes)
- Test: Run full integration test

### Step 1: Update module docstring in schemas.py

Replace the module docstring (lines 1-8) in `src/nemdb/nemweb/schemas.py` with:

```python
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
```

### Step 2: Add DataSource docstring note

Add to DataSource.**init** docstring (around line 1093-1101):

```python
def __init__(
    self,
    config: type[Config],
    table_name: str,
    table_columns: list[str],
    table_primary_keys: list[str] | None = None,
    add_partitions: list[str] | None = None,
    low_memory: bool = False,
    schema_class: type | None = None,
):
    """Creates a parquet dataset.

    Args:
        schema_class: Optional Pandera schema class for this DataSource. When provided,
                     enables IDE type inference and schema validation via
                     validate_against_schema(). See nemdb.nemweb.schemas for available schemas.
    """
```

### Step 3: Run full test suite

```bash
pytest test/test_nemweb_dbloader.py -v
pytest test/test_nemweb_schemas.py -v
```

Expected: All tests PASS (should be ~35+ tests total)

### Step 4: Run linting

```bash
cd /home/simba/workspace/nemdb
ruff check src/nemdb/nemweb/
ruff format src/nemdb/nemweb/ --check
mypy src/nemdb/nemweb/
```

Expected: All pass (no errors)

### Step 5: Commit

```bash
git add src/nemdb/nemweb/schemas.py src/nemdb/nemweb/dbloader.py
git commit -m "docs: add schema integration documentation and verification"
```

---

## Task 7: Verify Integration and Full Test Suite

**Files:**

- Test: Run all tests

### Step 1: Run full test suite for nemweb module

```bash
cd /home/simba/workspace/nemdb
pytest test/test_nemweb_dbloader.py test/test_nemweb_schemas.py -v --tb=short
```

Expected: All tests PASS

### Step 2: Run full project test suite

```bash
pytest test/ -v --tb=short
```

Expected: All tests PASS (should not break any existing tests)

### Step 3: Check coverage

```bash
pytest test/test_nemweb_schemas.py test/test_nemweb_dbloader.py --cov=src/nemdb/nemweb --cov-report=term-missing
```

Expected: schemas.py should have 100% coverage for new code (or very close)

### Step 4: Verify integration works end-to-end

Create quick integration verification script:

```python
# Temporary verification script
from nemdb import Config
from nemdb.nemweb.dbloader import NEMWEBManager
from nemdb.nemweb.schemas import SCHEMA_MAP, validate_against_schema
from pathlib import Path
import tempfile

with tempfile.TemporaryDirectory() as tmp_dir:
    config = Config()
    config.CACHE_DIR = Path(tmp_dir)
    config.TEMP_DIR = Path(tmp_dir)
    manager = NEMWEBManager(config)

    # Verify each DataSource has correct schema class
    checked = 0
    for table_name, schema_class in SCHEMA_MAP.items():
        # Find matching DataSource
        for attr in dir(manager):
            if attr.startswith("_"):
                continue
            ds = getattr(manager, attr)
            if hasattr(ds, "table_name") and ds.table_name == table_name:
                assert ds.schema_class == schema_class, (
                    f"{table_name}: schema_class mismatch"
                )
                checked += 1
                break

    assert checked == len(SCHEMA_MAP), (
        f"Only verified {checked}/{len(SCHEMA_MAP)} DataSources"
    )

    print(f"✓ Integration verified: {checked} DataSources correctly wired to schemas")
```

Run it:

```bash
cd /home/simba/workspace/nemdb
python -c "
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

    checked = 0
    for table_name, schema_class in SCHEMA_MAP.items():
        for attr in dir(manager):
            if attr.startswith('_'):
                continue
            ds = getattr(manager, attr)
            if hasattr(ds, 'table_name') and ds.table_name == table_name:
                assert ds.schema_class == schema_class
                checked += 1
                break

    print(f'✓ Integration verified: {checked} DataSources correctly wired')
"
```

Expected: "✓ Integration verified: 26 DataSources correctly wired"

### Step 5: Commit

```bash
git add -A
git commit -m "test: verify full DataSource-Schema integration"
```

---

## Summary

**What this achieves:**

1. ✅ DataSource instances now store their schema class (non-breaking change)
2. ✅ All 26 NEMWEBManager DataSources wired to corresponding schemas
3. ✅ SCHEMA_MAP registry enables discovery and programmatic access
4. ✅ Schema-consistency tests prevent divergence between schemas and tables
5. ✅ Optional validation utility available for tests/CI/CD scenarios
6. ✅ IDE can now infer schema types from DataSource instances
7. ✅ Full backward compatibility (all existing code continues to work)

**Testing:**

- 4 new validation tests added
- All existing tests continue to pass
- No runtime performance impact (validation is opt-in)

**Next Steps:**

- Use `from nemdb.nemweb.schemas import SCHEMA_MAP` to discover schemas
- Use `manager.DISPATCHREGIONSUM.schema_class` for IDE type hints
- Use `validate_against_schema()` in tests for runtime validation
- Add type hints to custom code using the schemas

---

## Execution

Plan complete and saved to `docs/plans/2026-02-21-nemweb-schemas-integration.md`.

**Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
