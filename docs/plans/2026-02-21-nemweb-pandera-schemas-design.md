# Design: NEMWEB Pandera Schemas

**Date**: 2026-02-21
**Objective**: Create Pandera schemas for all NEMWEB database tables for documentation and type contracts.

## Overview

Add machine-readable type contracts for the 25+ NEMWEB tables currently managed by `NEMWEBManager`. These schemas serve as **documentation and type specifications** for the data returned by each table's `get_data()` method.

## Design Decisions

### Purpose

- **Primary**: Documentation and type contracts for IDE support and code clarity
- **Not for**: Runtime validation (to avoid performance overhead on large datasets)

### File Location

- **Path**: `src/nemdb/nemweb/schemas.py`
- **Rationale**: Co-located with `dbloader.py` in the nemweb module for easy discoverability

### Schema Style

- **API**: `pandera.polars.DataFrameModel` (class-based)
- **Rationale**: Modern pandera API, native Polars support, readable as documentation, IDE-friendly

### Naming Convention

- Schema class names: `{CamelCaseTableName}Schema`
  - Example: `DISPATCHREGIONSUM` → `DispatchRegionSumSchema`
  - Example: `BIDDAYOFFER_D` → `BidDayOfferDSchema`

### Field Types

- **Source of truth**: Existing `DTYPES` dictionary in `dbloader.py`
- **Nullable columns**: Fields that can have null values (due to missing columns in source data) are marked `Optional[PolarsType]`
- **Mapping examples**:
  - `int` → `pl.Int32`
  - `float` → `pl.Float32`
  - `str` → `pl.String` or `pl.Categorical` (depending on DTYPES)
  - `datetime` → `pl.Datetime`

### Coverage

All 25+ tables in NEMWEBManager:

1. DUALLOC
2. GENUNITS
3. RESERVE
4. DISPATCHREGIONSUM
5. DISPATCHLOAD
6. DISPATCHPRICE
7. DUDETAILSUMMARY
8. DUDETAIL
9. STATION
10. STATIONOPERATINGSTATUS
11. STATIONOWNER
12. STADUALLOC
13. BIDDAYOFFER_D
14. BIDPEROFFER_D
15. DISPATCHCONSTRAINT
16. GENCONDATA
17. SPDREGIONCONSTRAINT
18. SPDCONNECTIONPOINTCONSTRAINT
19. SPDINTERCONNECTORCONSTRAINT
20. INTERCONNECTOR
21. INTERCONNECTORCONSTRAINT
22. LOSSMODEL
23. LOSSFACTORMODEL
24. DISPATCHINTERCONNECTORRES
25. MNSP_INTERCONNECTOR

(Plus ZONE_SUBSTATION if columns can be mapped from DNSP data sources.)

### What's Excluded

- **archive_month**: Partition column added during storage, not part of the logical table schema
- **ZONE_SUBSTATION**: DNSP-specific columns (time, zss, MW, network) not in DTYPES; may be covered in a separate DNSP schemas module if needed

### Example Schema

```python
import pandera.polars as pa
import polars as pl
from typing import Optional

class DispatchRegionSumSchema(pa.DataFrameModel):
    SETTLEMENTDATE: pl.Datetime
    REGIONID: pl.Categorical
    TOTALDEMAND: Optional[pl.Float32]
    DEMANDFORECAST: Optional[pl.Float32]
    DISPATCHABLELOAD: Optional[pl.Float32]
    INITIALSUPPLY: Optional[pl.Float32]
    SS_SOLAR_AVAILABILITY: Optional[pl.Float32]
    SS_WIND_AVAILABILITY: Optional[pl.Float32]
    AVAILABLEGENERATION: Optional[pl.Float32]
    AVAILABLELOAD: Optional[pl.Float32]
    # ... more fields
```

### Exports

- All schemas exported from `nemdb.nemweb.schemas` module
- Optional: Re-export from `nemdb.nemweb.__init__` for convenience
- Not auto-imported at package level to avoid namespace pollution

## Implementation Notes

1. **Column ordering**: Preserve the order from `table_columns` for each DataSource
2. **Docstrings**: Add a brief docstring to each schema class describing the table's purpose
3. **Config**: No runtime validation needed; schemas exist purely as type contracts
4. **Future extensibility**: Schemas can later be used with Pandera's `@check_types` decorator if runtime validation becomes desired

## Success Criteria

- ✓ All 25+ tables have corresponding `DataFrameModel` schemas
- ✓ All column types match the existing `DTYPES` dictionary
- ✓ Schemas are readable and serve as type documentation
- ✓ Module is properly exported and documented
- ✓ Pre-commit hooks pass (ruff lint, mypy if applicable)
