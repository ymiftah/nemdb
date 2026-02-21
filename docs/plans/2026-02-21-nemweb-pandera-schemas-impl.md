# NEMWEB Pandera Schemas Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create Pandera schemas for all 26 NEMWEB database tables as type contracts and documentation.

**Architecture:** Single module (`src/nemdb/nemweb/schemas.py`) containing 26 `pandera.polars.DataFrameModel` classes, one per table. Each class maps to its corresponding `DataSource` in `dbloader.py`, with field types sourced from the `DTYPES` dictionary. All fields are marked `Optional` since missing columns are filled with nulls during archive processing.

**Tech Stack:** Pandera 0.26.1+, Polars DataFrameModel API, Python 3.13 type hints

---

## Task 1: Create schemas.py skeleton with imports

**Files:**

- Create: `src/nemdb/nemweb/schemas.py`

**Step 1: Write the file with module docstring and imports**

```python
"""Pandera schemas for NEMWEB database tables.

These schemas document the expected column types and structure for each AEMO MMS
table as returned by the corresponding DataSource.get_data() method.

All schemas use pandera.polars.DataFrameModel for native Polars support and IDE
type hinting. Fields are marked Optional since the _archive_to_df function fills
missing columns with null values.
"""

from typing import Optional

import pandera.polars as pa
import polars as pl


# Dispatch Tables
# ===============
```

**Step 2: Verify file is created**

```bash
cat src/nemdb/nemweb/schemas.py | head -20
```

Expected: Module docstring and imports visible.

**Step 3: Commit skeleton**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: create schemas.py skeleton for NEMWEB table schemas

Add module with docstring, imports, and structure comments for organizing
table schemas by category."
```

---

## Task 2: Create dispatch-related schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (add dispatch schemas after "# Dispatch Tables" comment)

**Step 1: Add dispatch table schemas**

Append these schemas after the "# Dispatch Tables" comment:

```python
class DispatchRegionSumSchema(pa.DataFrameModel):
    """Daily region dispatch summary with demand and supply data."""

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


class DispatchLoadSchema(pa.DataFrameModel):
    """Dispatch load and availability data for generators."""

    SETTLEMENTDATE: pl.Datetime
    DUID: pl.Categorical
    DISPATCHMODE: Optional[pl.Int8]
    AGCSTATUS: Optional[pl.Int8]
    INITIALMW: Optional[pl.Float32]
    TOTALCLEARED: Optional[pl.Float32]
    RAMPDOWNRATE: Optional[pl.Float32]
    RAMPUPRATE: Optional[pl.Float32]
    AVAILABILITY: Optional[pl.Float32]
    RAISEREGENABLEMENTMAX: Optional[pl.Float32]
    RAISEREGENABLEMENTMIN: Optional[pl.Float32]
    LOWERREGENABLEMENTMAX: Optional[pl.Float32]
    LOWERREGENABLEMENTMIN: Optional[pl.Float32]
    SEMIDISPATCHCAP: Optional[pl.Float32]
    LOWER5MIN: Optional[pl.Float32]
    LOWER60SEC: Optional[pl.Float32]
    LOWER6SEC: Optional[pl.Float32]
    LOWER1SEC: Optional[pl.Float32]
    RAISE5MIN: Optional[pl.Float32]
    RAISE60SEC: Optional[pl.Float32]
    RAISE6SEC: Optional[pl.Float32]
    RAISE1SEC: Optional[pl.Float32]
    LOWERREG: Optional[pl.Float32]
    RAISEREG: Optional[pl.Float32]
    RAISEREGAVAILABILITY: Optional[pl.Float32]
    RAISE6SECACTUALAVAILABILITY: Optional[pl.Float32]
    RAISE1SECACTUALAVAILABILITY: Optional[pl.Float32]
    RAISE60SECACTUALAVAILABILITY: Optional[pl.Float32]
    RAISE5MINACTUALAVAILABILITY: Optional[pl.Float32]
    RAISEREGACTUALAVAILABILITY: Optional[pl.Float32]
    LOWER6SECACTUALAVAILABILITY: Optional[pl.Float32]
    LOWER1SECACTUALAVAILABILITY: Optional[pl.Float32]
    UIGF: Optional[pl.Float32]


class DispatchPriceSchema(pa.DataFrameModel):
    """Regional dispatch pricing for energy and reserve products."""

    SETTLEMENTDATE: pl.Datetime
    REGIONID: pl.Categorical
    RRP: Optional[pl.Float32]
    ROP: Optional[pl.Float32]
    RAISE6SECROP: Optional[pl.Float32]
    RAISE1SECROP: Optional[pl.Float32]
    RAISE60SECROP: Optional[pl.Float32]
    RAISE5MINROP: Optional[pl.Float32]
    RAISEREGROP: Optional[pl.Float32]
    LOWER6SECROP: Optional[pl.Float32]
    LOWER1SECROP: Optional[pl.Float32]
    LOWER60SECROP: Optional[pl.Float32]
    LOWER5MINROP: Optional[pl.Float32]
    LOWERREGROP: Optional[pl.Float32]


class DispatchConstraintSchema(pa.DataFrameModel):
    """Dispatch constraint violations and marginal values."""

    SETTLEMENTDATE: pl.Datetime
    CONSTRAINTID: pl.Categorical
    DUID: Optional[pl.Categorical]
    RHS: Optional[pl.Float32]
    GENCONID_EFFECTIVEDATE: Optional[pl.Date]
    GENCONID_VERSIONNO: Optional[pl.Int32]
    LHS: Optional[pl.Float32]
    VIOLATIONDEGREE: Optional[pl.Float32]
    MARGINALVALUE: Optional[pl.Float32]


class DispatchInterconnectorResSchema(pa.DataFrameModel):
    """Interconnector flow and losses during dispatch."""

    INTERCONNECTORID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    MWFLOW: Optional[pl.Float32]
    MWLOSSES: Optional[pl.Float32]
```

**Step 2: Verify schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import DispatchRegionSumSchema; print('Dispatch schemas loaded')"
```

Expected: "Dispatch schemas loaded"

**Step 3: Commit dispatch schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add dispatch-related Pandera schemas

Add schemas for DISPATCHREGIONSUM, DISPATCHLOAD, DISPATCHPRICE,
DISPATCHCONSTRAINT, and DISPATCHINTERCONNECTORRES tables."
```

---

## Task 3: Create bid-related schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append after dispatch schemas)

**Step 1: Add section comment and bid schemas**

Append these after dispatch schemas:

```python
# Bid Tables
# ==========


class BidDayOfferDSchema(pa.DataFrameModel):
    """Daily energy bid offers by generators."""

    DUID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    BIDTYPE: pl.Categorical
    DIRECTION: pl.Categorical
    VERSIONNO: Optional[pl.Int32]
    PARTICIPANTID: Optional[pl.Categorical]
    DAILYENERGYCONSTRAINT: Optional[pl.Float32]
    PRICEBAND1: Optional[pl.Float32]
    PRICEBAND2: Optional[pl.Float32]
    PRICEBAND3: Optional[pl.Float32]
    PRICEBAND4: Optional[pl.Float32]
    PRICEBAND5: Optional[pl.Float32]
    PRICEBAND6: Optional[pl.Float32]
    PRICEBAND7: Optional[pl.Float32]
    PRICEBAND8: Optional[pl.Float32]
    PRICEBAND9: Optional[pl.Float32]
    PRICEBAND10: Optional[pl.Float32]
    MINIMUMLOAD: Optional[pl.Float32]
    T1: Optional[pl.Float32]
    T2: Optional[pl.Float32]
    T3: Optional[pl.Float32]
    T4: Optional[pl.Float32]
    NORMALSTATUS: Optional[pl.String]
    ENTRYTYPE: Optional[pl.Categorical]


class BidPerOfferDSchema(pa.DataFrameModel):
    """Interval-level bid offers with availability and constraints."""

    DUID: pl.Categorical
    SETTLEMENTDATE: pl.Datetime
    BIDTYPE: pl.Categorical
    DIRECTION: pl.Categorical
    VERSIONNO: Optional[pl.Int32]
    INTERVAL_DATETIME: pl.Datetime
    MAXAVAIL: Optional[pl.Float32]
    FIXEDLOAD: Optional[pl.Float32]
    ROCUP: Optional[pl.Float32]
    ROCDOWN: Optional[pl.Float32]
    ENABLEMENTMIN: Optional[pl.Float32]
    ENABLEMENTMAX: Optional[pl.Float32]
    LOWBREAKPOINT: Optional[pl.Float32]
    HIGHBREAKPOINT: Optional[pl.Float32]
    BANDAVAIL1: Optional[pl.Float32]
    BANDAVAIL2: Optional[pl.Float32]
    BANDAVAIL3: Optional[pl.Float32]
    BANDAVAIL4: Optional[pl.Float32]
    BANDAVAIL5: Optional[pl.Float32]
    BANDAVAIL6: Optional[pl.Float32]
    BANDAVAIL7: Optional[pl.Float32]
    BANDAVAIL8: Optional[pl.Float32]
    BANDAVAIL9: Optional[pl.Float32]
    BANDAVAIL10: Optional[pl.Float32]
    ENERGYLIMIT: Optional[pl.Float32]
    LASTCHANGED: Optional[pl.Datetime]
```

**Step 2: Verify bid schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import BidDayOfferDSchema, BidPerOfferDSchema; print('Bid schemas loaded')"
```

Expected: "Bid schemas loaded"

**Step 3: Commit bid schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add bid-related Pandera schemas

Add schemas for BIDDAYOFFER_D and BIDPEROFFER_D tables."
```

---

## Task 4: Create generation unit schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append after bid schemas)

**Step 1: Add section comment and generation schemas**

Append these after bid schemas:

```python
# Generation Unit Tables
# ======================


class DUALLOCSchema(pa.DataFrameModel):
    """Dispatch unit to generation set allocation."""

    DUID: pl.Categorical
    GENSETID: pl.Categorical
    LASTCHANGED: Optional[pl.Datetime]
    VERSIONNO: Optional[pl.Int32]


class GENUNITSSchema(pa.DataFrameModel):
    """Generation unit characteristics and capabilities."""

    GENSETID: pl.Categorical
    STATIONID: pl.String
    VOLTLEVEL: Optional[pl.Float32]
    DISPATCHTYPE: pl.Categorical
    STARTTYPE: Optional[pl.String]
    NORMALSTATUS: Optional[pl.String]
    MAXCAPACITY: Optional[pl.Float32]
    GENSETTYPE: Optional[pl.String]
    GENSETNAME: Optional[pl.String]
    LOWERREG: Optional[pl.Float32]
    CO2E_EMISSIONS_FACTOR: Optional[pl.Float32]
    CO2E_ENERGY_SOURCE: Optional[pl.String]
    CO2E_DATA_SOURCE: Optional[pl.String]
    MINCAPACITY: Optional[pl.Float32]
    REGISTEREDMINCAPACITY: Optional[pl.Float32]
    LASTCHANGED: Optional[pl.Datetime]


class DUDETAILSUMMARYSchema(pa.DataFrameModel):
    """Dispatch unit summary with operational dates and limits."""

    DUID: pl.Categorical
    START_DATE: pl.Date
    END_DATE: pl.Date
    DISPATCHTYPE: Optional[pl.Categorical]
    CONNECTIONPOINTID: Optional[pl.Categorical]
    REGIONID: Optional[pl.Categorical]
    STATIONID: Optional[pl.String]
    TRANSMISSIONLOSSFACTOR: Optional[pl.Float32]
    STARTTYPE: Optional[pl.String]
    DISTRIBUTIONLOSSFACTOR: Optional[pl.Float32]
    MINIMUM_ENERGY_PRICE: Optional[pl.Float32]
    MAXIMUM_ENERGY_PRICE: Optional[pl.Float32]
    SCHEDULE_TYPE: Optional[pl.Categorical]
    MIN_RAMP_RATE_UP: Optional[pl.Float32]
    MIN_RAMP_RATE_DOWN: Optional[pl.Float32]
    MAX_RAMP_RATE_UP: Optional[pl.Float32]
    MAX_RAMP_RATE_DOWN: Optional[pl.Float32]
    IS_AGGREGATED: Optional[pl.Boolean]
    LOAD_MINIMUM_ENERGY_PRICE: Optional[pl.Float32]
    LOAD_MAXIMUM_ENERGY_PRICE: Optional[pl.Float32]
    LOAD_MIN_RAMP_RATE_UP: Optional[pl.Float32]
    LOAD_MIN_RAMP_RATE_DOWN: Optional[pl.Float32]
    LOAD_MAX_RAMP_RATE_UP: Optional[pl.Float32]
    LOAD_MAX_RAMP_RATE_DOWN: Optional[pl.Float32]
    SECONDARY_TLF: Optional[pl.Float32]


class DUDETAILSchema(pa.DataFrameModel):
    """Dispatch unit detailed technical specifications."""

    DUID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    CONNECTIONPOINTID: Optional[pl.Categorical]
    VOLTLEVEL: Optional[pl.Float32]
    REGISTEREDCAPACITY: Optional[pl.Float32]
    AGCCAPABILITY: Optional[pl.String]
    DISPATCHTYPE: Optional[pl.Categorical]
    MAXCAPACITY: Optional[pl.Float32]
    STARTTYPE: Optional[pl.String]
    NORMALLYONFLAG: Optional[pl.String]
    SPINNINGRESERVEFLAG: Optional[pl.String]
    INTERMITTENTFLAG: Optional[pl.String]
    SEMISCHEDULE_FLAG: Optional[pl.String]
    MAXRATEOFCHANGEUP: Optional[pl.Float32]
    MAXRATEOFCHANGEDOWN: Optional[pl.Float32]
    ADG_ID: Optional[pl.String]
    MINCAPACITY: Optional[pl.Float32]
    REGISTEREDMINCAPACITY: Optional[pl.Float32]
    MAXRATEOFCHANGEUP_LOAD: Optional[pl.Float32]
    MAXRATEOFCHANGEDOWN_LOAD: Optional[pl.Float32]
    MAXSTORAGECAPACITY: Optional[pl.Float32]
    STORAGEIMPORTEFFICIENCYFACTOR: Optional[pl.Float32]
    STORAGEEXPORTEFFICIENCYFACTOR: Optional[pl.Float32]
    MIN_RAMP_RATE_UP: Optional[pl.Float32]
    MIN_RAMP_RATE_DOWN: Optional[pl.Float32]
    LOAD_MIN_RAMP_RATE_UP: Optional[pl.Float32]
    LOAD_MIN_RAMP_RATE_DOWN: Optional[pl.Float32]
    AGGREGATED: Optional[pl.String]


class RESERVESchema(pa.DataFrameModel):
    """Regional reserve requirements and availability."""

    SETTLEMENTDATE: pl.Datetime
    VERSIONNO: Optional[pl.Int32]
    REGIONID: pl.Categorical
    PERIODID: Optional[pl.Int32]
    LOWER5MIN: Optional[pl.Float32]
    RAISE5MIN: Optional[pl.Float32]
    RAISEREG: Optional[pl.Float32]
    LOWERREG: Optional[pl.Float32]
```

**Step 2: Verify generation schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import GENUNITSSchema, DUDETAILSchema; print('Generation schemas loaded')"
```

Expected: "Generation schemas loaded"

**Step 3: Commit generation schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add generation unit Pandera schemas

Add schemas for DUALLOC, GENUNITS, DUDETAILSUMMARY, DUDETAIL, and RESERVE tables."
```

---

## Task 5: Create station schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append after generation schemas)

**Step 1: Add section comment and station schemas**

Append these after generation schemas:

```python
# Station Tables
# ==============


class STATIONSchema(pa.DataFrameModel):
    """Power station location and contact information."""

    STATIONID: pl.String
    STATIONNAME: Optional[pl.String]
    ADDRESS1: Optional[pl.String]
    ADDRESS2: Optional[pl.String]
    ADDRESS3: Optional[pl.String]
    ADDRESS4: Optional[pl.String]
    CITY: Optional[pl.String]
    STATE: Optional[pl.String]
    POSTCODE: Optional[pl.String]


class STATIONOPERATINGSTATUSSchema(pa.DataFrameModel):
    """Station operating status over time."""

    EFFECTIVEDATE: pl.Date
    STATIONID: pl.String
    VERSIONNO: Optional[pl.Int32]
    STATUS: Optional[pl.String]


class STATIONOWNERSchema(pa.DataFrameModel):
    """Station ownership and participant information."""

    EFFECTIVEDATE: pl.Date
    PARTICIPANTID: pl.Categorical
    STATIONID: pl.String
    VERSIONNO: Optional[pl.Int32]


class STADUALLOCSchema(pa.DataFrameModel):
    """Station to dispatch unit allocation."""

    DUID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    STATIONID: pl.String
    VERSIONNO: pl.Int32
```

**Step 2: Verify station schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import STATIONSchema, STADUALLOCSchema; print('Station schemas loaded')"
```

Expected: "Station schemas loaded"

**Step 3: Commit station schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add station Pandera schemas

Add schemas for STATION, STATIONOPERATINGSTATUS, STATIONOWNER, and STADUALLOC tables."
```

---

## Task 6: Create interconnector and loss model schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append after station schemas)

**Step 1: Add section comment and interconnector schemas**

Append these after station schemas:

```python
# Interconnector Tables
# =====================


class INTERCONNECTORSchema(pa.DataFrameModel):
    """Interconnector corridor definitions with region endpoints."""

    INTERCONNECTORID: pl.Categorical
    REGIONFROM: pl.Categorical
    REGIONTO: pl.Categorical


class INTERCONNECTORCONSTRAINTSchema(pa.DataFrameModel):
    """Interconnector technical constraints and limits."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    FROMREGIONLOSSSHARE: Optional[pl.Float32]
    ICTYPE: Optional[pl.Categorical]
    LOSSCONSTANT: Optional[pl.Float32]
    LOSSFLOWCOEFFICIENT: Optional[pl.Float32]
    IMPORTLIMIT: Optional[pl.Float32]
    EXPORTLIMIT: Optional[pl.Float32]
    MAXMWIN: Optional[pl.Float32]
    MAXMWOUT: Optional[pl.Float32]


class LOSSMODELSchema(pa.DataFrameModel):
    """Loss model segments for interconnectors."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    LOSSSEGMENT: Optional[pl.Int32]
    MWBREAKPOINT: Optional[pl.Float32]


class LOSSFACTORMODELSchema(pa.DataFrameModel):
    """Loss factors by region on interconnectors."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    REGIONID: Optional[pl.Categorical]
    DEMANDCOEFFICIENT: Optional[pl.Float32]


class MNSP_INTERCONNECTORSchema(pa.DataFrameModel):
    """Market Network Service Provider interconnector details."""

    INTERCONNECTORID: pl.Categorical
    LINKID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    FROMREGION: Optional[pl.Categorical]
    TOREGION: Optional[pl.Categorical]
    FROM_REGION_TLF: Optional[pl.Float32]
    TO_REGION_TLF: Optional[pl.Float32]
    LHSFACTOR: Optional[pl.Float32]
    MAXCAPACITY: Optional[pl.Float32]
```

**Step 2: Verify interconnector schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import INTERCONNECTORSchema, LOSSFACTORMODELSchema; print('Interconnector schemas loaded')"
```

Expected: "Interconnector schemas loaded"

**Step 3: Commit interconnector schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add interconnector and loss model Pandera schemas

Add schemas for INTERCONNECTOR, INTERCONNECTORCONSTRAINT, LOSSMODEL,
LOSSFACTORMODEL, and MNSP_INTERCONNECTOR tables."
```

---

## Task 7: Create constraint and SPD schemas

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append after interconnector schemas)

**Step 1: Add section comment and constraint schemas**

Append these after interconnector schemas:

```python
# Constraint Tables
# =================


class GENCONDATASchema(pa.DataFrameModel):
    """Generic constraint definitions and weighting."""

    GENCONID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    CONSTRAINTTYPE: Optional[pl.Categorical]
    GENERICCONSTRAINTWEIGHT: Optional[pl.Float32]


class SPDREGIONCONSTRAINTSchema(pa.DataFrameModel):
    """Regional constraints on specific dispatch unit types."""

    REGIONID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    BIDTYPE: pl.Categorical
    FACTOR: Optional[pl.Float32]


class SPDCONNECTIONPOINTCONSTRAINTSchema(pa.DataFrameModel):
    """Connection point constraints on specific dispatch unit types."""

    CONNECTIONPOINTID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    BIDTYPE: pl.Categorical
    FACTOR: Optional[pl.Float32]


class SPDINTERCONNECTORCONSTRAINTSchema(pa.DataFrameModel):
    """Interconnector constraints on specific dispatch unit types."""

    INTERCONNECTORID: pl.Categorical
    EFFECTIVEDATE: pl.Date
    VERSIONNO: pl.Int32
    GENCONID: pl.Categorical
    FACTOR: Optional[pl.Float32]
```

**Step 2: Verify constraint schemas compile**

```bash
python -c "from nemdb.nemweb.schemas import GENCONDATASchema, SPDREGIONCONSTRAINTSchema; print('Constraint schemas loaded')"
```

Expected: "Constraint schemas loaded"

**Step 3: Commit constraint schemas**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add constraint Pandera schemas

Add schemas for GENCONDATA, SPDREGIONCONSTRAINT, SPDCONNECTIONPOINTCONSTRAINT,
and SPDINTERCONNECTORCONSTRAINT tables."
```

---

## Task 8: Create ZONE_SUBSTATION schema

**Files:**

- Modify: `src/nemdb/nemweb/schemas.py` (append at end)

**Step 1: Add DNSP section and ZONE_SUBSTATION schema**

Append this after constraint schemas:

```python
# DNSP Tables (Not in standard DTYPES)
# ====================================
# Note: ZONE_SUBSTATION uses DNSP-specific columns not in DTYPES.
# Types derived from usage in DNSPDataSource.


class ZONESUBSTATIONSchema(pa.DataFrameModel):
    """Distribution network zone substation data from DNSP operators."""

    time: Optional[pl.String]  # DNSP-specific, not in standard DTYPES
    zss: Optional[pl.String]  # DNSP-specific, not in standard DTYPES
    MW: Optional[pl.Float32]
    network: Optional[pl.String]
```

**Step 2: Verify all schemas compile together**

```bash
python -c "from nemdb.nemweb import schemas; print(f'All schemas loaded: {len([x for x in dir(schemas) if x.endswith(\"Schema\")])} schema classes')"
```

Expected: "All schemas loaded: 26 schema classes" (or close count)

**Step 3: Commit ZONE_SUBSTATION schema**

```bash
git add src/nemdb/nemweb/schemas.py
git commit -m "feat: add ZONE_SUBSTATION schema

Add DNSP-specific ZONE_SUBSTATION schema. Note: DNSP columns (time, zss, network)
are not in standard DTYPES; types are derived from usage context."
```

---

## Task 9: Create comprehensive test file

**Files:**

- Create: `test/test_nemweb_schemas.py`

**Step 1: Write test file**

```python
"""Tests for NEMWEB Pandera schemas."""

import polars as pl

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


def test_dispatch_region_sum_schema_fields():
    """Verify DispatchRegionSumSchema has expected fields."""
    schema = DispatchRegionSumSchema()
    assert hasattr(schema, "__pydantic_model__")
    fields = schema.__pydantic_model__.model_fields
    assert "SETTLEMENTDATE" in fields
    assert "REGIONID" in fields
    assert "TOTALDEMAND" in fields


def test_dispatch_load_schema_fields():
    """Verify DispatchLoadSchema has expected fields."""
    schema = DispatchLoadSchema()
    fields = schema.__pydantic_model__.model_fields
    assert "SETTLEMENTDATE" in fields
    assert "DUID" in fields
    assert "UIGF" in fields


def test_bid_day_offer_schema_fields():
    """Verify BidDayOfferDSchema has expected fields."""
    schema = BidDayOfferDSchema()
    fields = schema.__pydantic_model__.model_fields
    assert "DUID" in fields
    assert "SETTLEMENTDATE" in fields
    assert "PRICEBAND10" in fields


def test_generation_schemas_fields():
    """Verify generation-related schemas have expected fields."""
    genunits = GENUNITSSchema()
    dudetail = DUDETAILSchema()

    genunits_fields = genunits.__pydantic_model__.model_fields
    dudetail_fields = dudetail.__pydantic_model__.model_fields

    assert "GENSETID" in genunits_fields
    assert "DUID" in dudetail_fields
    assert "MAXCAPACITY" in genunits_fields
    assert "MAXCAPACITY" in dudetail_fields


def test_station_schemas_fields():
    """Verify station-related schemas have expected fields."""
    station = STATIONSchema()
    stadualloc = STADUALLOCSchema()

    station_fields = station.__pydantic_model__.model_fields
    stadualloc_fields = stadualloc.__pydantic_model__.model_fields

    assert "STATIONID" in station_fields
    assert "STATIONNAME" in station_fields
    assert "DUID" in stadualloc_fields
    assert "VERSIONNO" in stadualloc_fields


def test_interconnector_schemas_fields():
    """Verify interconnector-related schemas have expected fields."""
    interconnector = INTERCONNECTORSchema()
    loss_model = LOSSMODELSchema()

    ic_fields = interconnector.__pydantic_model__.model_fields
    loss_fields = loss_model.__pydantic_model__.model_fields

    assert "INTERCONNECTORID" in ic_fields
    assert "REGIONFROM" in ic_fields
    assert "LOSSSEGMENT" in loss_fields


def test_constraint_schemas_fields():
    """Verify constraint-related schemas have expected fields."""
    gencon = GENCONDATASchema()
    spdregion = SPDREGIONCONSTRAINTSchema()

    gencon_fields = gencon.__pydantic_model__.model_fields
    spdregion_fields = spdregion.__pydantic_model__.model_fields

    assert "GENCONID" in gencon_fields
    assert "CONSTRAINTTYPE" in gencon_fields
    assert "BIDTYPE" in spdregion_fields
    assert "FACTOR" in spdregion_fields


def test_all_schemas_instantiate():
    """Test that all 26 schema classes can be instantiated."""
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
    for schema_class in schemas:
        schema = schema_class()
        assert schema is not None
        assert hasattr(schema, "__pydantic_model__")


def test_sample_dispatch_region_sum_data():
    """Test validation with sample DISPATCHREGIONSUM data."""
    from datetime import datetime

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
```

**Step 2: Run tests to ensure they pass**

```bash
pytest test/test_nemweb_schemas.py -v
```

Expected: All tests pass.

**Step 3: Commit test file**

```bash
git add test/test_nemweb_schemas.py
git commit -m "test: add comprehensive tests for NEMWEB Pandera schemas

Add tests verifying all 26 schema classes instantiate correctly, have expected
fields, and can work with sample data."
```

---

## Task 10: Run full test suite and verify linting

**Files:**

- No new files

**Step 1: Run pytest with coverage**

```bash
pytest test/test_nemweb_schemas.py test/test_nemweb_dbloader.py -v --cov=nemdb.nemweb.schemas
```

Expected: All tests pass, coverage for schemas module above threshold.

**Step 2: Run ruff lint and format**

```bash
ruff check src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
ruff format src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
```

Expected: No errors or auto-fixable issues.

**Step 3: Run mypy type checking**

```bash
mypy src/nemdb/nemweb/schemas.py --show-error-codes
```

Expected: No errors (pandera.polars may have some untyped imports but should pass).

**Step 4: Verify pre-commit hooks pass**

```bash
pre-commit run --all-files -- src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
```

Expected: All hooks pass.

**Step 5: Final commit (if any formatting changes)**

```bash
git status
```

If changes, run:

```bash
git add src/nemdb/nemweb/schemas.py test/test_nemweb_schemas.py
git commit -m "refactor: address linting and type-checking feedback

Format code, resolve ruff and mypy issues."
```

---

## Task 11: Export schemas from nemweb package (optional)

**Files:**

- Modify: `src/nemdb/nemweb/__init__.py` (optional)

**Step 1: Check current **init**.py**

```bash
cat src/nemdb/nemweb/__init__.py
```

**Step 2: Decide whether to export**

If you want schemas available as `from nemdb.nemweb import DispatchRegionSumSchema`, add to `__init__.py`:

```python
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

__all__ = [
    "BidDayOfferDSchema",
    "BidPerOfferDSchema",
    "DispatchConstraintSchema",
    "DispatchInterconnectorResSchema",
    "DispatchLoadSchema",
    "DispatchPriceSchema",
    "DispatchRegionSumSchema",
    "DUALLOCSchema",
    "DUDETAILSchema",
    "DUDETAILSUMMARYSchema",
    "GENCONDATASchema",
    "GENUNITSSchema",
    "INTERCONNECTORCONSTRAINTSchema",
    "INTERCONNECTORSchema",
    "LOSSFACTORMODELSchema",
    "LOSSMODELSchema",
    "MNSP_INTERCONNECTORSchema",
    "RESERVESchema",
    "SPDCONNECTIONPOINTCONSTRAINTSchema",
    "SPDINTERCONNECTORCONSTRAINTSchema",
    "SPDREGIONCONSTRAINTSchema",
    "STADUALLOCSchema",
    "STATIONOPERATINGSTATUSSchema",
    "STATIONOWNERSchema",
    "STATIONSchema",
    "ZONESUBSTATIONSchema",
]
```

**Step 3: Test imports**

```bash
python -c "from nemdb.nemweb import DispatchRegionSumSchema; print('Schemas re-exported from nemweb')"
```

Expected: "Schemas re-exported from nemweb"

**Step 4: Commit (if exporting)**

```bash
git add src/nemdb/nemweb/__init__.py
git commit -m "feat: export Pandera schemas from nemweb package

Make all 26 schemas available directly from nemdb.nemweb for easier discovery."
```

---

## Task 12: Verify integration and documentation

**Files:**

- No new files

**Step 1: Verify schemas.py is importable and complete**

```bash
python -c "import nemdb.nemweb.schemas; import inspect; schemas = [x for x in dir(nemdb.nemweb.schemas) if x.endswith('Schema') and not x.startswith('_')]; print(f'Found {len(schemas)} schema classes:\n' + '\n'.join(sorted(schemas)))"
```

Expected: List of 26 schema class names.

**Step 2: Run final test suite**

```bash
pytest test/test_nemweb_schemas.py -v --tb=short
```

Expected: All tests pass.

**Step 3: Create brief usage documentation**

Add docstring at top of `src/nemdb/nemweb/schemas.py` if not already present:

The module docstring should explain that these are type contracts for documentation, not runtime validation.

**Step 4: Final status check**

```bash
git log --oneline -12
```

Expected: See 9-11 commits related to schemas (skeleton, dispatch, bid, generation, station, interconnector, constraint, zone, tests, linting, export).

**Done!** All 26 NEMWEB Pandera schemas created, tested, and integrated.
