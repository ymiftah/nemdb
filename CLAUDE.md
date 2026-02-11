# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NEMDB is a Python package for fetching, caching, and accessing data from the Australian National Electricity Market (NEM). It downloads MMSDM tables from AEMO's NEMWEB archive, stores them as partitioned parquet files, and provides interfaces for market data analysis, geospatial infrastructure data, and power system modelling.

## Common Commands

```bash
# Install dependencies
uv sync                      # core deps
uv sync --group dev          # dev deps (ruff, pytest, pre-commit)
uv sync --all-extras         # all optional groups (grid, viz, doc)

# Lint and format
uv run ruff check .          # lint
uv run ruff format --check . # check formatting
uv run ruff format .         # auto-format

# Test
uv run pytest                # run full test suite
uv run pytest test/test_nemweb_dbloader.py           # single test file
uv run pytest test/test_nemweb_dbloader.py::test_name -v  # single test

# CLI
uv run populate --help
uv run populate --location ./data --date_range 2024-01-01->2024-03-31
uv run populate --location ./data --date_range 2024-01-01->2024-03-31 --table DISPATCHREGIONSUM
```

## Architecture

### Data Flow

NEMWEB HTTP archives → download as zip → parse CSV → cast types with Polars → write partitioned parquet → read via `scan_parquet` (lazy)

### Key Abstractions

**`NEMWEBManager`** (`src/nemdb/nemweb/dbloader.py`) — Central class that manages downloading and reading 50+ MMSDM tables. Each table is backed by a `DataSource` subclass that determines its time-indexing/partitioning strategy:
- `BySettlementDate` — dispatch data indexed by settlement period
- `ByIntervalDate` — interval-based data
- `BySettlementDay` — daily aggregates
- `ByStartEnd` — effective date ranges
- `ByEffectiveDateVersionNo` — versioned static reference data

The NEMWEBManager interface is compatible with the [nempy](https://github.com/UNSW-CEEM/nempy) library.

**`Config`** (`src/nemdb/config.py`) — Global config with `CACHE_DIR`, `FILESYSTEM`, and `TEMP_DIR`. Filesystem is abstracted via fsspec (supports local, S3, GCS). Set via env vars `NEMDB_CACHE_DIR` and `NEMDB_FILESYSTEM`.

**`@cache_to_parquet`** (`src/nemdb/utils.py`) — Decorator that caches function results as parquet files. Dispatches read/write based on return type (Polars DataFrame, GeoPandas GeoDataFrame, or Pandas DataFrame).

### Module Layout

- **`nemweb/`** — Core NEMWEB data fetching, the `NEMWEBManager`, and DER register data
- **`dnsp/`** — Distribution Network Service Provider zone substation data (Ausgrid, AusNet, Endeavour, Energex, etc.)
- **`geodata/`** — Substation locations (from Geoscience Australia API) and transmission line geometries
- **`models/`** — PandaPower model generation from geospatial + market data; uses DBSCAN clustering to map line extremities to buses
- **`isp/`** — ISP (Integrated System Plan) assumptions reader from Excel workbooks in `artefacts/`
- **`opennem/`** — Async client for the OpenElectricity API (requires `OPENELECTRICITY_API_KEY` in `.env`)
- **`near/`** — Maps facilities to nearest power stations using GeoPandas spatial joins

### Data Libraries

The project uses **Polars** as the primary DataFrame library for performance and lazy evaluation. Pandas and GeoPandas are used where spatial operations or API compatibility requires them. PyArrow is the parquet backend.

## CI

CI runs on push/PR to main: lint (ruff) → test (pytest) → deploy docs (mkdocs, main only). Pre-commit hooks run ruff check with `--fix` and ruff format.

## Environment

- Python >=3.13, managed with `uv`
- Build system: hatchling
- `.env` file holds `OPENELECTRICITY_API_KEY` (gitignored)
