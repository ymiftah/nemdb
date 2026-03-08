# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-03-07

### Added

- **ISP 2025 Sheet Extraction** (`src/nemdb/isp/isp2025.py`): Full extraction layer for
  all 73 extractable sheets from `ISP_2025.xlsm`
  - `read_sheet(name, header_row)` — plain DataFrame extraction with automatic preamble skipping
  - `read_timeseries(name, header_row)` — unpivots financial-year columns to tidy long format
  - ~55 sheet-specific functions covering dispatch, generation, fuel prices, economic growth,
    network constraints, storage, electrification, and more
  - Bespoke parsers for structurally irregular sheets (stacked tables, null-column year rows,
    stacked-scenario Pattern F sheets)
  - `ISP2025` convenience class with cached property access for all sheets
  - Support for reading directly from the zipped `.xlsm` file
- **ISP 2025 Pandera Schemas** (`src/nemdb/isp/schemas.py`): 69 `pa.DataFrameModel` subclasses,
  one per extracted table, with typed fields and `pa.Field(alias=...)` for non-identifier columns
- **ISP 2025 Reference Documentation** (`docs/reference/isp-2025-sheet-structure.md`): Describes
  the sheet structure and extraction patterns
- **73 tests** in `test/test_isp2025.py` covering all sheet extraction functions
- **Pooch-based ISP 2025 spreadsheet distribution**: The `ISP_2025.xlsm` workbook is hosted as
  a GitHub release asset (`data-v1`) and fetched on demand via `pooch`, with SHA-256 integrity
  verification. The file is cached under `NEMDB_CACHE_DIR` (default `~/.nemdb_cache`) and can
  be overridden locally by setting the `NEMDB_ISP_2025` environment variable to a local zip path.
- **Pooch-based NEM facilities distribution** (`src/nemdb/opennem/opennemapi.py`): A pre-built
  snapshot of the OpenElectricity NEM facilities table is hosted as a GitHub release asset
  (`data-v2`, `facilities_nem.parquet`) and fetched on demand via `pooch` — no OpenElectricity
  account required.
  - `read_facilities_cached()` — loads the parquet snapshot via pooch; respects a
    `NEMDB_FACILITIES` environment variable override for local files.
  - `match_facilities_to_gis()` gains a `source` parameter (`"pooch"` | `"api"`, default
    `"pooch"`) controlling whether facilities are fetched from the cached parquet or live from
    the OpenElectricity API.
  - `get_pandapower_model_with_opennem()` and `create_pandapower_network()` expose the same
    `source` parameter, making the previously hidden HTTP dependency explicit.
  - `scripts/extract_facilities.py` — one-off script to regenerate the parquet snapshot from
    the API and print the SHA-256 for updating the registry.

### Changed

- **Global config singleton**: `NEMWEBManager` and `DataSource` no longer accept a `config`
  argument — they read from the module-level `config` singleton (`nemdb.config`) directly.
  Tests that need an isolated cache directory should mutate `config.cache_dir` / `config.temp_dir`
  and restore them on teardown (a `conftest.py` autouse fixture is provided for this).
- **Partitioned schema layout** in `src/nemdb/nemweb/schemas.py`: reorganised schema definitions
  into logical groups for improved readability
- **`isp2025.py` parsing improvements**: refined column type handling and parsing logic across
  multiple sheet types
- **Doc fix**: removed stale `config` parameter from `DataSource` docstrings after config
  singleton refactor

## [0.3.0] - 2026-02-25

### Added

- **Pandera Schemas** (`src/nemdb/nemweb/schemas.py`): 26 typed Pandera schema classes for all
  NEMWEB tables, providing IDE type inference and optional runtime validation
  - Schemas for dispatch, bid, generation unit, station, interconnector, loss model,
    constraint, and DNSP tables
  - All schemas exported from `nemdb.nemweb` for convenient imports
    (e.g. `from nemdb.nemweb import DispatchLoadSchema`)
- **Schema-driven DataSource**: `DataSource` now accepts a `schema_class` parameter to attach
  a Pandera schema for type safety and IDE inference
- **SCHEMA_MAP registry**: Table-to-schema discovery via `SCHEMA_MAP` dict
- **`validate_against_schema()` utility**: Opt-in runtime DataFrame validation against schemas
  with no performance overhead when not used

### Changed

- **Single source of truth for types**: The `DTYPES` dictionary (200+ lines) has been removed
  from `dbloader.py`; column types are now derived automatically from Pandera schema annotations
- `_archive_to_df()` and `_archive_to_df_low_memory()` now accept a `dtypes` parameter instead
  of referencing the global `DTYPES` dict

### Documentation

- Added design and implementation plan docs under `docs/plans/`

## [0.2.0] - 2026-02-16

### Fixed

- **Cross-voltage island connections**: Fixed network constraint violation where synthetic transmission lines were directly connecting buses with different nominal voltages
  - Implemented synthetic intermediate buses at island voltage levels for cross-voltage connections
  - All synthetic lines now properly connect same-voltage bus pairs
  - Cross-voltage bridging handled through transformers instead of direct line connections
  - Added `_create_cross_voltage_connection()` helper function for proper transformer architecture
  - Network sanity check `different_voltage_levels_connected` now passes ✓

### Added

- **Island Connectivity Explanation** (`docs/explanation/island-connectivity.md`): Comprehensive guide to island detection, connection strategies, and cross-voltage bridging architecture

### Dependencies

- Upgraded GitHub Actions dependencies:
  - Bump astral-sh/setup-uv from 6 to 7
  - Bump actions/setup-python from 5 to 6
  - Bump actions/checkout from 4 to 6

## [0.1.0] - 2026-02-15

### Added

- **Visualization Module** (`src/nemdb/models/visualize.py`): Interactive Plotly-based visualization for NEM transmission network with geographic mapping
- **Geodata Matching** (`src/nemdb/geodata/matching.py`): Enhanced geodata matching capabilities for facility-to-gis integration
- **Comprehensive Documentation**:
  - Getting started guide
  - Tutorial: Build a network model
  - How-to guides: Fetch NEMWEB data, work with geodata, use DNSP data, use ISP assumptions, use OpenNEM API
  - Explanation: Transmission line cleaning procedures
  - Reference: Configuration documentation
- **Test Coverage**:
  - Pandapower connectivity tests (`test/test_pandapower_connectivity.py`)
  - Visualization tests (`test/test_visualize.py`)
- **Python Best Practices Tooling**:
  - Pre-commit hooks configuration with YAML, TOML, Markdown, and Python linting
  - Ruff configuration for code quality
  - MyPy configuration for type checking
  - Rumdl configuration for markdown linting

### Fixed

- **Pre-commit Hook Compliance**: All 12 pre-commit hooks now pass
  - Fixed YAML validation issues in mkdocs.yml
  - Fixed Python linting errors (72+ issues resolved)
  - Fixed markdown formatting and line length issues
  - Fixed type checking errors
- **Code Quality**:
  - Removed 70+ lines of commented-out code from pandapower.py
  - Refactored `_validate_and_fix_connectivity()` function to reduce cyclomatic complexity (17 → 12 branches)
    - Extracted `_build_connectivity_graph()` helper function
    - Extracted `_connect_islands()` helper function
  - Added proper type annotations in visualize.py
  - Fixed return type assertions in pandapower.py with explicit type casts
  - Updated method signatures in dbloader.py to handle signature mismatches
  - Fixed test code in dnsp modules to use correct year integers instead of file paths
- **Type Safety**:
  - Added mypy configuration to handle missing library stubs
  - Removed unnecessary type: ignore comments
  - Kept legitimate type: ignore comments only where genuinely needed (sapn.py line 78)

### Changed

- **DNSP Module Test Code**: Updated all test code blocks to use year-based function calls
  - Changed from: `read_all_zss('/path/to/file.zip')`
  - Changed to: `read_all_zss(2024)`
- **Markdown Configuration**: Created rumdl.toml with sensible defaults
  - Set line length to 120 characters (practical for documentation with URLs)
  - Disabled MD057 rule (broken links) since api/ is auto-generated by mkdocs-autoapi
- **Python Version**: Support for Python 3.13+ with proper type hints

### Dependencies

- Added visualization dependencies: `plotly>=6.5.2`, `nbformat>=5.10.4`
- Added grid modeling dependencies: `pandapower>=3.2.0`, `scikit-learn>=1.7.2`
- Upgraded development dependencies for improved tooling

### Documentation

- Restructured documentation with mkdocs
- Added material theme for better presentation
- Comprehensive API documentation via mkdocs-autoapi
- Docstring documentation via mkdocstrings

## [0.0.1] - Previous Release

Initial release with core functionality for NEMDB data processing.

---

**Release Date**: 2026-02-15
**Commit**: 8be9a79
**Branch**: extract-visualization-functions
