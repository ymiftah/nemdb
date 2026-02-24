# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
