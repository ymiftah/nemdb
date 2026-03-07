# NEMDB

Python package for fetching, caching, and querying AEMO NEM data from NEMWEB as Parquet files.

## Commands

```bash
uv sync --all-extras          # Install all dependencies
uv run pytest                 # Run tests (min 40% coverage enforced)
uv run pytest -m "not slow"   # Skip slow/integration tests
uv run ruff check src/        # Lint
uv run ruff format src/       # Format
uv run mypy src/              # Type check
uv run populate --help        # CLI help
```

## Architecture

```text
src/nemdb/
├── config.py            # Config class — reads NEMDB_CACHE_DIR, NEMDB_FILESYSTEM
├── main.py              # CLI entry point (populate command)
├── nemweb/
│   ├── dbloader.py      # NEMWEBManager + DataSource subclasses
│   ├── schemas.py       # Pandera schemas + SCHEMA_MAP
│   └── nemweb.py        # Low-level HTTP/zip utilities
├── dnsp/                # Distribution network substation data
├── isp/                 # ISP (Integrated System Plan) assumptions
├── geodata/             # Geospatial utilities
└── models/              # Grid models (pandapower)
```

## Key Patterns

**DataSource subclasses** — pick based on how the table is queried:

- `DataSource` — no time filter (STATION, PARTICIPANT, GENUNITS)
- `BySettlementDate` — 5-min dispatch interval (DISPATCHLOAD, DISPATCHPRICE)
- `ByEffectiveDateVersionNo` — latest version at date (GENCONDATA, DUDETAIL)
- `ByStartEnd` — validity window START_DATE/END_DATE (DUDETAILSUMMARY)

**Adding a new NEMWEB table:**

1. Look up the table's columns and types in the AEMO MMS Data Model Report:
   - Index of packages: <https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report_files/Elec43.htm>
   - Each package links to a detail page (e.g. Elec44.htm) with full column specs
   - Note which columns are mandatory (primary keys) vs optional (nullable)
2. Add Pandera schema in `nemweb/schemas.py`, register in `SCHEMA_MAP`
3. Instantiate correct `DataSource` subclass in `NEMWEBManager.__init__`
4. Add table name to `_active_tables`

**Storage** — Parquet, partitioned by `archive_month` (date `YYYY-MM-01`) under `NEMDB_CACHE_DIR/<TABLE>/`.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `NEMDB_CACHE_DIR` | `~/.nemdb_cache` | Local parquet cache root |
| `NEMDB_FILESYSTEM` | `local` | fsspec filesystem (`gcs` for cloud) |
