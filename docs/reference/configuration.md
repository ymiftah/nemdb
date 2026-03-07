# Configuration reference

## Environment variables

Some of nemdb behaviour is controlled through environment variables. The can be set with a .env file at the root of the project in development (see .env.example for starting)

| Variable | Default | Description |
|----------|---------|-------------|
| [`NEMDB_CACHE_DIR`](#nemdb_cache_dir) | `~/.nemdb_cache` | Root directory for **all** cached data — NEMWEB parquets, pooch downloads, and geodata |
| [`NEMDB_FILESYSTEM`](#nemdb_filesystem) | `"local"` | fsspec storage backend (use `"gcs"` for Google Cloud Storage) |
| [`NEMDB_ISP_2025`](#nemdb_isp_2025) | *(not set)* | Local path override for the ISP 2025 workbook zip — skips the pooch download |
| [`NEMDB_FACILITIES`](#nemdb_facilities) | *(not set)* | Local path override for the NEM facilities parquet — skips the pooch download |

---

### `NEMDB_CACHE_DIR`

**Default:** `~/.nemdb_cache`

Root directory where nemdb stores all cached files. This includes:

- NEMWEB table parquets (partitioned by `archive_month`)
- DNSP zone-substation parquets (partitioned by `network` and `year`)
- Geodata parquets (substations, transmission lines, powerstations)
- Pooch-managed data assets (`ISP_2025.zip`, `facilities_nem.parquet`)

```bash
export NEMDB_CACHE_DIR=/data/nemdb
```

```python
from nemdb import Config
Config.set_cache_dir("/data/nemdb")   # equivalent runtime setter
```

---

### `NEMDB_FILESYSTEM`

**Default:** `"local"`

[fsspec](https://filesystem-spec.readthedocs.io/) filesystem identifier used for reading and writing NEMWEB parquets.
Set to `"gcs"` to read/write from Google Cloud Storage.

```bash
export NEMDB_FILESYSTEM=gcs
export NEMDB_CACHE_DIR=gs://my-bucket/nemdb
```

See [Storage backends](#storage-backends) for the full list of supported values.

---

### `NEMDB_ISP_2025`

**Default:** *(not set — file is downloaded via pooch)*

Absolute path to a local copy of the ISP 2025 workbook, packaged as a zip containing the `.xlsm` file.
When set, nemdb uses this file directly instead of downloading from the GitHub release (`data-v1`).

Useful for:

- Air-gapped environments without internet access.
- Testing against a custom or patched version of the spreadsheet.
- Avoiding re-download during development.

```bash
export NEMDB_ISP_2025=/data/ISP_2025.zip
```

When **not** set, the file is fetched once from:

```text
https://github.com/ymiftah/nemdb/releases/download/data-v1/ISP_2025.zip
```

and cached under `NEMDB_CACHE_DIR`. SHA-256 integrity is verified on every access.

---

### `NEMDB_FACILITIES`

**Default:** *(not set — file is downloaded via pooch)*

Absolute path to a local parquet file containing the NEM facilities table (one row per
generation unit, same schema as the output of `read_facilities()`).
When set, nemdb uses this file directly instead of downloading from the GitHub release (`data-v2`).

Useful for:

- Air-gapped environments or environments without an OpenElectricity account.
- Testing against a custom or refreshed facilities snapshot.
- Providing a more recent snapshot without waiting for a new data release.

```bash
export NEMDB_FACILITIES=/data/facilities_nem.parquet
```

To regenerate the parquet from the live API:

```bash
uv run scripts/extract_facilities.py --output /data/facilities_nem.parquet
```

When **not** set, the file is fetched once from:

```text
https://github.com/ymiftah/nemdb/releases/download/data-v2/facilities_nem.parquet
```

and cached under `NEMDB_CACHE_DIR`. SHA-256 integrity is verified on every access.

---

## `Config` class

::: nemdb.config.Config

The global configuration is defined in `nemdb.config.Config` and controls data storage locations and backend.

| Attribute | Default | Environment variable | Description |
|-----------|---------|----------------------|-------------|
| `CACHE_DIR` | `~/.nemdb_cache` | `NEMDB_CACHE_DIR` | Root directory for all cached data |
| `FILESYSTEM` | `"local"` | `NEMDB_FILESYSTEM` | Storage backend identifier |
| `TEMP_DIR` | `<system-temp>/.nemweb_temp` | -- | Temporary directory for API caches |

### Set at runtime

```python
from nemdb import Config

Config.set_cache_dir("/path/to/data")
Config.set_filesystem("gcs")
```

### Set via environment variables

```bash
export NEMDB_CACHE_DIR=/path/to/data
export NEMDB_FILESYSTEM=gcs
```

## Storage backends

nemdb uses [fsspec](https://filesystem-spec.readthedocs.io/) for filesystem abstraction. Any fsspec-compatible backend can be used:

| Backend | `FILESYSTEM` value | URI example | Extra dependency |
|---------|-------------------|-------------|-----------------|
| Local | `"local"` or `"file"` | `/home/user/data` | -- |
| Google Cloud Storage | `"gcs"` | `gs://bucket/path` | `gcsfs` |
| Amazon S3 | `"s3"` | `s3://bucket/path` | `s3fs` |
| Azure Blob | `"az"` | `az://container/path` | `adlfs` |

## CLI reference

### `populate`

Download and cache NEMWEB data.

```text
uv run populate [OPTIONS]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--location` | Path/URI | `~/.nemdb_cache` | Where to write data |
| `--filesystem` | String | `file` | fsspec filesystem identifier |
| `--date_range` | String | *(required)* | Date range as `YYYY-MM-DD->YYYY-MM-DD` |
| `--table` | String | `all` | Table name or `all` for all active tables |
| `--force_new` | Flag | `False` | Re-download existing data |

## Dependency groups

nemdb uses optional dependency groups for heavyweight packages:

| Group | Install command | Packages |
|-------|----------------|----------|
| *(core)* | `uv pip install -e .` | polars, pandas, geopandas, requests, shapely, click, structlog, ... |
| `grid` | `uv pip install -e ".[grid]"` | pandapower, scikit-learn, networkx |
| `viz` | `uv pip install -e ".[viz]"` | plotly, nbformat |

## Data storage layout

All data is stored as Hive-partitioned Parquet under `CACHE_DIR`:

```text
CACHE_DIR/                                  ← NEMDB_CACHE_DIR (default ~/.nemdb_cache)
├── DISPATCHREGIONSUM/
│   └── archive_month=2024-01-01/
│       └── DISPATCHREGIONSUM-0.parquet
├── DISPATCHLOAD/
│   └── archive_month=2024-01-01/
│       └── DISPATCHLOAD-0.parquet
├── ZONE_SUBSTATION/
│   └── network=ausgrid/
│       └── year=2024/
│           └── ZONE_SUBSTATION-0.parquet
├── geodata/
│   ├── substations.parquet
│   ├── transmission_lines.parquet
│   ├── transmission_lines_clean.parquet
│   └── powerstations.parquet
├── ISP_2025.zip                            ← pooch data-v1 (overridden by NEMDB_ISP_2025)
└── facilities_nem.parquet                  ← pooch data-v2 (overridden by NEMDB_FACILITIES)
```

NEMWEB tables are partitioned by `archive_month` (first of month). DNSP data is partitioned by
`network` and `year`. Geodata files are single Parquet files (not partitioned). Pooch-managed
assets (`ISP_2025.zip`, `facilities_nem.parquet`) are flat files at the root of `CACHE_DIR`.

## Caching behaviour

- **NEMWEB tables**: cached as Parquet. `populate()` checks for existing data before downloading. Use `force_new=True` to re-download.
- **Geodata**: cached via `@cache_to_parquet` decorator. Delete the Parquet file to force a refresh.
- **OpenNEM API**: cached via joblib in `TEMP_DIR`. Call `memory.clear()` to invalidate.
- **Bid data**: cached as ZIP files in `TEMP_DIR` via `@cache_response_zip`.
- **Pooch assets** (`ISP_2025.zip`, `facilities_nem.parquet`): downloaded once into `CACHE_DIR`
  and SHA-256 verified on every access. Delete the file to force a re-download. Use the
  corresponding environment variable override (`NEMDB_ISP_2025`, `NEMDB_FACILITIES`) to point
  at a local copy instead.
