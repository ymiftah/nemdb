# Configuration reference

## `Config` class

::: nemdb.config.Config

The global configuration is defined in `nemdb.config.Config` and controls data storage locations and backend.

| Attribute | Default | Environment variable | Description |
|-----------|---------|----------------------|-------------|
| `CACHE_DIR` | `~/.nemweb_cache` | `NEMDB_CACHE_DIR` | Root directory for all cached data |
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
| `--location` | Path/URI | `~/.nemweb_cache` | Where to write data |
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
CACHE_DIR/
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
└── ...
```

NEMWEB tables are partitioned by `archive_month` (first of month). DNSP data is partitioned by `network` and `year`. Geodata files are single Parquet files (not partitioned).

## Caching behaviour

- **NEMWEB tables**: cached as Parquet. `populate()` checks for existing data before downloading. Use `force_new=True` to re-download.
- **Geodata**: cached via `@cache_to_parquet` decorator. Delete the Parquet file to force a refresh.
- **OpenNEM API**: cached via joblib in `TEMP_DIR`. Call `memory.clear()` to invalidate.
- **Bid data**: cached as ZIP files in `TEMP_DIR` via `@cache_response_zip`.
