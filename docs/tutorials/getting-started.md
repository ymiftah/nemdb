# Getting started

This tutorial walks you through installing nemdb, downloading your first dataset, and querying it from Python. By the end you will have a local cache of NEM dispatch data and know how to filter it by date and region.

## Prerequisites

- Python 3.13 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Internet connection (data is downloaded from AEMO's NEMWEB portal)

## 1. Install nemdb

Clone the repository and install in development mode:

```bash
git clone https://github.com/ymiftah/nemdb.git
cd nemdb
uv pip install -e .
```

Verify the installation:

```bash
python -c "from nemdb import NEMWEBManager; print('OK')"
```

## 2. Download data with the CLI

The `populate` command downloads NEMWEB tables and stores them as Parquet files. Let's fetch one month of data:

```bash
uv run populate --location ./data --date_range 2024-01-01->2024-01-31
```

When prompted for a table, type `all` to download all active tables, or enter a specific table name like `DISPATCHREGIONSUM`.

The data is saved under `./data/` in Hive-partitioned Parquet format. Each table gets its own directory:

```text
data/
├── DISPATCHREGIONSUM/
│   └── archive_month=2024-01-01/
│       └── DISPATCHREGIONSUM-0.parquet
├── DISPATCHLOAD/
│   └── ...
└── ...
```

## 3. Query data from Python

Create a Python script or open a REPL:

```python
from nemdb import NEMWEBManager, Config

# Point to your data directory
Config.set_cache_dir("./data")

# Create the manager
nemweb = NEMWEBManager(Config)

# List available tables
print(nemweb)
```

### Query by settlement date

Tables like `DISPATCHREGIONSUM` are indexed by 5-minute settlement dates:

```python
# Get regional demand at noon on January 15
df = nemweb.DISPATCHREGIONSUM.get_data("2024/01/15 12:00:00")
print(df.select("REGIONID", "TOTALDEMAND", "DEMANDFORECAST"))
```

Expected output:

```text
shape: (5, 3)
┌──────────┬─────────────┬────────────────┐
│ REGIONID ┆ TOTALDEMAND ┆ DEMANDFORECAST │
│ ---      ┆ ---         ┆ ---            │
│ cat      ┆ f32         ┆ f32            │
╞══════════╪═════════════╪════════════════╡
│ NSW1     ┆ ...         ┆ ...            │
│ QLD1     ┆ ...         ┆ ...            │
│ SA1      ┆ ...         ┆ ...            │
│ TAS1     ┆ ...         ┆ ...            │
│ VIC1     ┆ ...         ┆ ...            │
└──────────┴─────────────┴────────────────┘
```

### Use lazy scanning for large queries

For analytical queries across the full dataset, use `scan()` to get a Polars LazyFrame:

```python
import polars as pl

# Scan without loading everything into memory
lf = nemweb.DISPATCHREGIONSUM.scan()

# Filter and aggregate lazily
result = (
    lf.filter(pl.col("REGIONID") == "NSW1")
    .select("SETTLEMENTDATE", "TOTALDEMAND")
    .sort("SETTLEMENTDATE")
    .collect()
)
print(result.head())
```

### Query unit dispatch data

```python
# Get all generator dispatch at a specific interval
dispatch = nemweb.DISPATCHLOAD.get_data("2024/01/15 12:00:00")
print(dispatch.select("DUID", "TOTALCLEARED", "AVAILABILITY").head(10))
```

### Query unit details

The `DUDETAILSUMMARY` table uses start/end date filtering:

```python
# Get unit details valid on a given date
units = nemweb.DUDETAILSUMMARY.get_data("2024/01/15")
print(units.select("DUID", "REGIONID", "DISPATCHTYPE", "SCHEDULE_TYPE").head(10))
```

## 4. Add more data incrementally

You don't have to re-download everything. Add a new month:

```python
nemweb.DISPATCHREGIONSUM.add_data(year=2024, month=2)
```

Or populate a date range (existing months are skipped):

```python
nemweb.populate(slice("2024-01-01", "2024-06-30"))
```

To force re-download (overwrites existing data):

```python
nemweb.populate(slice("2024-01-01", "2024-01-31"), force_new=True)
```

## Next steps

- [Fetch NEMWEB data](../how-to/fetch-nemweb-data.md) -- detailed guide on all available tables and query patterns
- [Build a network model](build-network-model.md) -- use GIS data to create a pandapower model
- [Configuration reference](../reference/configuration.md) -- cache directories, cloud storage, environment variables
