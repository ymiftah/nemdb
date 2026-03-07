# Fetch NEMWEB data

## Download data with the CLI

```bash
# All active tables for a date range
uv run populate --location ./data --date_range 2024-01-01->2024-06-30

# A specific table only
uv run populate --location ./data --date_range 2024-01-01->2024-06-30 --table DISPATCHREGIONSUM

# Force re-download (overwrite existing)
uv run populate --location ./data --date_range 2024-01-01->2024-01-31 --force_new
```

CLI flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--location` | `~/.nemdb_cache` | Directory to store Parquet files |
| `--filesystem` | `file` | Storage backend (`file`, `gcs`, etc.) |
| `--date_range` | *(required)* | Date range as `YYYY-MM-DD->YYYY-MM-DD` |
| `--table` | `all` | Table name or `all` |
| `--force_new` | `False` | Re-download existing data |

## Download data from Python

```python
from nemdb import NEMWEBManager, Config

Config.set_cache_dir("./data")
nemweb = NEMWEBManager(Config)

# Populate all active tables
nemweb.populate(slice("2024-01-01", "2024-06-30"))

# Populate a single table
nemweb.DISPATCHREGIONSUM.populate(slice("2024-01-01", "2024-06-30"))

# Add a single month
nemweb.DISPATCHREGIONSUM.add_data(year=2024, month=1)
```

## Available tables

### Tables populated by default

These tables are downloaded when running `populate` with `table=all`:

| Table | Filter type | Description |
|-------|-------------|-------------|
| `DISPATCHREGIONSUM` | Settlement date | Regional demand, supply, and availability by 5-min interval |
| `DISPATCHLOAD` | Settlement date | Unit-level dispatch (MW cleared, ramp rates, FCAS) |
| `DISPATCHPRICE` | Settlement date | Regional reference prices and FCAS prices |
| `BIDPEROFFER_D` | Settlement date | Unit volume bids by 5-min interval (large table) |
| `BIDDAYOFFER_D` | Settlement date | Unit price bids by market day |
| `DUDETAILSUMMARY` | Start/end date | Unit metadata (region, connection point, loss factors) |
| `DUDETAIL` | Effective date | Detailed unit parameters (capacity, ramp rates, storage) |
| `DUALLOC` | None | DUID to GENSETID mapping |
| `GENUNITS` | None | Generator set information (voltage, fuel, emissions) |
| `STATION` | None | Station name and address |
| `STATIONOPERATINGSTATUS` | None | Station operating status |
| `MNSP_INTERCONNECTOR` | Effective date | Market network service provider interconnectors |

### Additional tables (not populated by default)

| Table | Filter type | Description |
|-------|-------------|-------------|
| `DISPATCHCONSTRAINT` | Settlement date | Generic constraints used in dispatch |
| `GENCONDATA` | Effective date | Generic constraint definitions |
| `SPDREGIONCONSTRAINT` | Effective date | Regional constraint LHS terms |
| `SPDCONNECTIONPOINTCONSTRAINT` | Effective date | Connection point constraint LHS terms |
| `SPDINTERCONNECTORCONSTRAINT` | Effective date | Interconnector constraint LHS terms |
| `INTERCONNECTOR` | Effective date | Interconnector region links |
| `INTERCONNECTORCONSTRAINT` | Effective date | Interconnector loss parameters |
| `LOSSMODEL` | Effective date | Interconnector loss model breakpoints |
| `LOSSFACTORMODEL` | Effective date | Loss factor demand coefficients |
| `DISPATCHINTERCONNECTORRES` | Settlement date | Interconnector dispatch results |
| `RESERVE` | Settlement date | FCAS reserve requirements |
| `ZONE_SUBSTATION` | Year | DNSP zone substation loads |

## Query patterns

### By settlement date (5-minute intervals)

```python
# Returns data for a single 5-minute interval
df = nemweb.DISPATCHREGIONSUM.get_data("2024/01/15 12:00:00")
```

### By start/end date range

```python
# Returns data where start_date <= date <= end_date
df = nemweb.DUDETAILSUMMARY.get_data("2024/01/15")
```

### By effective date and version

```python
# Returns latest version of data effective on or before the date
df = nemweb.GENCONDATA.get_data("2024/01/15")
```

### Lazy scanning (recommended for analytics)

```python
import polars as pl

# Create a lazy query
lf = nemweb.DISPATCHREGIONSUM.scan()

# Filter, aggregate, then collect
result = (
    lf.filter(
        (pl.col("REGIONID") == "NSW1")
        & (pl.col("SETTLEMENTDATE") >= pl.datetime(2024, 1, 1))
        & (pl.col("SETTLEMENTDATE") < pl.datetime(2024, 2, 1))
    )
    .group_by(pl.col("SETTLEMENTDATE").dt.date())
    .agg(pl.col("TOTALDEMAND").mean())
    .sort("SETTLEMENTDATE")
    .collect()
)
```

### Read bid data

Bid data is handled separately due to its size:

```python
# Price and volume bids for a specific day
price_bids = nemweb.get_unit_price_bids("2024/01/15 12:00:00")
volume_bids = nemweb.get_unit_volume_bids("2024/01/15 12:00:00")
```

## Use cloud storage

nemdb supports any [fsspec](https://filesystem-spec.readthedocs.io/)-compatible backend:

```python
from nemdb import Config

# Google Cloud Storage
Config.set_cache_dir("gs://my-bucket/nemweb")
Config.set_filesystem("gcs")
```

```bash
# Via CLI
uv run populate --location gs://my-bucket/nemweb --filesystem gcs --date_range 2024-01-01->2024-01-31
```
