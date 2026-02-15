# Use DNSP data

nemdb collects zone substation (ZSS) load data from distribution network service providers across the NEM.

## Available DNSPs

| DNSP | State | Module |
|------|-------|--------|
| Ausgrid | NSW | `nemdb.dnsp.ausgrid` |
| Endeavour Energy | NSW | `nemdb.dnsp.endeavour` |
| Essential Energy | NSW | `nemdb.dnsp.essential_energy` |
| CitiPower & Powercor | VIC | `nemdb.dnsp.cppal` |
| Jemena | VIC | `nemdb.dnsp.jemena` |
| United Energy | VIC | `nemdb.dnsp.united_energy` |
| Energex | QLD | `nemdb.dnsp.energex` |
| Ergon Energy | QLD | `nemdb.dnsp.ergon` |
| SA Power Networks | SA | `nemdb.dnsp.sapn` |
| TasNetworks | TAS | `nemdb.dnsp.tasnetworks` |

## Download ZSS data

### Via NEMWEBManager

ZSS data is integrated into `NEMWEBManager` as the `ZONE_SUBSTATION` table:

```python
from nemdb import NEMWEBManager, Config

Config.set_cache_dir("./data")
nemweb = NEMWEBManager(Config)

# Download ZSS data for a date range
nemweb.ZONE_SUBSTATION.populate(slice("2024-01-01", "2024-12-31"))

# Scan as a LazyFrame
lf = nemweb.ZONE_SUBSTATION.scan()
```

### Directly from a DNSP module

```python
from nemdb.dnsp import ausgrid

df = ausgrid.read_all_zss(2024)
print(df.head())
```

Each DNSP module provides:

- `get_url(year)` -- returns the download URL for a given year (or `None` if unavailable)
- `read_all_zss(year)` -- downloads, parses, and returns a Polars DataFrame

### Read all DNSPs at once

```python
from nemdb.dnsp.dnsp import read_all_zss

for network_name, df in read_all_zss(2024):
    print(f"{network_name}: {len(df)} rows")
```

## Data schema

All DNSP data is normalised to a common schema:

| Column | Type | Description |
|--------|------|-------------|
| `zss` | String | Zone substation name |
| `time` | Datetime | Timestamp (typically half-hourly) |
| `MW` | Float | Load in megawatts |

When loaded via `NEMWEBManager`, the data is partitioned by `network` (DNSP name) and `year`.

## Data availability

Data availability varies by DNSP and year. Not all DNSPs publish data for all years. If a DNSP's data is unavailable for a requested year, `get_url()` returns `None` and the download is skipped with a log message.
