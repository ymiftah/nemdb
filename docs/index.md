# nemdb

**nemdb** is a Python package for working with data from Australia's
[National Electricity Market](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem)
(NEM). It provides tools for fetching, storing, and analysing dispatch data,
geographic infrastructure data, and network modelling.

## Key capabilities

- **NEMWEB data management** -- Download and cache historical dispatch data
  from AEMO's NEMWEB portal as efficient Parquet files. Query 30+ MMSDM
  tables by settlement date, interval, or effective date.
- **Geographic data** -- Fetch substation locations, transmission line
  geometries, and power station locations from Geoscience Australia. Includes
  a cleaning pipeline that fixes topology errors in line data.
- **Network modelling** -- Build pandapower network models directly from GIS
  data, with automatic bus clustering, transformer inference, and
  connectivity validation.
- **DNSP zone substation loads** -- Collect half-hourly zone substation load
  data from 10 distribution network service providers across the NEM.
- **ISP assumptions** -- Read AEMO's Integrated System Plan assumption
  workbooks as Polars DataFrames.
- **OpenNEM integration** -- Query the OpenElectricity API for facility
  metadata and time-series generation/demand data.
- **Visualisation** -- Interactive Plotly maps of the transmission network,
  colour-coded by voltage and fuel type.

## Quick start

### Installation

```bash
git clone https://github.com/ymiftah/nemdb.git
cd nemdb
uv pip install -e .
```

For network modelling and visualisation, install the optional dependency groups:

```bash
uv pip install -e ".[grid,viz]"
```

### Fetch your first data

```bash
uv run populate --location ./data --date_range 2024-01-01->2024-01-31
```

Or from Python:

```python
from nemdb import NEMWEBManager, Config

Config.set_cache_dir("./data")
nemweb = NEMWEBManager(Config)
nemweb.populate(slice("2024-01-01", "2024-01-31"))

# Query a specific dispatch interval
df = nemweb.DISPATCHREGIONSUM.get_data("2024/01/15 12:00:00")
print(df)
```

## Documentation

This documentation follows the [Diataxis](https://diataxis.fr/) framework:

| Section | Purpose |
|---------|---------|
| [Tutorials](tutorials/getting-started.md) | Step-by-step learning exercises |
| [How-to guides](how-to/fetch-nemweb-data.md) | Task-oriented recipes |
| [Explanation](explanation/transmission-line-cleaning.md) | Background and design |
| [Reference](reference/configuration.md) | Technical specifications |
| [API](api/) | Auto-generated API reference |

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended)

## License

MIT. Data is subject to AEMO's
[Privacy and Legal Notice](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb).
