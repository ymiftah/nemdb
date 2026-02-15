# Use the OpenNEM API

nemdb integrates with the [OpenElectricity](https://openelectricity.org.au/) API (formerly OpenNEM) for facility metadata and time-series data.

## Read facility data

```python
import asyncio
from nemdb.opennem.opennemapi import read_facilities

# All NEM facilities
facilities = asyncio.run(read_facilities(network_id=["NEM"]))
print(facilities.columns)
print(f"Total units: {len(facilities)}")
```

Filter by status or fuel technology:

```python
from openelectricity import UnitStatusType, UnitFueltechType

# Only operating wind farms
wind = asyncio.run(
    read_facilities(
        network_id=["NEM"],
        status_id=[UnitStatusType.OPERATING],
        fueltech_id=[UnitFueltechType.WIND],
    )
)
print(wind.select("facility_code", "name", "capacity_registered").head())
```

Results are cached locally using joblib so repeated calls are fast.

## Read time-series data

```python
import asyncio
import datetime
from nemdb.opennem.opennemapi import read_data

df = asyncio.run(
    read_data(
        metric="power",
        network_code="NEM",
        interval="1h",
        date_start=datetime.datetime(2024, 1, 1),
        date_end=datetime.datetime(2024, 1, 7),
        primary_grouping="network_region",
        secondary_grouping="fueltech",
    )
)
print(df.head())
```

Output columns: `timestamp`, `metric`, `value`, `unit`, `region`, `tech`.

### Available metrics

Pass any `DataMetric` value as `metric`:

- `"power"` -- generation/consumption power (MW)
- `"energy"` -- energy (MWh)
- `"emissions"` -- CO2 emissions
- `"market_value"` -- market value ($)

### Available groupings

- **Primary**: `"network_region"`, `"network"`, `"facility"`
- **Secondary**: `"fueltech"`, `"status"`, `"unit"`

### Default date range

If `date_start` and `date_end` are both `None`, the API returns data for the last 2 days.

## API caching

Both `read_facilities` and `read_data` use joblib caching stored in the temp directory (`Config.TEMP_DIR`). To clear the cache:

```python
from nemdb.opennem.opennemapi import memory
memory.clear()
```
