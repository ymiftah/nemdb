import datetime

import polars as pl
from joblib import Memory
from openelectricity import AsyncOEClient, UnitFueltechType, UnitStatusType
from openelectricity.types import (
    DataInterval,
    DataMetric,
    DataPrimaryGrouping,
    DataSecondaryGrouping,
    NetworkCode,
)

from nemdb.config import Config

memory = Memory(location=Config.TEMP_DIR, verbose=1)


@memory.cache
async def read_facilities(
    network_id: list[str] | None = None,
    status_id: list[UnitStatusType] | None = None,
    fueltech_id: list[UnitFueltechType] | None = None,
) -> pl.DataFrame:
    async with AsyncOEClient() as client:
        # Make async API calls
        response = await client.get_facilities(
            network_id=network_id,
            status_id=status_id,
            fueltech_id=fueltech_id,
        )
    df = pl.concat(
        pl.DataFrame(facility.units).with_columns(
            pl.lit(facility.code).alias("facility_code"),
            pl.lit(facility.name).alias("name"),
            pl.lit(facility.network_id).alias("network_id"),
            pl.lit(facility.location.lat if facility.location else None).alias("latitude"),
            pl.lit(facility.location.lng if facility.location else None).alias("longitude"),
        )
        for facility in response.data
    )
    return df


async def read_data(
    metric: DataMetric | str,
    network_code: NetworkCode = "NEM",
    interval: DataInterval = "1h",
    date_start: datetime.datetime | None = None,
    date_end: datetime.datetime | None = None,
    primary_grouping: DataPrimaryGrouping = "network_region",
    secondary_grouping: DataSecondaryGrouping = "fueltech",
) -> pl.DataFrame:
    if isinstance(metric, str):
        metric = DataMetric(metric)
    if date_start is None and date_end is None:
        today = datetime.datetime.today()
        date_start = today - datetime.timedelta(days=2)
        date_end = today
    response = await _fetch_api(
        metric=metric,
        network_code=network_code,
        interval=interval,
        date_start=date_start,
        date_end=date_end,
        primary_grouping=primary_grouping,
        secondary_grouping=secondary_grouping,
    )
    return _response_to_df(response)


@memory.cache
async def _fetch_api(
    metric: DataMetric,
    network_code: NetworkCode,
    interval: DataInterval,
    date_start: datetime.datetime,
    date_end: datetime.datetime,
    primary_grouping: DataPrimaryGrouping = "network_region",
    secondary_grouping: DataSecondaryGrouping = "fueltech",
):
    async with AsyncOEClient() as client:
        # Make async API calls
        response = await client.get_network_data(
            network_code=network_code,
            metrics=[metric],
            interval=interval,
            date_start=date_start,
            date_end=date_end,
            primary_grouping=primary_grouping,
            secondary_grouping=secondary_grouping,
        )
    return response


def _response_to_df(response):
    data = []
    for timeseries in response.data:
        for result in timeseries.results:
            for data_point in result.data:
                data.append(
                    {
                        "timestamp": data_point.timestamp,
                        "metric": timeseries.metric,
                        "value": data_point.value,
                        "unit": timeseries.unit,
                        "type": result.name,
                    }
                )
    # Same data structure as above
    return (
        pl.DataFrame(data)
        .with_columns(
            pl.col("type")
            .str.splitn("|", 2)
            .struct.rename_fields(["region", "tech"])
            .struct.unnest()
        )
        .with_columns(
            pl.col("region")
            .str.splitn("_", 2)
            .struct.rename_fields(["metric", "region"])
            .struct.unnest()
        )
        .drop("type")
    )
