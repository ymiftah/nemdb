from nemdb.dnsp import (
    ausgrid,
    cppal,
    essential_energy,
    energex,
    ergon,
    endeavour,
    jemena,
    tasnetworks,
    sapn,
    united_energy,
)
from nemdb import log
from contextlib import suppress


import fsspec
from tqdm import tqdm
import pandas as pd
import polars as pl


def read_all_zss(year: int):
    modules = {
        "ausgrid": ausgrid,
        "cppal": cppal,
        "endeavour": endeavour,
        "energex": energex,
        "ergon": ergon,
        "essential_energy": essential_energy,
        "jemena": jemena,
        "sapn": sapn,
        "tasnetworks": tasnetworks,
        "united_energy": united_energy,
    }
    for name, module in modules.items():
        try:
            df = module.read_all_zss(year)
            yield name, df
        except Exception:
            log.error("Error downloading Zone Substation loads from %s for year", name)
            continue


class DNSPDataSource:
    def __init__(
        self,
        config,
        table_name,
        table_columns,
        table_primary_keys=None,
        add_partitions=None,
        low_memory=False,
    ):
        """
        Creates a parquet dataset
        """
        self.config = config
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        self.partitions = add_partitions + ["year"] if add_partitions else ["year"]
        self.low_memory = False

        self.path = f"{config.CACHE_DIR}/{table_name}"
        self.fs = fsspec.filesystem(config.FILESYSTEM)
        self.fs.makedirs(f"{config.CACHE_DIR}/{table_name}", exist_ok=True)

    def scan(self, *args, **kwargs):
        """scans the parquet dataset with polars"""
        kwargs_ = {"hive_partitioning": True, "allow_missing_columns": True}
        kwargs_.update(kwargs if kwargs is None else {})
        return pl.scan_parquet(self.path, *args, **kwargs_)

    def read(self, *args, **kwargs):
        """Reads the parquet dataset with polars

        NOTE provided for compatibility with nempy, should be avoided as it can read massive data
        in memory.
        """
        return self.scan(self, *args, **kwargs).collect()

    def add_data(self, year, month, **kwargs):
        name = self.table_name
        partition_cols = self.partitions
        for network, df in read_all_zss(year):
            data = df.with_columns(
                pl.lit(network, pl.String).alias("network"),
                pl.lit(year, pl.Int32).alias("year"),
            ).sort(partition_cols + self.table_primary_keys)

            log.debug(
                "Writing data for %s - %s, at location %s",
                self.table_name,
                year,
                f"{name}-{{i}}.parquet",
            )
            data.write_parquet(
                self.path,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": partition_cols,
                    "existing_data_behavior": "overwrite_or_ignore",
                    "basename_template": f"{name}-{{i}}.parquet",
                },
                **kwargs,
            )

    def populate(self, date_slice: slice, force_new: bool = False):
        date_range = pd.date_range(
            start=date_slice.start, end=date_slice.stop, freq="MS"
        )
        log.info(
            "Populating database with data from %s to %s", date_range[0], date_range[-1]
        )
        years = date_range.year.unique()
        for year in tqdm(years):
            # Check if data already exists in tables before adding
            data_exists = False
            if not force_new:
                with suppress(pl.exceptions.ComputeError, FileNotFoundError, Exception):
                    log.info(
                        "Checking if data already exists for %s %s",
                        self.table_name,
                        year,
                    )
                    check = self.scan().filter(pl.col("year") == year).head()
                    data_exists = len(check.collect()) > 0
            if not data_exists:
                self.add_data(year=year, month=None)
            else:
                log.info(
                    "Data already exists for %s %s, skipping download. Use force_new=True to overwrite.",
                    self.table_name,
                    year,
                )


if __name__ == "__main__":
    for df in read_all_zss(2024):
        pass
