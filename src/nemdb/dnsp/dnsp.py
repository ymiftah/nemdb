from nemdb.dnsp import (
    ausgrid,
    essential_energy,
    energex,
    ergon,
    endeavour,
    jemena,
    tasnetworks,
    sapn,
)
from nemdb.utils import download_file
from nemdb import log
import tempfile
import os
from contextlib import suppress


from typing import Any

from pathlib import Path
from tqdm import tqdm
import pyarrow.dataset as ds
import pandas as pd
import polars as pl


def mktempdir():
    dir_name = os.path.join(tempfile.gettempdir(), "nemdb")
    os.makedirs(dir_name, exist_ok=True)
    return dir_name


def read_all_zss(year: int):
    modules = {
        "ausgrid": ausgrid,
        "essential_energy": essential_energy,
        "energex": energex,
        "ergon": ergon,
        "endeavour": endeavour,
        "jemena": jemena,
        "tasnetworks": tasnetworks,
        "sapn": sapn,
    }
    for name, module in modules.items():
        log.info("Downloading Zone Substation loads from %s for year", name)
        try:
            url = module.get_url(year)
        except NotImplementedError as exc:
            log.error(exc)
            continue
        if url is None:
            raise Exception(f"Network {name} does not have a url for year {year}")
        tempdir = mktempdir()
        path = os.path.join(tempdir, f"{name}-Zone-Substation-Load-Data-{year}")
        try:
            download_file(url, path)
            log.info("Reading Zone Substation loads from %s for year", name)
            df = module.read_all_zss(path)
            yield name, df
        except Exception:
            log.error("Error downloading Zone Substation loads from %s for year", name)
            continue


class DNSPDataSource:
    def __init__(
        self,
        source,
        table_name,
        table_columns,
        table_primary_keys=None,
        add_partitions=None,
        low_memory=False,
    ):
        """
        Creates a parquet dataset
        """
        self.source = source
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        self.partitions = add_partitions + ["year"] if add_partitions else ["year"]
        self.low_memory = False

        self.path = Path(source) / table_name
        self.path.mkdir(exist_ok=True)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.ds, name)

    def scan(self, *args, **kwargs):
        kwargs_ = {"hive_partitioning": True, "allow_missing_columns": True}
        kwargs_.update(kwargs if kwargs is None else {})
        return pl.scan_parquet(self.ds.files, *args, **kwargs_)

    def read(self, *args, **kwargs):
        return pl.read_parquet(self.ds.files, *args, **kwargs)

    @property
    def ds(self):
        return ds.dataset(self.path, format="parquet", partitioning="hive")

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
