from functools import lru_cache
import requests
import tempfile
import zipfile
from io import BytesIO

from bs4 import BeautifulSoup
import tqdm

import polars as pl
import pandas as pd

from .utils import retry, cache_response_zip

NEMWEB_ARCHIVE = "https://nemweb.com.au/Reports/Archive/"
MMSDM = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{data}"
BIDMOVE = "https://nemweb.com.au/Reports/Current/Bidmove_Complete/"


def read_bids(year, month, day):
    """Returns price and volume bids for the given day."""
    file = "PUBLIC_BIDMOVE_COMPLETE_{year}{month:02d}{day:02d}".format(
        year=year, month=month, day=day
    )
    files = __read_files_available(BIDMOVE, format=".zip")
    file = [f for f in files if file in f][0]
    file = cache_response_zip(file)
    with zipfile.ZipFile(file, "r") as z, z.open(z.namelist()[0]) as f:
        first = True
        dfs = []
        out = BytesIO()
        for line in f:
            line_ = line.decode("utf-8")
            if not (line_.startswith("D") or line_.startswith("I")):
                continue
            if first and line_.startswith("I"):
                first = False
            elif line_.startswith("I"):
                # continue
                out.seek(0)
                dfs.append(pl.read_csv(out))
                out.truncate(0)
            out.write(line)  # assuming the file is comma-separated

        out.seek(0)
        dfs.append(pl.read_csv(out))
        return dfs[0], dfs[1]


def read_genunits(year: int, month: int) -> pl.DataFrame:
    data = f"GENUNITS_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


def read_bidperoffer_d(year: int, month: int) -> pl.DataFrame:
    data = f"BIDPEROFFER_D_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


def read_dispatchprice(year: int, month: int) -> pl.DataFrame:
    data = f"DISPATCHPRICE_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


def read_dispatchload(year: int, month: int) -> pl.DataFrame:
    data = f"DISPATCHLOAD_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(cache_response_zip(url), compression="zip", skiprows=1, nrows=1000)
    ).filter(pl.col("I") == "D")
    return df


def read_station(year: int, month: int) -> pl.DataFrame:
    data = f"STATION_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


def read_dudetailsummary(year: int, month: int) -> pl.DataFrame:
    data = f"DUDETAILSUMMARY_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("END_DATE").str.contains("2999/") & (pl.col("I") == "D"))
    return df


def read_bidduiddetails(year: int, month: int) -> pl.DataFrame:
    data = f"BIDDUIDDETAILS_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


# def read_biddayoffer(year: int, month: int) -> pl.DataFrame:
#     data = f"BIDDAYOFFER_{year}{month:02d}010000.zip"
#     url = MMSDM.format(year=year, month=month, data=data)
#     df = pl.from_pandas(
#         pd.read_csv(
#             url,
#             skiprows=1,
#         )
#     ).filter(pl.col("I") == "D")
#     return df


def read_dudetails(year: int, month: int) -> pl.DataFrame:
    data = f"DUDETAIL_{year}{month:02d}010000.zip"
    url = MMSDM.format(year=year, month=month, data=data)
    print(url)
    df = pl.from_pandas(
        pd.read_csv(
            cache_response_zip(url),
            compression="zip",
            skiprows=1,
        )
    ).filter(pl.col("I") == "D")
    return df


def read_archived_rooftop_pv() -> pl.DataFrame:
    url = "http://www.nemweb.com.au/REPORTS/ARCHIVE/HistDemand"
    files = __read_files_available(url)
    dfs = []
    # Download the zip file
    for f in tqdm.tqdm(files):
        response = requests.get(f)
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        # For each inner zip file, read all CSV files
        # Extract all inner zip files
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file.extractall(temp_dir)
            for inner_zip_name in zip_file.namelist():
                dfs.append(
                    __process_pv(
                        pd.read_csv(f"{temp_dir}/{inner_zip_name}", skiprows=1)
                    )
                )
    return pl.concat(dfs, how="diagonal")


def read_archived_demand_actuals() -> pl.DataFrame:
    url = "http://www.nemweb.com.au/REPORTS/ARCHIVE/HistDemand"
    files = __read_files_available(url)
    dfs = []
    # Download the zip file
    for f in tqdm.tqdm(files):
        response = requests.get(f)
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        # For each inner zip file, read all CSV files
        # Extract all inner zip files
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file.extractall(temp_dir)
            for inner_zip_name in zip_file.namelist():
                dfs.append(
                    __process_demand(
                        pd.read_csv(f"{temp_dir}/{inner_zip_name}", skiprows=1)
                    )
                )
    return pl.concat(dfs, how="diagonal")


def read_demand_actuals() -> pl.DataFrame:
    url = "http://www.nemweb.com.au/REPORTS/CURRENT/HistDemand"
    files = __read_files_available(url)
    # TODO maybe use dask, but careful with 403
    df = pd.concat(__fetch(f) for f in files)
    # process in polars
    df = __process_demand(df)
    return df


def read_demand_forecast(date: str = None) -> pl.DataFrame:
    """Reads the demand forecast for each region

    Args:
        date (str, optional): Day of forecast. Defaults to None.

    Returns:
        pl.DataFrame: _description_
    """
    url = "https://nemweb.com.au/Reports/Current/Operational_Demand/Forecast_HH/"
    files = __read_files_available(url)
    if date is None:
        files = files[-1:]  # open the last one
    else:
        files = [f for f in files if date in f]  # TODO proper regex
    # TODO maybe use dask, but careful with 403
    df = pd.concat(__fetch(f) for f in files)
    # process in polars
    df = (
        pl.from_pandas(df)
        .drop_nulls()
        .sort(
            ["LOAD_DATE", "INTERVAL_DATETIME", "REGIONID"],
            descending=[True, False, False],
        )
        .unique(["INTERVAL_DATETIME", "REGIONID"], keep="first")
        .group_by(["INTERVAL_DATETIME", "REGIONID"])
        .agg(
            pl.sum("OPERATIONAL_DEMAND_POE10"),
            pl.sum("OPERATIONAL_DEMAND_POE50"),
            pl.sum("OPERATIONAL_DEMAND_POE90"),
        )
        .with_columns(
            pl.col("INTERVAL_DATETIME").str.to_datetime("%Y/%m/%d %H:%M:%S"),
        )
        .sort("INTERVAL_DATETIME", "REGIONID")
    )
    return df


# Helpers
@lru_cache(maxsize=256)
@retry(tries=2, delay=1, return_on_failure=pd.DataFrame())
def __fetch(f):
    return pd.read_csv(f, skiprows=1)


def __read_files_available(url, format=".zip"):
    # TODO REGEX
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    files = [f"{url}/{tag.text}" for tag in soup.find_all("a") if format in tag.text]
    if len(files) == 0:
        raise ValueError("No files available for the selected url")
    return files


def __process_demand(df: pd.DataFrame) -> pl.DataFrame:
    return (
        pl.from_pandas(df)
        .drop_nulls()
        .group_by(["REGIONID", "SETTLEMENTDATE", "PERIODID"])
        .agg(pl.sum("DEMAND.1").alias("DEMAND"))
        .with_columns(
            pl.col("SETTLEMENTDATE").str.to_datetime("%Y/%m/%d %H:%M:%S"),
            (pl.col("PERIODID") * 30 * 60 * 1e9).cast(pl.Time, strict=False),
        )
        .with_columns(
            pl.col("SETTLEMENTDATE").dt.combine(pl.col("PERIODID")).alias("time")
        )
        .pivot(values="DEMAND", index="time", columns="REGIONID")
        .sort("time")
    )


def __process_pv(df: pd.DataFrame) -> pl.DataFrame:
    return (
        pl.from_pandas(df)
        .drop_nulls()
        .group_by(["REGIONID", "SETTLEMENTDATE", "PERIODID"])
        .agg(pl.sum("DEMAND.1").alias("DEMAND"))
        .with_columns(
            pl.col("SETTLEMENTDATE").str.to_datetime("%Y/%m/%d %H:%M:%S"),
            (pl.col("PERIODID") * 30 * 60 * 1e9).cast(pl.Time, strict=False),
        )
        .with_columns(
            pl.col("SETTLEMENTDATE").dt.combine(pl.col("PERIODID")).alias("time")
        )
        .pivot(values="DEMAND", index="time", columns="REGIONID")
        .sort("time")
    )
