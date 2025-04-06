import requests
from typing import Any
import functools
import polars as pl
import geopandas as gpd
import pandas as pd

from nemdb import log

import os

from io import BytesIO


def download_file(url, path, stream=True):
    """
    Downloads a file from a specified URL and saves it to a local path.

    Parameters
    ----------
    url : str
        The URL of the file to be downloaded.
    path : str
        The local file path where the downloaded file will be saved.

    Returns
    -------
    str
        The path where the file has been saved.
    """
    if os.path.exists(path):
        log.info("File already exists at %s", path)
        return path
    log.info("Downloading %s to %s", url, path)
    with requests.get(url, stream=stream) as r:
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
    log.info("Successfully downloaded %s to %s", url, path)
    return path


def download_file_to_bytesio(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    bytes_io = BytesIO()
    for chunk in response.iter_content(chunk_size=4096):
        bytes_io.write(chunk)

    bytes_io.seek(0)
    return bytes_io


def cache_to_parquet(file_path, *, type_: Any = pl.DataFrame):
    """Cache the decorated function into a parquet file. (function must return a dataframe)"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if os.path.exists(file_path):
                log.info("Reading from cache : %s", file_path)
                return _dispatch_read(file_path, type_)
            else:
                result = func(*args, **kwargs)
                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))
                _dispatch_write(result, file_path, type_)
                return result

        return wrapper

    return decorator


def _dispatch_read(file_path, type_):
    if type_ == pl.DataFrame:
        return pl.read_parquet(file_path)
    elif type_ == gpd.GeoDataFrame:
        return gpd.read_parquet(file_path)
    elif type_ == pd.DataFrame:
        return pd.read_parquet(file_path)


def _dispatch_write(df, file_path, type_):
    if type_ == pl.DataFrame:
        return df.write_parquet(file_path)
    elif type_ == gpd.GeoDataFrame:
        return df.to_parquet(file_path)
    elif type_ == pd.DataFrame:
        return df.to_parquet(file_path)
