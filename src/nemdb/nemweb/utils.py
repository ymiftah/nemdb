import requests
import os
import functools
import polars as pl

from time import sleep

from nemdb import Config
from nemdb.logger import log as logger


def cache_response_zip(url):
    base_name = os.path.basename(url)
    path = os.path.join(Config.CACHE_DIR, base_name)
    if os.path.exists(path):
        logger.info("reading from cache: %s", path)
        return path
    logger.info("Requesting file form %s", url)
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to download {url}")
    logger.info("Writing response to cache: %s", path)

    with open(path, "wb") as f:
        f.write(response.content)

    return path


def cache_to_parquet(file_path):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if os.path.exists(file_path):
                return pl.read_parquet(file_path)
            else:
                result = func(*args, **kwargs)
                if not isinstance(result, pl.DataFrame):
                    result = pl.from_pandas(result)
                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))
                result.write_parquet(file_path)
                return result

        return wrapper

    return decorator


# Retry decorator
def retry(tries: int, delay=3, return_on_failure=None):
    """Retries a function or method until it returns True.

    delay sets the initial delay in seconds. tries must be at least 0, and delay
    greater than 0.
    """

    tries = round(tries)
    if tries < 0:
        raise ValueError("tries must be 0 or greater")

    if delay <= 0:
        raise ValueError("delay must be greater than 0")

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay  # make mutable

            while mtries > 0:
                try:
                    return f(*args, **kwargs)  # first attempt
                except Exception:
                    mtries -= 1  # consume an attempt
                    sleep(mdelay)  # wait...

                    return f(*args, **kwargs)  # Try again

            return return_on_failure  # Ran out of tries :-(

        return f_retry  # true decorator -> decorated function

    return deco_retry  # @retry(arg[, ...]) -> true decorator
