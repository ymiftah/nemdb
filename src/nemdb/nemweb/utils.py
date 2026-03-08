from pathlib import Path
from time import sleep

import requests

from nemdb.config import config
from nemdb.logger import log as logger


# Retry decorator
def retry(tries: int, delay: int = 3, return_on_failure=None):
    """Retries a function or method until it returns True.

    This decorator retries a function a specified number of times with a delay
    between each attempt.

    Args:
        tries (int): The number of times to retry the function.
        delay (int, optional): The delay in seconds between retries.
            Defaults to 3.
        return_on_failure (any, optional): The value to return if the
            function fails after all retries. Defaults to None.
    """

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


@retry(3)
def cache_response_zip(url: str) -> str:
    """Write in cache the file from the url and return the path to the file.

    This function downloads a file from a URL, saves it to a temporary directory,
    and returns the path to the saved file. If the file already exists in the
    cache, it returns the path without downloading it again.

    Args:
        url (str): The URL of the file to download.

    Returns:
        str: The path to the cached file.

    Raises:
        ValueError: If the file fails to download.
    """
    base_name = Path(url).name
    path = Path(config.temp_dir) / base_name
    if path.exists():
        logger.info("reading from cache: %s", path)
        return str(path)
    logger.info("Requesting file from %s", url)
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to download {url}")
    logger.info("Writing response to cache: %s", path)

    path.write_bytes(response.content)

    return str(path)
