""" "Download from
"https://www.essentialenergy.com.au/ext/schools/EE-Zone-Substation-Load-Data-2023-24.zip"
"""

import polars as pl
import pandas as pd


import requests

import pandera as pa
from nemdb.dnsp.common import LoadSchema
from nemdb.utils import download_file_to_bytesio


def read_all_zss(year: int):
    file = download_file_to_bytesio(get_url(year))
    loads = _read_all_zss(file)
    return loads


def get_url(year: int):
    """
    Download from
    "https://www.essentialenergy.com.au/ext/schools/EE-Zone-Substation-Load-Data-2023-24.zip"
    """
    raise NotImplementedError(
        """
        Essential energy protects from webscraping and the download fails. You can download the data manually from their website
        "https://www.essentialenergy.com.au/ext/schools/EE-Zone-Substation-Load-Data-2023-24.zip"
        and run the processing method manually
        """
    )
    if year != 2024:
        return None

    with requests.Session() as s:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
            # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            # "Accept-Encoding": "gzip, deflate",
            # "Accept-Language": "en-GB,en-US;q=0.8,en;q=0.6",
            # "Connection": "keep-alive",
            # "DNT": "1",
            # "Host": "www.essentialenergy.com.au",
            # "Referer": ,
            # "Upgrade-Insecure-Requests": "1",
            # "Cookie": "JSESSIONID=5F0C8AF27E4C1E0EDEE55F7B9E8AA5C4; _ga=GA1.3.1433753942.1593115043; _gid=GA1.3.1353433574.1593115043; _gat_UA-1053426-4=1",
        }
        resp1 = requests.get(
            "https://www.essentialenergy.com.au/our-network/network-projects/zone-substation-reports",
            headers=headers,
        )
        r = s.get(
            "https://www.essentialenergy.com.au/ext/schools/EE-Zone-Substation-Load-Data-2023-24.zip",
            stream=True,
            headers=headers,
            cookies=resp1.cookies,
        )
        return r.content


@pa.check_output(LoadSchema)
def _read_all_zss(file):
    """
    Read a zip file containing csvs of load data for each zone substation (ZSS)

    Parameters
    ----------
    file : str
        Path to the zip file

    Returns
    -------
    pl.DataFrame
        A dataframe with columns "zss", "time", and "MW"
    """
    return (
        pl.from_pandas(pd.read_csv(file, usecols=["Name", "IntervalEnd", "kW", "kVAr"]))
        .lazy()
        .with_columns(
            pl.col("kW").cast(pl.Float32).truediv(1000).alias("mw").cast(pl.Float32),
            pl.col("kVAr")
            .cast(pl.Float32)
            .truediv(1000)
            .alias("mvar")
            .cast(pl.Float32),
            pl.col("IntervalEnd")
            .str.to_datetime("%Y-%m-%dT%H:%M:%S.000Z")
            .alias("time"),
        )
        .rename({"Name": "zss"})
        .select(["zss", "time", "mw", "mvar"])
        .collect()
    )


if __name__ == "__main__":
    # content = get_url(2024)
    # print(content)
    file = "/home/simba/Downloads/EE-Zone-Substation-Load-Data-2023-24.zip"
    # file = download_file(
    #     get_url(2024),
    #     file,
    # )
    df = read_all_zss(file)
    print(df)
