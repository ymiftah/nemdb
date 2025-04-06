import zipfile
import polars as pl


import pandera as pa
from nemdb.dnsp.common import LoadSchema
from nemdb.utils import download_file_to_bytesio


def read_all_zss(year: int):
    file = download_file_to_bytesio(get_url(year))
    loads = _read_all_zss(file)
    return loads


def get_url(year: int):
    return {
        2024: "https://www.ergon.com.au/__data/assets/file/0007/1385755/Ergon-Energy-Network-Zone-Substation-Data-2023-24.zip"
    }.get(year, None)


@pa.check_output(LoadSchema)
def _read_all_zss(file):
    """
    Reads a zip file of zone substation load data from Ergon Energy into a polars dataframe.

    Parameters
    ----------
    file : str
        Path to the zip file

    Returns
    -------
    pl.DataFrame
        A dataframe with columns "zss", "time", "mw" and "mva"
    """
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for f in zip_ref.namelist():
            zss_name = f.split("_EECL_")[0]
            df = (
                pl.read_csv(
                    zip_ref.open(f),
                    columns=["Date", "Time", "MW", "MVA"],
                )
                .with_columns(
                    pl.lit(zss_name).alias("zss"),
                    (pl.col("Date") + " " + pl.col("Time"))
                    .str.to_datetime("%Y-%m-%d %H:%M:%S")
                    .alias("time"),
                    pl.col("MW").cast(float),
                )
                .select(["zss", "time", "MW", "MVA"])
                .cast(
                    {
                        "MW": pl.Float32,
                        "MVA": pl.Float32,
                    }
                )
                .rename({"MW": "mw", "MVA": "mva"})
            )
            dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    path = "/home/simba/Downloads/Ergon-Network-Substation-Load-Data-2023-24.zip"
    df = read_all_zss(path)
    print(df)
