import zipfile

import pandera as pa
import polars as pl

from nemdb.dnsp.common import LoadSchema
from nemdb.utils import download_file_to_bytesio


def get_url(year: int):
    return {
        2024: "https://www.ausgrid.com.au/-/media/Documents/Data-to-share/Distribution-zone-substation-informaton/FY2024.zip?rev=6fc7bcc1b5464355b0370de40aae283d"
    }.get(year)


@pa.check_output(LoadSchema)
def _read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for filename in zip_ref.namelist():
            with zip_ref.open(filename) as f:
                df = (
                    (
                        pl.read_csv(f)
                        .rename({"Zone Substation": "zss", "Date": "date", "Unit": "metric"})
                        .drop("Year")
                        .filter(pl.col("metric") == "MW")
                        .unpivot(
                            index=["zss", "date", "metric"],
                            value_name="value",
                            variable_name="time",
                        )
                        .with_columns(
                            (pl.col("date") + " " + pl.col("time").str.replace("24:", "00:"))
                            .str.to_datetime("%Y-%m-%d %H:%M")
                            .alias("time"),
                            pl.col("metric").str.to_lowercase(),
                        )
                        .select(["zss", "time", "metric", "value"])
                    )
                    .cast(
                        {
                            "value": pl.Float32,
                        }
                    )
                    .pivot(
                        index=["zss", "time"],
                        on="metric",
                        values="value",
                    )
                )
                dfs.append(df)
    return pl.concat(dfs)


def read_all_zss(year: int):
    """
    Reads a zip file containing csvs of load data for each zone substation (ZSS)

    Parameters
    ----------
    year : int
        The year for which to download the data

    Returns
    -------
    pl.DataFrame
        A dataframe with columns "zss", "time", "mw" and "mva"
    """
    file = download_file_to_bytesio(get_url(year))
    loads = _read_all_zss(file)
    return loads


if __name__ == "__main__":
    df = read_all_zss(2024)
    print(df)
