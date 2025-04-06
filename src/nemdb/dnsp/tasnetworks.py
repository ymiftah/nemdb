import polars as pl
import pandas as pd


import pandera as pa
from nemdb.dnsp.common import LoadSchema
from nemdb.utils import download_file_to_bytesio


def read_all_zss(year: int):
    file = download_file_to_bytesio(get_url(year))
    loads = _read_all_zss(file)
    return loads


def get_url(year: int):
    return {
        2023: "https://www.tasnetworks.com.au/Documents/Manual-documents/Planning-and-upgrades/Substation-Load-Information/2023-Zone-Substation-Report",
        2024: "https://www.tasnetworks.com.au/Documents/Manual-documents/Planning-and-upgrades/Substation-Load-Information/2023-Zone-Substation-Report",
    }.get(year, None)


@pa.check_output(LoadSchema)
def _read_all_zss(file):
    df = (
        pl.from_pandas(
            pd.read_csv(file, header=[0, 1], index_col=0)
            .rename_axis(index="time")
            .rename_axis(columns=["zss", "metric"])
            .stack(["zss", "metric"], future_stack=True)
            .to_frame("value")
            .reset_index()
        )
        .with_columns(
            pl.when(pl.col("zss").str.contains("Unnamed:"))
            .then(pl.lit(None))
            .otherwise(pl.col("zss"))
            .forward_fill()
            .alias("name")
        )
        .with_columns(
            pl.col("time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("metric").str.to_lowercase(),
            pl.col("name").str.extract(r" \((\w+)\)$").alias("zss"),
            pl.col("value").cast(pl.Float32),
        )
        .pivot(on="metric", index=["time", "name", "zss"], values="value")
    )
    return df


if __name__ == "__main__":
    path = "/home/simba/Downloads/Tasnetworks-Zone-Substation-Load-Data-2023-24.csv"
    # url = get_url(2023)
    # df = download_file(url, path)
    df = read_all_zss(path)
    print(df)
