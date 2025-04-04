import polars as pl
import pandas as pd

from nemdb.utils import download_file


def get_url(year: int):
    return {
        2023: "https://www.tasnetworks.com.au/Documents/Manual-documents/Planning-and-upgrades/Substation-Load-Information/2023-Zone-Substation-Report",
        2024: "https://www.tasnetworks.com.au/Documents/Manual-documents/Planning-and-upgrades/Substation-Load-Information/2023-Zone-Substation-Report",
    }.get(year, None)


def read_all_zss(file):
    return (
        pl.from_pandas(
            pd.read_csv(file, header=[0, 1], index_col=0)
            .rename_axis(index="time")
            .rename_axis(columns=["zss", "metric"])
            .xs("MW", axis=1, level="metric")
            .reset_index()
        )
        .with_columns(pl.col("time").str.to_datetime("%Y-%m-%d %H:%M:%S"))
        .unpivot(index="time", variable_name="zss", value_name="MW")
        .cast(
            {
                "MW": pl.Float32,
            }
        )
    )


if __name__ == "__main__":
    path = "/home/simba/Downloads/Tasnetworks-Zone-Substation-Load-Data-2023-24.csv"
    url = get_url(2023)
    df = download_file(url, path)
    df = read_all_zss(path)
    print(df)
