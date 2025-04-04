import zipfile
import polars as pl
from nemdb.utils import download_file


def get_url(year: int):
    return {
        2024: "https://www.energex.com.au/__data/assets/file/0007/1385728/Energex-Network-Substation-Load-Data-2023-24.zip"
    }.get(year, None)


def read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for f in zip_ref.namelist():
            zss_name = f.split("_EGX_")[0]
            df = (
                pl.read_csv(zip_ref.open(f), columns=["Date", "Time", "MW"])
                .with_columns(
                    pl.lit(zss_name).alias("zss"),
                    (pl.col("Date") + " " + pl.col("Time"))
                    .str.to_datetime("%Y-%m-%d %H:%M:%S")
                    .alias("time"),
                )
                .select(["zss", "time", "MW"])
                .cast(
                    {
                        "MW": pl.Float32,
                    }
                )
            )
            dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    df = download_file(
        get_url(2024),
        "/home/simba/Downloads/Energex-Network-Substation-Load-Data-2023-24.zip",
    )
    df = read_all_zss(
        "/home/simba/Downloads/Energex-Network-Substation-Load-Data-2023-24.zip"
    )
    print(df)
