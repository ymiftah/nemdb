import zipfile
import polars as pl


def get_url(year: int):
    return {
        2024: "https://daprprd.blob.core.windows.net/historial-loadtrace-data/zone%20substations.zip"
    }.get(year, None)


def read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for f in zip_ref.namelist():
            zss_name = f.split(" Zone Substation")[0]
            with zip_ref.open(f) as file:
                df = pl.read_excel(file.read())
            idx, names = _get_start_index(df)
            df = df[idx + 1 :]
            df.columns = names
            dfs.append(
                df.with_columns(
                    pl.lit(zss_name).alias("zss"),
                    pl.col("From").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("time"),
                    pl.col("MW").cast(pl.Float32),
                ).select(["zss", "time", "MW"])
            )
    return pl.concat(dfs)


def _get_start_index(df):
    for i, row in enumerate(df.iter_rows()):
        if row[0] == "From":
            return i, row


if __name__ == "__main__":
    # df = download_file(
    #     get_url(2024),
    #     "/home/simba/Downloads/Jemena-Network-Substation-Load-Data-2023-24.zip",
    # )
    df = read_all_zss(
        "/home/simba/Downloads/Jemena-Network-Substation-Load-Data-2023-24.zip"
    )
    print(df)
