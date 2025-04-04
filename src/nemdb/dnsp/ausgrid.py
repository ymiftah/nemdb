import polars as pl
import zipfile


def get_url(year: int):
    return {
        2024: "https://www.ausgrid.com.au/-/media/Documents/Data-to-share/Distribution-zone-substation-informaton/FY2024.zip?rev=6fc7bcc1b5464355b0370de40aae283d"
    }.get(year, None)


def read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for file in zip_ref.namelist():
            with zip_ref.open(file) as f:
                df = (
                    pl.read_csv(f)
                    .rename(
                        {"Zone Substation": "zss", "Date": "date", "Unit": "metric"}
                    )
                    .drop("Year")
                    .filter(pl.col("metric") == "MW")
                    .unpivot(
                        index=["zss", "date", "metric"],
                        value_name="MW",
                        variable_name="time",
                    )
                    .with_columns(
                        (
                            pl.col("date")
                            + " "
                            + pl.col("time").str.replace("24:", "00:")
                        )
                        .str.to_datetime("%Y-%m-%d %H:%M")
                        .alias("time")
                    )
                    .select(["zss", "time", "MW"])
                ).cast(
                    {
                        "MW": pl.Float32,
                    }
                )
                dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    path = "/home/simba/Downloads/ausgrid-Zone-Substation-Load-Data-2023-24.zip"
    # df = download_file(URLS[2024], path)
    df = read_all_zss(path)
    print(df)
