import zipfile
import polars as pl


import pandera as pa
from nemdb.dnsp.common import LoadSchema


def get_url(year: int):
    return {
        2024: "https://media.unitedenergy.com.au/reports/UE-1-July-2022-to-30-June-2023-1.zip"
    }.get(year, None)


@pa.check_output(LoadSchema)
def read_all_zss(file):
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
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for file in zip_ref.namelist():
            if not file.endswith(".csv"):
                continue
            zss_name = file.split("/")[1].split("_20")[0].split("UE/")[0]
            with zip_ref.open(file) as f:
                df = (
                    pl.read_csv(f)
                    .select(pl.all().name.to_lowercase())
                    .with_columns(
                        zss=pl.lit(zss_name),
                        time=pl.col("date_time").str.to_datetime("%Y-%m-%d: %H:%M"),
                    )
                    .select(["zss", "time", "mw", "mvar", "mva"])
                )
                df = LoadSchema.validate(df)
            dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    from nemdb.utils import download_file

    url = get_url(2024)
    path = "/home/simba/Downloads/UE-1-July-2022-to-30-June-2023-1.zip"
    file = download_file(url, path)
    df = read_all_zss(file)
    print(df)
