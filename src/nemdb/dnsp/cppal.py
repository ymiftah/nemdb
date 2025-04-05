import zipfile
import polars as pl

import pandera as pa
from nemdb.dnsp.common import LoadSchema


def get_url(year: int):
    """
    Download from
    "https://www.essentialenergy.com.au/ext/schools/EE-Zone-Substation-Load-Data-2023-24.zip"
    """
    raise NotImplementedError(
        """
        Citipower and Powercor share their data through an external provider Hightails
        "https://spaces.hightail.com/space/1aUFTWDtim"
        Run the processing method manually for now.
        """
    )


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
            zss_name = file.split("/")[1].split("_20")[0]
            with zip_ref.open(file) as f:
                df = (
                    pl.read_csv(f)
                    .with_columns(
                        zss=pl.lit(zss_name),
                        time=pl.col("date_time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                    )
                    .select(["zss", "time", "MW", "MVAR", "MVA"])
                    .rename({"MW": "mw", "MVAR": "mvar", "MVA": "mva"})
                )
                df = LoadSchema.validate(df)
            dfs.append(df)
    return pl.concat(dfs)


if __name__ == "__main__":
    file = "/home/simba/Downloads/1 - All CP and PAL ZSS data - FY22-23.zip"
    df = read_all_zss(file)
    print(df)
