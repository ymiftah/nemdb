"""
Data downloaded from https://www.endeavourenergy.com.au/modern-grid/creating-the-modern-grid/network-planning/distribution-annual-planning-report

"""

import polars as pl
import zipfile

import pandera as pa
from nemdb.dnsp.common import LoadSchema


def get_url(year: int):
    return {
        2024: "https://www.endeavourenergy.com.au/__data/assets/file/0025/78352/FY-23-DAPR-Upload-Folder.zip"
    }.get(year, None)


def list_zip_files(file):
    with zipfile.ZipFile(file, "r") as zip_ref:
        return zip_ref.namelist()


def list_zss(file):
    files = list_zip_files(file)
    return [f.split("/")[1].split(" ZS_")[0] for f in files]


@pa.check_output(LoadSchema)
def read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for f in zip_ref.namelist():
            zss_name = f.split("/")[1].split(" ZS_")[0]
            dfs.append(
                pl.read_csv(
                    zip_ref.open(f), has_header=False, new_columns=["time", "MW"]
                )
                .with_columns(
                    zss=pl.lit(zss_name),
                    time=pl.col("time").str.to_datetime("%d/%m/%Y %H:%M:%S"),
                )
                .cast(
                    {
                        "MW": pl.Float32,
                    }
                )
            )
    return pl.concat(dfs)


if __name__ == "__main__":
    # file = download_file(
    #     get_url(2024), "/home/simba/Downloads/Endeavour-FY-23-DAPR-Upload-Folder.zip"
    # )
    file = "/home/simba/Downloads/FY-23-DAPR-Upload-Folder.zip"
    # files = list_zip_files(file)
    # zss = list_zss(file)
    df = read_all_zss(file)
    print(df)
