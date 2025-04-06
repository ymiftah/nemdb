import polars as pl

from nemdb.dnsp.common import LoadSchema

from nemdb.utils import download_file_to_bytesio
from nemdb import log

"""List of ZSS from the regulatory information notice 2024.
https://www.aer.gov.au/documents/ausnet-services-d-2023-24-category-analysis-rin-templates
"""
ALL_ZSS = [
    "BDL",
    "BGE",
    "BN",
    "BRA",
    "BRT",
    "BWA",
    "BWN",
    "BWR",
    "CF",
    "CLN",
    "CNR",
    "CPK",
    "CRE",
    "CYN",
    "DRN",
    "ELM",
    "EPG",
    "FGY",
    "FTR",
    "HPK",
    "KLK",
    "KLO",
    "KMS",
    "LDL",
    "LGA",
    "LLG",
    "LYD",
    "MBY",
    "MFA",
    "MJG",
    "MOE",
    "MSD",
    "MYT",
    "NLA",
    "NRN",
    "OFR",
    "PHI",
    "PHM",
    "RUBA",
    "RWN",
    "SLE",
    "SMG",
    "SMR",
    "TGN",
    "TT",
    "WGI",
    "WGL",
    "WN",
    "WO",
    "WT",
    "WYK",
    "MDI",
    "MWL",
    "RVE",
]


def get_url(zss):
    return f"https://dapr.ausnetservices.com.au/export_all_load_trace_data.php?station={zss}"


def read_all_zss(year: int):
    frames = []
    for zss in ALL_ZSS:
        log.info("Downloading Zone Substation loads from %s for year.", zss)
        try:
            frames.append(
                LoadSchema.validate(
                    _read_zss(download_file_to_bytesio(get_url(zss))).with_columns(
                        pl.lit(zss, pl.String).alias("zss")
                    )
                )
            )
        except Exception:
            log.warning("Error downloading Zone Substation loads from %s for year", zss)
    return pl.concat(frames)


def _read_zss(file):
    """
    Reads a csv file containing load data for a single zone substation (ZSS)

    Parameters
    ----------
    file : bytes
        A bytes object containing the csv file

    Returns
    -------
    pl.DataFrame
        A dataframe with columns "zss", "time", and "mw"
    """
    load = (
        pl.read_csv(
            file,
            has_header=False,
            new_columns=["date", "time", "mw"],
            schema_overrides={"date": str, "time": str, "mw": pl.Float32},
        )
        .with_columns(
            (pl.col("date") + " " + pl.col("time").str.replace("24:", "00:"))
            .str.to_datetime("%d-%b-%Y %H:%M")
            .alias("time"),
        )
        .select(["time", "mw"])
        .sort("time")
    )
    return load


# def read_all_zss():


if __name__ == "__main__":
    # zss = "WO"
    # url = get_url(zss)
    # path = f"/home/simba/Downloads/ausnet-{zss}-Zone-Substation-Load-Data-2023-24.zip"
    # bytesio = download_file_to_bytesio(url)
    df = read_all_zss(2024)
    print(df)
