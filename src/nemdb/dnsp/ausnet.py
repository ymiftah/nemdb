import polars as pl

import pandera as pa
from nemdb.dnsp.common import LoadSchema

from nemdb.utils import download_file_to_bytesio

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


@pa.check_output(LoadSchema)
def read_zss(file):
    load = (
        pl.read_csv(file, has_header=False, new_columns=["date", "time", "mw"])
        .with_columns(
            (pl.col("date") + " " + pl.col("time").str.replace("24:", "00:"))
            .str.to_datetime("%d-%b-%Y %H:%M")
            .alias("time"),
            pl.lit(zss).alias("zss"),
        )
        .select(["zss", "time", "mw"])
        .sort("time")
    )
    return load


# def read_all_zss():


if __name__ == "__main__":
    zss = "WO"
    url = get_url(zss)
    path = f"/home/simba/Downloads/ausnet-{zss}-Zone-Substation-Load-Data-2023-24.zip"
    bytesio = download_file_to_bytesio(url)
    df = read_zss(bytesio)
    print(df)
