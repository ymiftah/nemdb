import re
import os
import polars as pl

from nemdb import Config

RENAME = {
    "state": "state",
    "postcode": "postcode",
    "NMI_Bus_res": "nmi_bus_res",
    "Sum of Num_DER_Connections": "sum_der_connections",
    "Sum of Installed_DER_capacity_kVA": "sum_der_kva",
    "Sum of Solar_Connections": "sum_pv_count",
    "Sum of Solar_Devices": "sum_pv_devices",
    "Sum of Solar_capacity_kVA": "sum_pv_kva",
    "Sum of Battery_Connections": "sum_ess_count",
    "Sum of Battery_Devices": "sum_ess_devices",
    "Sum of Battery_capacity_kVA": "sum_ess_kva",
    "Sum of Battery_Storage_kVAh": "sum_ess_kvah",
    "Sum of Num_Other_Connections": "sum_install_other",
    "Sum of Installed_OtherDER_capacity_kVA": "sum_kva_other",
    "post_code": "postcode",
    "Sum of Num_DER_Sites": "sum_der_connections",
}


def _read_rename(file, method):
    df = method(file)
    df = df.rename({k: v for k, v in RENAME.items() if k in df.columns})
    return df


DIR = os.path.join(Config.CACHE_DIR, "DER_REGISTER")
files = os.listdir(DIR)
csv_files = list(f"{DIR}/{f}" for f in files if re.search(".csv$", f))
xlsx_files = list(f"{DIR}/{f}" for f in files if re.search(".xlsx$", f))

df_xlsx = pl.concat(
    (
        _read_rename(f, pl.read_excel).with_columns(
            date=pl.lit(re.sub(" DERR[ -]data.xlsx", "", os.path.basename(f)))
        )
        for f in xlsx_files
    ),
    how="diagonal_relaxed",
)
df_csv = pl.concat(
    (
        _read_rename(f, pl.read_csv).with_columns(
            date=pl.lit(re.sub(" DERR[ -]data.csv", "", os.path.basename(f)))
        )
        for f in csv_files
    ),
    how="diagonal_relaxed",
)
df = pl.concat(
    (
        df_csv,
        df_xlsx,
    ),
    how="diagonal_relaxed",
)
df.write_parquet("DERR_data_Jan_2025.parquet")
