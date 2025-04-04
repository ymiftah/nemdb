import zipfile
import polars as pl
import pandas as pd

from nemdb import log


import pandera as pa
from nemdb.dnsp.common import LoadSchema


def get_url(year: int):
    return {
        2024: "https://www.sapowernetworks.com.au/public/download.jsp?id=331119"
    }.get(year, None)


@pa.check_output(LoadSchema)
def read_all_zss(file):
    dfs = []
    with zipfile.ZipFile(file, "r") as zip_ref:
        for file in zip_ref.namelist():
            with zip_ref.open(file) as f:
                try:
                    df = (
                        pd.read_csv(f, header=[1, 2, 3], index_col=[0, 1])
                        .rename_axis(index=["date", "time"])
                        .rename_axis(columns=["zss", "connection_point", "metric"])
                    )
                except pd.errors.ParserError as exc:
                    log.error(
                        "Error %s while reading file %s, retrying ignoring bad lines",
                        exc,
                        file,
                    )
                    f.seek(0)  # return to start of file
                    df = (
                        pd.read_csv(
                            f, header=[1, 2, 3], index_col=[0, 1], on_bad_lines="skip"
                        )
                        .rename_axis(index=["date", "time"])
                        .rename_axis(columns=["zss", "connection_point", "metric"])
                    )
            _fix_columns(df)
            df = (
                df
                # Some files may have AMP readings we do not need, drop them ignoring errors
                .drop("Amp", level="metric", axis=1, errors="ignore")
                .stack(["zss", "connection_point"], future_stack=True)
                .reset_index()
            )
            load = (
                pl.from_pandas(df)
                .lazy()
                .with_columns(
                    (pl.col("date") + " " + pl.col("time"))
                    .str.to_datetime("%d/%m/%Y %H:%M")
                    .alias("time")
                )
                .select(["zss", "time", "MW", "MVar", "MVA"])
                .rename({"MVar": "mvar", "MVA": "mva", "MW": "mw"})
                .cast(
                    {
                        "mw": pl.Float32,
                        "mvar": pl.Float32,
                        "mva": pl.Float32,
                    }
                )
                .collect()
            )
            dfs.append(load)
    return pl.concat(dfs)


def _fix_columns(df: pd.DataFrame):
    columns = pd.DataFrame.from_records(df.columns).set_axis(df.columns.names, axis=1)
    clean_columns = (
        columns.apply(
            lambda s: s.where(
                ~(
                    s.str.contains("Zone Sub Name")
                    | s.str.contains("Unnamed")
                    | s.str.contains("Associated Connection Point")
                ),
                pd.NA,
            )
        )
        .assign(metric=lambda s: s["metric"].str.replace(r".\d", "", regex=True))
        .bfill(limit=1)
        .ffill(limit=1)
    )
    df.columns = pd.MultiIndex.from_frame(clean_columns)
    return df


if __name__ == "__main__":
    path = "/home/simba/Downloads/SAPN-Zone-Substation-Load-Data-2023-24.zip"
    # df = download_file(get_url(2024), path)
    df = read_all_zss(path)
    print(df)
