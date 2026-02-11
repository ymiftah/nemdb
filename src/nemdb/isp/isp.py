import polars as pl
import fastexcel

import os
import nemdb


ISP_FILE = os.path.join(*nemdb.__path__, "artefacts", "ISP_2024.xlsx")
ISP_2025_FILE = os.path.join(*nemdb.__path__, "artefacts", "ISP_2025.xlsm")

# Sheets that are metadata/navigation, not data tables
_NON_DATA_SHEETS = {"Disclaimer", "Change Log", "Assumptions Summary"}


def _detect_header_row(
    df: pl.DataFrame, threshold: float = 0.4, max_scan: int = 50
) -> int:
    """Detect the column-header row in a raw DataFrame loaded with header_row=None.

    Finds the first row where at least 3 of the first 4 columns are non-null
    and at least ``threshold`` proportion of all columns are non-null.

    Parameters
    ----------
    df : pl.DataFrame
        Raw sheet loaded with ``header_row=None``.
    threshold : float
        Minimum proportion of non-null values in the row.
    max_scan : int
        Maximum number of rows to inspect.

    Returns
    -------
    int
        0-based row index of the detected header.
    """
    ncols = df.width
    min_non_null = max(3, int(ncols * threshold))

    for i in range(min(max_scan, len(df))):
        row = df.row(i)
        non_null_total = sum(
            1 for v in row if v is not None and str(v).strip() != ""
        )
        if non_null_total < min_non_null:
            continue
        first_n = min(4, ncols)
        first_non_null = sum(
            1
            for v in row[:first_n]
            if v is not None and str(v).strip() != ""
        )
        if first_non_null >= min(3, first_n):
            return i
    return 0


def _clean_table(df: pl.DataFrame) -> pl.DataFrame:
    """Drop fully-null rows and columns, and strip ``__UNNAMED__`` columns."""
    # Drop columns that are entirely null
    df = df[[c for c in df.columns if df[c].null_count() < len(df)]]
    # Drop rows where every value is null
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    return df


class ISPAssumptions(fastexcel.ExcelReader):
    def __init__(self, isp_file: str = ISP_2025_FILE):
        """
        Constructor for ISPAssumptions.

        Parameters
        ----------
        isp_file : str, default ISP_FILE
            Path to the ISP assumptions spreadsheet.

        Notes
        -----
        The path to the ISP assumptions spreadsheet can be overridden by setting the
        ``ISP_FILE`` environment variable.
        """
        self._reader = fastexcel.read_excel(isp_file)._reader

    def __repr__(self):
        return "ISP spreadsheet, with following tables :\n" + "\n - ".join(self.tables)

    @property
    def tables(self):
        """
        Returns the list of tables in the ISP assumptions spreadsheet.

        Returns
        -------
        List[str]
            List of table names.
        """
        return self.sheet_names

    @property
    def data_tables(self):
        """
        Returns the list of data tables (excludes metadata sheets like
        Disclaimer, Change Log, Assumptions Summary).

        Returns
        -------
        List[str]
            List of data table names.
        """
        return [s for s in self.sheet_names if s not in _NON_DATA_SHEETS]

    def read_table(self, table_name: str):
        """
        Reads the specified table from the ISP assumptions spreadsheet (raw).

        Parameters
        ----------
        table_name : str
            Name of the table to read.

        Returns
        -------
        pl.DataFrame
            The specified table as a Polars DataFrame.
        """
        return self.load_sheet_by_name(table_name).to_polars()

    def read_clean_table(
        self, table_name: str, *, header_row: int | None = None
    ) -> pl.DataFrame:
        """
        Reads a table with automatic header detection and cleaning.

        Skips the human-readable description rows at the top of each sheet,
        uses the detected (or supplied) row as column headers, drops all-null
        rows and columns, and returns a tidy DataFrame.

        Parameters
        ----------
        table_name : str
            Name of the sheet to read.
        header_row : int or None
            Explicit 0-based row index to use as column headers.
            If ``None``, the header row is auto-detected.

        Returns
        -------
        pl.DataFrame
            Cleaned table as a Polars DataFrame.
        """
        if header_row is None:
            raw = self.load_sheet_by_name(table_name, header_row=None).to_polars()
            header_row = _detect_header_row(raw)

        df = self.load_sheet_by_name(table_name, header_row=header_row).to_polars()
        return _clean_table(df)

    def summary(self) -> pl.DataFrame:
        """
        Reads the 'Assumptions Summary' sheet and returns the worksheet
        descriptions as a structured DataFrame.

        Returns
        -------
        pl.DataFrame
            DataFrame with columns: Worksheet, Assumptions Grouping,
            Description, Source.
        """
        raw = self.load_sheet_by_name(
            "Assumptions Summary", header_row=None
        ).to_polars()

        # Find the "Worksheet Descriptions" section and extract the table
        cols = ["Worksheet", "Assumptions Grouping", "Description", "Source"]
        start = None
        for i in range(len(raw)):
            row = raw.row(i)
            if row[0] is not None and str(row[0]).strip() == "Worksheet":
                start = i + 1
                break

        if start is None:
            return self.read_table("Assumptions Summary")

        rows = []
        for i in range(start, len(raw)):
            row = raw.row(i)
            first = str(row[0]) if row[0] is not None else ""
            if "Supporting Materials" in first:
                break
            if all(v is None for v in row):
                break
            rows.append({c: row[j] for j, c in enumerate(cols)})

        return pl.DataFrame(rows)


def read_coal_prices():
    df = pl.read_excel(ISP_FILE, sheet_name="Coal prices")
    df = df.unpivot(
        index=["Scenario", "Generator", "Coal Price Scenario"],
        value_name="price",
        variable_name="fy",
    ).drop("Scenario")
    return df
