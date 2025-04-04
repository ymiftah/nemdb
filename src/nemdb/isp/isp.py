import polars as pl
import fastexcel

import os
import nemdb


ISP_FILE = os.path.join(*nemdb.__path__, "artefacts", "ISP_2024.xlsx")


class ISPAssumptions(fastexcel.ExcelReader):
    def __init__(self, isp_file: str = ISP_FILE):
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

    def read_table(self, table_name: str):
        """
        Reads the specified table from the ISP assumptions spreadsheet.

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


def read_coal_prices():
    df = pl.read_excel(ISP_FILE, sheet_name="Coal prices")
    df = df.unpivot(
        index=["Scenario", "Generator", "Coal Price Scenario"],
        value_name="price",
        variable_name="fy",
    ).drop("Scenario")
    return df
