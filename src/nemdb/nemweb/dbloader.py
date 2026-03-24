"""
The code below is largely inspired by Gorman et al., (2022). Nempy: A Python package for modelling the Australian National Electricity Market dispatch procedure.
Journal of Open Source Software, 7(70), 3596, https://doi.org/10.21105/joss.03596

The NEMWEBManager class is a wrapper around the DataSource class and its interface should be compatible with nempy.
This offers a compressed parquet based backend for the MMSDM tables instead of the incompressed SQLite tables
used in nempy.
"""

from contextlib import suppress
from datetime import datetime, timedelta
from functools import lru_cache

import fsspec
import pandas as pd
import pandera.polars as pa_polars
import polars as pl
from tqdm import tqdm

from nemdb import log as logger
from nemdb.config import config
from nemdb.dnsp import DNSPDataSource
from nemdb.nemweb.schemas import (
    BidDayOfferDSchema,
    BidPerOfferDSchema,
    DispatchConstraintSchema,
    DispatchInterconnectorResSchema,
    DispatchLoadSchema,
    DispatchPriceSchema,
    DispatchRegionSumSchema,
    DispatchUnitScadaSchema,
    DUALLOCSchema,
    DUDETAILSchema,
    DUDETAILSUMMARYSchema,
    GENCONDATASchema,
    GENCONSETINVOKESchema,
    GENCONSETSchema,
    GENCONSETTRKSchema,
    GENUNITSSchema,
    INTERCONNECTORCONSTRAINTSchema,
    INTERCONNECTORSchema,
    LOSSFACTORMODELSchema,
    LOSSMODELSchema,
    MNSP_INTERCONNECTORSchema,
    PARTICIPANTSchema,
    RESERVESchema,
    SPDCONNECTIONPOINTCONSTRAINTSchema,
    SPDINTERCONNECTORCONSTRAINTSchema,
    SPDREGIONCONSTRAINTSchema,
    STADUALLOCSchema,
    STATIONOPERATINGSTATUSSchema,
    STATIONOWNERSchema,
    STATIONSchema,
    _schema_to_dtypes,
)

from .nemweb import read_bids
from .utils import cache_response_zip

URL = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month:02d}010000.zip"
URL_ALT = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE%23{table}%23FILE01%23{year}{month:02d}010000.zip"

STRPTIME = "%Y/%m/%d %H:%M:%S"


class NEMWEBManager:
    """Interface for accessing historical date from NEM spot market dispatch.

    This class provides an interface to download, cache, and access historical
    data from the Australian National Electricity Market (NEM). It manages
    various data sources and tables, storing them in Parquet format for
    efficient access.

    Attributes
    ----------
    tables : list[str]
        A list of available table names.

    Examples
    --------
    >>> from nemdb import NEMWEBManager
    >>> nemweb = NEMWEBManager()
    >>> nemweb.populate(slice("2024-01-01", "2024-01-31"))
    >>> df = nemweb.DISPATCHREGIONSUM.get_data("2024/01/01 12:00:00")


    Add data from AEMO nemweb data portal. In this case we are adding data from the table DISPATCHREGIONSUM which contains
    a dispatch summary by region, the data comes in monthly chunks.

    >>> historical.DISPATCHREGIONSUM.add_data(year=2020, month=1)

    >>> historical.DISPATCHREGIONSUM.add_data(year=2020, month=2)

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical.DISPATCHREGIONSUM.get_data('2020/01/10 12:35:00').head())
            SETTLEMENTDATE REGIONID  TOTALDEMAND  DEMANDFORECAST  INITIALSUPPLY
    0  2020/01/10 12:35:00     NSW1      9938.01        34.23926     9902.79199
    1  2020/01/10 12:35:00     QLD1      6918.63        26.47852     6899.76270
    2  2020/01/10 12:35:00      SA1      1568.04         4.79657     1567.85864
    3  2020/01/10 12:35:00     TAS1      1124.05        -3.43994     1109.36963
    4  2020/01/10 12:35:00     VIC1      6633.45        37.05273     6570.15527


    >>> historical.DUDETAILSUMMARY.add_data(year=2020, month=2)

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical.DUDETAILSUMMARY.get_data('2020/01/10 12:35:00').head())
           DUID           START_DATE             END_DATE DISPATCHTYPE CONNECTIONPOINTID REGIONID  TRANSMISSIONLOSSFACTOR  DISTRIBUTIONLOSSFACTOR  SCHEDULE_TYPE
    0    AGLHAL  2019/07/01 00:00:00  2020/01/20 00:00:00    GENERATOR             SHPS1      SA1                  0.9748                  1.0000      SCHEDULED
    1   AGLNOW1  2019/07/01 00:00:00  2999/12/31 00:00:00    GENERATOR             NDT12     NSW1                  0.9929                  1.0000  NON-SCHEDULED
    2  AGLSITA1  2019/07/01 00:00:00  2999/12/31 00:00:00    GENERATOR            NLP13K     NSW1                  1.0009                  1.0000  NON-SCHEDULED
    3    AGLSOM  2019/07/01 00:00:00  2999/12/31 00:00:00    GENERATOR             VTTS1     VIC1                  0.9915                  0.9891      SCHEDULED
    4   ANGAST1  2019/07/01 00:00:00  2999/12/31 00:00:00    GENERATOR             SDRN1      SA1                  0.9517                  0.9890      SCHEDULED

    Parameters
    ----------
    source : Path to the parquet data set (accepts cloud storage paths)


    Attributes
    ----------
    BIDPEROFFER_D : InputsByIntervalDateTime
        Unit volume bids by 5 min dispatch intervals.
    BIDDAYOFFER_D : InputsByDay
        Unit price bids by market day.
    DISPATCHREGIONSUM : InputsBySettlementDate
        Regional demand terms by 5 min dispatch intervals.
    DISPATCHLOAD : InputsBySettlementDate
        Unit operating conditions by 5 min dispatch intervals.
    DUDETAILSUMMARY : InputsStartAndEnd
        Unit information by the start and end times of when the information is applicable.
    DISPATCHCONSTRAINT : InputsBySettlementDate
        The generic constraints that were used in each 5 min interval dispatch.
    GENCONDATA : InputsByMatchDispatchConstraints
        The generic constraints information, their applicability to a particular dispatch interval is determined by
        reference to DISPATCHCONSTRAINT.
    SPDREGIONCONSTRAINT : InputsByMatchDispatchConstraints
        The regional lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    SPDCONNECTIONPOINTCONSTRAINT : InputsByMatchDispatchConstraints
        The connection point lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    SPDINTERCONNECTORCONSTRAINT : InputsByMatchDispatchConstraints
        The interconnector lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    INTERCONNECTOR : InputsNoFilter
        The the regions that each interconnector links.
    INTERCONNECTORCONSTRAINT : InputsByEffectiveDateVersionNoAndDispatchInterconnector
        Interconnector properties FROMREGIONLOSSSHARE, LOSSCONSTANT, LOSSFLOWCOEFFICIENT, MAXMWIN, MAXMWOUT by
        EFFECTIVEDATE and VERSIONNO.
    LOSSMODEL : InputsByEffectiveDateVersionNoAndDispatchInterconnector
        Break points used in linearly interpolating interconnector loss funtctions by EFFECTIVEDATE and VERSIONNO.
    LOSSFACTORMODEL : InputsByEffectiveDateVersionNoAndDispatchInterconnector
        Coefficients of demand terms in interconnector loss functions.
    DISPATCHINTERCONNECTORRES : InputsBySettlementDate
        Record of which interconnector were used in a particular dispatch interval.

    """

    def __init__(self):
        self._active_tables = [
            "BIDDAYOFFER_D",
            "BIDPEROFFER_D",
            "DISPATCHCONSTRAINT",
            "DISPATCHINTERCONNECTORRES",
            "DISPATCHLOAD",
            "DISPATCHREGIONSUM",
            "DISPATCHPRICE",
            "DISPATCH_UNIT_SCADA",
            "DUDETAILSUMMARY",
            "DUDETAIL",
            "DUALLOC",
            "INTERCONNECTOR",
            "INTERCONNECTORCONSTRAINT",
            "LOSSMODEL",
            "LOSSFACTORMODEL",
            "GENUNITS",
            "GENCONDATA",
            "SPDREGIONCONSTRAINT",
            "SPDCONNECTIONPOINTCONSTRAINT",
            "SPDINTERCONNECTORCONSTRAINT",
            "GENCONSET",
            "GENCONSETINVOKE",
            "GENCONSETTRK",
            "PARTICIPANT",
            "STATION",
            "STATIONOPERATINGSTATUS",
            "STADUALLOC",
            "MNSP_INTERCONNECTOR",
            "RESERVE",
            # "ZONE_SUBSTATION",
        ]
        self.ZONE_SUBSTATION = DNSPDataSource(
            table_name="ZONE_SUBSTATION",
            table_columns=[
                "time",
                "zss",
                "MW",
                "network",
            ],
            add_partitions=["network"],
            table_primary_keys=["zss", "time"],
        )
        self.DUALLOC = DataSource(
            table_name="DUALLOC",
            schema_class=DUALLOCSchema,
        )
        self.GENUNITS = DataSource(
            table_name="GENUNITS",
            table_primary_keys=["STATIONID", "LASTCHANGED"],
            schema_class=GENUNITSSchema,
        )
        self.RESERVE = BySettlementDate(
            table_name="RESERVE",
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
            schema_class=RESERVESchema,
        )
        self.DISPATCHREGIONSUM = BySettlementDate(
            table_name="DISPATCHREGIONSUM",
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
            schema_class=DispatchRegionSumSchema,
        )
        self.DISPATCHLOAD = BySettlementDate(
            table_name="DISPATCHLOAD",
            table_primary_keys=["SETTLEMENTDATE", "DUID"],
            schema_class=DispatchLoadSchema,
        )
        self.DISPATCHPRICE = BySettlementDate(
            table_name="DISPATCHPRICE",
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
            schema_class=DispatchPriceSchema,
        )
        self.DUDETAILSUMMARY = ByStartEnd(
            table_name="DUDETAILSUMMARY",
            table_primary_keys=["END_DATE", "REGIONID", "DUID"],
            schema_class=DUDETAILSUMMARYSchema,
        )
        self.DUDETAIL = ByEffectiveDateVersionNo(
            table_name="DUDETAIL",
            table_primary_keys=["DUID", "VERSIONNO"],
            schema_class=DUDETAILSchema,
        )
        self.PARTICIPANT = DataSource(
            table_name="PARTICIPANT",
            table_primary_keys=["PARTICIPANTID", "LASTCHANGED"],
            schema_class=PARTICIPANTSchema,
        )
        self.STATION = DataSource(
            table_name="STATION",
            table_primary_keys=["STATIONID"],
            schema_class=STATIONSchema,
        )
        self.STATIONOPERATINGSTATUS = DataSource(
            table_name="STATIONOPERATINGSTATUS",
            table_primary_keys=["STATIONID", "EFFECTIVEDATE"],
            schema_class=STATIONOPERATINGSTATUSSchema,
        )
        self.STATIONOWNER = DataSource(
            table_name="STATIONOWNER",
            table_primary_keys=["STATIONID", "EFFECTIVEDATE"],
            schema_class=STATIONOWNERSchema,
        )
        self.STADUALLOC = DataSource(
            table_name="STADUALLOC",
            table_primary_keys=["DUID", "EFFECTIVEDATE", "STATIONID", "VERSIONNO"],
            schema_class=STADUALLOCSchema,
        )
        self.BIDDAYOFFER_D = BySettlementDate(
            table_name="BIDDAYOFFER_D",
            table_primary_keys=["SETTLEMENTDATE", "DUID", "VERSIONNO"],
            schema_class=BidDayOfferDSchema,
        )
        self.BIDPEROFFER_D = BySettlementDate(
            table_name="BIDPEROFFER_D",
            table_primary_keys=["SETTLEMENTDATE", "DUID", "INTERVAL_DATETIME"],
            low_memory=True,
            schema_class=BidPerOfferDSchema,
        )
        self.DISPATCHCONSTRAINT = BySettlementDate(
            table_name="DISPATCHCONSTRAINT",
            table_primary_keys=["SETTLEMENTDATE", "CONSTRAINTID"],
            schema_class=DispatchConstraintSchema,
        )
        self.GENCONDATA = ByEffectiveDateVersionNo(
            table_name="GENCONDATA",
            table_primary_keys=["GENCONID", "EFFECTIVEDATE", "VERSIONNO"],
            schema_class=GENCONDATASchema,
        )
        self.SPDREGIONCONSTRAINT = ByEffectiveDateVersionNo(
            table_name="SPDREGIONCONSTRAINT",
            table_primary_keys=[
                "REGIONID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "BIDTYPE",
            ],
            schema_class=SPDREGIONCONSTRAINTSchema,
        )
        self.SPDCONNECTIONPOINTCONSTRAINT = ByEffectiveDateVersionNo(
            table_name="SPDCONNECTIONPOINTCONSTRAINT",
            table_primary_keys=[
                "CONNECTIONPOINTID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "BIDTYPE",
            ],
            schema_class=SPDCONNECTIONPOINTCONSTRAINTSchema,
        )
        self.SPDINTERCONNECTORCONSTRAINT = ByEffectiveDateVersionNo(
            table_name="SPDINTERCONNECTORCONSTRAINT",
            table_primary_keys=[
                "INTERCONNECTORID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
            ],
            schema_class=SPDINTERCONNECTORCONSTRAINTSchema,
        )
        self.GENCONSET = ByEffectiveDateVersionNo(
            table_name="GENCONSET",
            table_primary_keys=["GENCONSETID", "EFFECTIVEDATE", "VERSIONNO", "GENCONID"],
            schema_class=GENCONSETSchema,
        )
        # GENCONSETINVOKE is keyed by INVOCATION_ID (no EFFECTIVEDATE/VERSIONNO).
        # Use base DataSource so .scan() works; nemo pipeline never calls .get_data().
        self.GENCONSETINVOKE = DataSource(
            table_name="GENCONSETINVOKE",
            table_primary_keys=["INVOCATION_ID"],
            schema_class=GENCONSETINVOKESchema,
        )
        self.GENCONSETTRK = ByEffectiveDateVersionNo(
            table_name="GENCONSETTRK",
            table_primary_keys=["GENCONSETID", "EFFECTIVEDATE", "VERSIONNO"],
            schema_class=GENCONSETTRKSchema,
        )
        self.INTERCONNECTOR = ByEffectiveDateVersionNo(
            table_name="INTERCONNECTOR",
            table_primary_keys=["INTERCONNECTORID"],
            schema_class=INTERCONNECTORSchema,
        )
        self.INTERCONNECTORCONSTRAINT = ByEffectiveDateVersionNo(
            table_name="INTERCONNECTORCONSTRAINT",
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
            schema_class=INTERCONNECTORCONSTRAINTSchema,
        )
        self.LOSSMODEL = ByEffectiveDateVersionNo(
            table_name="LOSSMODEL",
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
            schema_class=LOSSMODELSchema,
        )
        self.LOSSFACTORMODEL = ByEffectiveDateVersionNo(
            table_name="LOSSFACTORMODEL",
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
            schema_class=LOSSFACTORMODELSchema,
        )
        self.DISPATCHINTERCONNECTORRES = BySettlementDate(
            table_name="DISPATCHINTERCONNECTORRES",
            table_primary_keys=["INTERCONNECTORID", "SETTLEMENTDATE"],
            schema_class=DispatchInterconnectorResSchema,
        )
        self.DISPATCH_UNIT_SCADA = BySettlementDate(
            table_name="DISPATCH_UNIT_SCADA",
            table_primary_keys=["SETTLEMENTDATE", "DUID"],
            schema_class=DispatchUnitScadaSchema,
        )
        self.MNSP_INTERCONNECTOR = ByEffectiveDateVersionNo(
            table_name="MNSP_INTERCONNECTOR",
            table_primary_keys=[
                "INTERCONNECTORID",
                "LINKID",
                "EFFECTIVEDATE",
                "VERSIONNO",
            ],
            schema_class=MNSP_INTERCONNECTORSchema,
        )

    def __repr__(self):
        source = config.cache_dir
        return "\n".join(
            (
                f"DBManager at {source}, with tables:",
                *[f"-- {table}" for table in self.tables if table != "source"],
            )
        )

    @property
    def tables(self):
        return [v for v in vars(self) if v != "source"]

    def active_tables(self):
        """
        Retrieve the list of active tables in the database.

        This method returns a list of table names that are currently active
        and available for data processing and population in the database.
        """
        return self._active_tables

    def populate(self, date_slice: slice, force_new: bool = False):
        """Fetch data for all active tables and populate the parquet datasets.

        This method iterates through all active tables and populates them with
        data for the given date range.

        Args:
            date_slice (slice): A slice object representing the date range.
            force_new (bool, optional): If True, re-downloads and overwrites
                existing data. Defaults to False.
        """
        logger.info(
            "Populating database with data from %s to %s",
            date_slice.start,
            date_slice.stop,
        )
        with pl.StringCache():  # Ensures consistent Categorical values across all tables
            for table in tqdm(self.active_tables()):
                table_: DataSource = getattr(self, table)
                table_.populate(date_slice, force_new=force_new)

    @staticmethod
    @lru_cache(maxsize=4)
    def read_bids(year: int, month: int, day: int) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Read bid data for a specific date

        NOTE This method reads data for the specific date, zips are cached locally
        But not persisted in the database yet because of the size.
        """
        return read_bids(year, month, day)

    def get_unit_volume_bids(self, date: str) -> pl.DataFrame:
        """Get unit volume bids for a specific date.

        Args:
            date (str): The date in "YYYY/MM/DD HH:MM:SS" format.

        Returns:
            pl.DataFrame: A DataFrame containing unit volume bids.
        """
        date_parsed = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        _, volume = self.read_bids(date_parsed.year, date_parsed.month, date_parsed.day)
        volume_pl = volume.with_columns(
            (pl.col("ROCUP") * 60).alias("RAMPUPRATE"),
            (pl.col("ROCDOWN") * 60).alias("RAMPDOWNRATE"),
        )[
            [
                "INTERVAL_DATETIME",
                "DUID",
                "BIDTYPE",
                "MAXAVAIL",
                "FIXEDLOAD",
                "ENABLEMENTMIN",
                "ENABLEMENTMAX",
                "LOWBREAKPOINT",
                "HIGHBREAKPOINT",
                "BANDAVAIL1",
                "BANDAVAIL2",
                "BANDAVAIL3",
                "BANDAVAIL4",
                "BANDAVAIL5",
                "BANDAVAIL6",
                "BANDAVAIL7",
                "BANDAVAIL8",
                "BANDAVAIL9",
                "BANDAVAIL10",
                "RAMPUPRATE",
                "RAMPDOWNRATE",
            ]
        ]
        return volume_pl

    def get_unit_price_bids(self, date: str) -> pl.DataFrame:
        """Get unit price bids for a specific date.

        Args:
            date (str): The date in "YYYY/MM/DD HH:MM:SS" format.

        Returns:
            pl.DataFrame: A DataFrame containing unit price bids.
        """
        date_parsed = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        price, _ = self.read_bids(date_parsed.year, date_parsed.month, date_parsed.day)
        return price[
            [
                "SETTLEMENTDATE",
                "DUID",
                "BIDTYPE",
                "PRICEBAND1",
                "PRICEBAND2",
                "PRICEBAND3",
                "PRICEBAND4",
                "PRICEBAND5",
                "PRICEBAND6",
                "PRICEBAND7",
                "PRICEBAND8",
                "PRICEBAND9",
                "PRICEBAND10",
            ]
        ]


def _get_archive(table_name, year, month):
    # Insert the table_name, year and month into the url.
    url = URL.format(table=table_name, year=year, month=month)
    # Download the file.
    try:
        r = cache_response_zip(url)
    except ValueError:  # TODO better errors
        logger.info("Retry with alternative url")
        url = URL_ALT.format(table=table_name, year=year, month=month)
        try:
            r = cache_response_zip(url)
        except ValueError as err:
            raise _MissingData(
                f"""Requested data for table: {table_name}, year: {year}, month: {month}
                                not downloaded. Please check your internet connection. Also check
                                http://nemweb.com.au/#mms-data-model, to see if your requested
                                data is uploaded."""
            ) from err
    return r


def _archive_to_df(
    archive: str,
    table_columns: list[str],
    dtypes: dict[str, type],
    year: int,
    month: int,
    _low_memory: bool = False,
) -> pl.DataFrame:
    """Downloads a zipped csv file and converts it to a polars DataFrame.

    Args:
        archive: Path to CSV file (from ZIP)
        table_columns: List of column names to read
        dtypes: Dict mapping column names to Polars types
        year: Year of data
        month: Month of data
        _low_memory: Unused (for compatibility); low-memory handling is in add_data()

    Returns:
        Polars DataFrame with data cast to correct types

    Examples
    --------
    This will only work if you are connected to the internet.

    >>> url = ('http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' +
    ...        'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip')

    >>> table_name = 'DISPATCHREGIONSUM'

    >>> df = _download_to_df(url, table_name='DISPATCHREGIONSUM', year=2020, month=1)

    >>> print(df)
           I  DISPATCH  ... SEMISCHEDULE_CLEAREDMW  SEMISCHEDULE_COMPLIANCEMW
    0      D  DISPATCH  ...              549.30600                    0.00000
    1      D  DISPATCH  ...              102.00700                    0.00000
    2      D  DISPATCH  ...              387.40700                    0.00000
    3      D  DISPATCH  ...              145.43200                    0.00000
    4      D  DISPATCH  ...              136.85200                    0.00000
    ...   ..       ...  ...                    ...                        ...
    45380  D  DISPATCH  ...              757.47600                    0.00000
    45381  D  DISPATCH  ...              142.71600                    0.00000
    45382  D  DISPATCH  ...              310.28903                    0.36103
    45383  D  DISPATCH  ...               83.94100                    0.00000
    45384  D  DISPATCH  ...              196.69610                    0.69010
    <BLANKLINE>
    [45385 rows x 109 columns]

    Parameters
    ----------
    url : str
        A url of the format 'PUBLIC_DVD_{table}_{year}{month}010000.zip', typically this will be a location on AEMO's
        nemweb portal where data is stored in monthly archives.

    table_name : str
        The name of the table you want to download from nemweb.

    year : int
        The year the table is from.

    month : int
        The month the table is form.

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    MissingData
        If internet connection is down, nemweb is down or data requested is not on nemweb.

    """
    # Read the file into a DataFrame.
    available_cols = read_header(archive)
    table_dtypes = {k: dtypes[k] for k in set(table_columns).intersection(available_cols)}

    data = pd.read_csv(
        archive,
        skiprows=1,
        usecols=list(table_dtypes.keys()),
    )
    missing_columns = set(table_columns).difference(available_cols)
    if len(missing_columns):
        logger.info(
            "Columns %s were not found in the file %s for %d-%d, filling with null values",
            missing_columns,
            table_columns,
            year,
            month,
        )
    data = data.assign(**dict.fromkeys(missing_columns))
    date_types = [k for k in table_dtypes if table_dtypes[k] in (pl.Date, pl.Datetime)]
    for col in date_types:
        data[col] = pd.to_datetime(data[col].to_list(), format=STRPTIME, errors="coerce")
    # Discard last row of DataFrame
    data = data[:-1]
    return pl.from_dataframe(data).cast({k: dtypes[k] for k in set(table_columns)})


def read_header(file: str):
    """Returns the set of columns in the file"""
    return set(pd.read_csv(file, skiprows=1, nrows=1).columns)


class _MissingData(Exception):
    """Raise for nemweb not returning status 200 for file request."""


class DataSource:
    """Manages Market Management System (MMS) tables stored as parquet files.

    This class provides a base for managing MMS tables, including downloading,
    caching, and accessing data.

    Args:
        table_name (str): The name of the table.
        schema_class: Pandera DataFrameModel schema for this table.
        table_primary_keys (list[str], optional): A list of primary key
            columns. Defaults to None.
        add_partitions (list[str], optional): A list of additional columns
            to partition by. Defaults to None.
        low_memory (bool, optional): If True, uses a low-memory mode for
            processing data. Defaults to False.
    """

    def __init__(
        self,
        table_name: str,
        schema_class: type[pa_polars.DataFrameModel],
        table_primary_keys: list[str] | None = None,
        add_partitions: list[str] | None = None,
        low_memory: bool = False,
    ):
        """Creates a parquet dataset.

        Args:
            table_name: Name of the table (used as subdirectory in cache)
            schema_class: Pandera DataFrameModel schema for this table (defines columns and types)
            table_primary_keys: Optional list of primary key column names
            add_partitions: Optional list of additional partition columns
            low_memory: Whether to use lower memory mode for reading
        """
        self.table_name = table_name
        self.schema_class = schema_class
        # Derive table_columns from schema
        self.table_columns = list(schema_class.empty().columns)
        # Extract types from schema (unwrapping X | None unions to bare types)
        self._dtypes = _schema_to_dtypes(schema_class)
        self.table_primary_keys = table_primary_keys if table_primary_keys is not None else []
        self.partitions = (
            [*add_partitions, "archive_month"] if add_partitions is not None else ["archive_month"]
        )
        self.low_memory = low_memory

        self.path = f"{config.cache_dir}/{table_name}/"
        self.fs = fsspec.filesystem(config.filesystem)
        self.fs.makedirs(f"{config.cache_dir}/{table_name}", exist_ok=True)

    def scan(self, *args, **kwargs) -> pl.LazyFrame:
        """Scans the parquet dataset with polars.

        Returns:
            pl.LazyFrame: A LazyFrame representing the scanned dataset.
        """
        kwargs_ = {"hive_partitioning": True, "missing_columns": "insert"}
        if kwargs:
            kwargs_.update(kwargs)
        return pl.scan_parquet(self.path, *args, **kwargs_)  # type: ignore[arg-type]

    def read(self, *args, **kwargs) -> pl.DataFrame:
        """Reads the parquet dataset with polars.

        Note:
            This method is provided for compatibility with nempy, but it should
            be avoided as it can read massive amounts of data into memory.

        Returns:
            pl.DataFrame: A DataFrame containing the dataset.
        """
        return self.scan(self, *args, **kwargs).collect()

    def populate(self, date_slice: slice, force_new: bool = False):
        """Adds data to the parquet dataset from a date range.

        Args:
            date_slice (slice): A slice object representing the date range.
            force_new (bool, optional): If True, re-downloads and overwrites
                existing data. Defaults to False.
        """
        date_range = pd.date_range(start=date_slice.start, end=date_slice.stop, freq="MS")
        logger.info("Populating database with data from %s to %s", date_range[0], date_range[-1])
        with pl.StringCache():  # Ensures consistent Categorical values across all tables
            for date in tqdm(date_range):
                year = date.year
                month = date.month
                # Check if data already exists in tables before adding
                data_exists = False
                if not force_new:
                    with suppress(pl.exceptions.ComputeError, FileNotFoundError, Exception):
                        logger.info(
                            "Checking if data already exists for %s %s / %s",
                            self.table_name,
                            year,
                            month,
                        )
                        check = (
                            self.scan()
                            .filter(pl.col("archive_month") == pl.date(year, month, 1))
                            .head()
                        )
                        data_exists = len(check.collect()) > 0
                if not data_exists:
                    self.add_data(year=year, month=month)
                else:
                    logger.info(
                        "Data already exists for %s %s / %s, skipping download. Use force_new=True to overwrite.",
                        self.table_name,
                        year,
                        month,
                    )

    def add_data(self, year: int, month: int, **kwargs):
        """Download data for the given table and time, replace any existing data.

        Parameters
        ----------
        year : int
            The year to download data for.
        month : int
            The month to download data for.

        Return
        ------
        None
        """
        name = self.table_name
        partition_cols: list[str] = self.partitions

        if self.low_memory:
            self._add_data_low_memory(year, month, name, **kwargs)
            return

        try:
            data = (
                self.fetch_data(year, month)
                .with_columns(
                    pl.date(year, month, 1).alias("archive_month"),
                )
                .sort(partition_cols + self.table_primary_keys)
            )
        except _MissingData:
            logger.error("No data available for %s %s / %s", self.table_name, year, month)
            return

        logger.debug(
            "Writing data for %s %s / %s, at location %s",
            self.table_name,
            year,
            month,
            f"{name}-{{i}}.parquet",
        )
        data.write_parquet(
            self.path,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": partition_cols,
                "existing_data_behavior": "overwrite_or_ignore",
                "basename_template": f"{name}-{{i}}.parquet",
            },
            **kwargs,
        )

    def _add_data_low_memory(self, year, month, name, **kwargs):
        logger.info("Fetching data (low memory mode) for %s %s / %s", name, year, month)
        try:
            archive = _get_archive(name, year, month)
            self._archive_to_df_low_memory(
                archive, name, self.table_columns, self._dtypes, year, month, self.path, **kwargs
            )
            return None
        except _MissingData:
            logger.error("No data available for %s %s / %s", self.table_name, year, month)
            return

    def _archive_to_df_low_memory(
        self, archive, name, table_columns, dtypes, year, month, path, **kwargs
    ):
        """Read CSV in chunks and write parquet, handling low-memory scenarios.

        Args:
            archive: Path to CSV file (from ZIP)
            name: Table name
            table_columns: List of column names
            dtypes: Dict mapping column names to Polars types
            year: Year of data
            month: Month of data
            path: Output parquet path
            **kwargs: Additional args for write_parquet
        """
        partition_cols = self.partitions

        # Read the file into a DataFrame.
        available_cols = read_header(archive)
        table_dtypes = {k: dtypes[k] for k in set(table_columns).intersection(available_cols)}
        missing_columns = set(table_columns).difference(available_cols)
        if len(missing_columns):
            logger.info(
                "Columns %s were not found in the file %s for %d-%d, filling with null values",
                missing_columns,
                table_columns,
                year,
                month,
            )

        reader = pd.read_csv(
            archive,
            skiprows=1,
            usecols=list(table_dtypes.keys()),
            chunksize=1_000_000,
        )
        for j, raw_chunk in enumerate(reader):
            chunk = raw_chunk.assign(**dict.fromkeys(missing_columns))
            date_types = [k for k in table_dtypes if table_dtypes[k] in (pl.Date, pl.Datetime)]
            for col in date_types:
                chunk[col] = pd.to_datetime(chunk[col], format=STRPTIME, errors="coerce")

            data = (
                pl.from_dataframe(chunk)
                .cast({k: dtypes[k] for k in set(table_columns)})
                .with_columns(
                    pl.date(year, month, 1).alias("archive_month"),
                )
                .sort(partition_cols + self.table_primary_keys)
            )

            logger.debug(
                "Writing data for %s %s / %s, at location %s",
                self.table_name,
                year,
                month,
                f"{name}-{j}.parquet",
            )
            data.write_parquet(
                path,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": partition_cols,
                    "existing_data_behavior": "overwrite_or_ignore",
                    "basename_template": f"{name}-{j}-{{i}}.parquet",
                },
                **kwargs,
            )

    def fetch_data(self, year: int, month: int) -> pl.DataFrame:
        """Fetches data for a specific year and month.

        Args:
            year (int): The year to fetch data for.
            month (int): The month to fetch data for.

        Returns:
            pl.DataFrame: A DataFrame containing the fetched data.
        """
        logger.info("Fetching data for %s %s / %s", self.table_name, year, month)
        archive = _get_archive(self.table_name, year, month)
        return _archive_to_df(
            archive, self.table_columns, self._dtypes, year, month, _low_memory=self.low_memory
        )

    def get_data(self) -> pl.DataFrame:  # type: ignore[return-value]
        return self.read()


class BySettlementDate(DataSource):
    """A DataSource filtered by settlement date."""

    def get_data(self, date_time: str) -> pl.DataFrame:  # type: ignore[override]
        """Gets data for a specific settlement date.

        Args:
            date_time (str): The date and time in "YYYY/MM/DD HH:MM:SS" format.

        Returns:
            pl.DataFrame: A DataFrame containing the data.
        """
        date_time_parsed = datetime.strptime(date_time, "%Y/%m/%d %H:%M:%S")
        return self.scan().filter(pl.col("SETTLEMENTDATE") == date_time_parsed).collect()


class ByIntervalDate(DataSource):
    """A DataSource filtered by interval date."""

    def get_data(self, date_time: str) -> pl.DataFrame:  # type: ignore[override]
        """Gets data for a specific interval date.

        Args:
            date_time (str): The date and time in "YYYY/MM/DD HH:MM:SS" format.

        Returns:
            pl.DataFrame: A DataFrame containing the data.
        """
        date_time_parsed = datetime.strptime(date_time, "%Y/%m/%d %H:%M:%S")
        return self.scan().filter(pl.col("INTERVAL_DATETIME") == date_time_parsed).collect()


class BySettlementDay(DataSource):
    """A DataSource filtered by settlement day."""

    def get_data(self, date_time: str) -> pl.DataFrame:  # type: ignore[override]
        """Gets data for a specific settlement day.

        Args:
            date_time (str): The date in "YYYY/MM/DD" format.

        Returns:
            pl.DataFrame: A DataFrame containing the data.
        """
        # Convert to datetime object
        date_time_parsed = datetime.strptime(date_time, "%Y/%m/%d")
        # Change date_time provided so any time less than 04:05:00 will have the previous days date.
        date_time_parsed = date_time_parsed - timedelta(hours=4, seconds=1)
        # Convert to date
        date = date_time_parsed.date()
        return self.scan().filter(pl.col("SETTLEMENTDATE") == date).collect()


class ByStartEnd(DataSource):
    """A DataSource filtered by start and end dates."""

    def get_data(self, date_time: str) -> pl.DataFrame:  # type: ignore[override]
        """Gets data for a specific date within the start and end dates.

        Args:
            date_time (str): The date in "YYYY/MM/DD" format.

        Returns:
            pl.DataFrame: A DataFrame containing the data.
        """
        date = datetime.strptime(date_time, "%Y/%m/%d")
        return (
            self.scan()
            .filter(
                (pl.col("START_DATE") <= date)
                & (pl.col("END_DATE").is_null() | (pl.col("END_DATE") >= date))
            )
            .collect()
        )


class ByEffectiveDateVersionNo(DataSource):
    """A DataSource filtered by effective date and version number."""

    def get_data(self, date_time: str) -> pl.DataFrame:  # type: ignore[override]
        """Gets data for a specific date, returning the latest version.

        Args:
            date_time (str): The date in "YYYY/MM/DD" format.

        Returns:
            pl.DataFrame: A DataFrame containing the data.
        """
        date = datetime.strptime(date_time, "%Y/%m/%d")
        ids = [key for key in self.table_primary_keys if key not in ["EFFECTIVEDATE", "VERSIONNO"]]
        return (
            self.scan()
            .filter(pl.col("EFFECTIVEDATE") <= date)
            .sort(self.table_primary_keys)
            .unique(subset=ids, keep="last")
            .collect()
        )
