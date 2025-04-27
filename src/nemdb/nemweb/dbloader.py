"""
The code below is largely inspired by Gorman et al., (2022). Nempy: A Python package for modelling the Australian National Electricity Market dispatch procedure.
Journal of Open Source Software, 7(70), 3596, https://doi.org/10.21105/joss.03596

The NEMWEBManager class is a wrapper around the DataSource class and its interface should be compatible with nempy.
This offers a compressed parquet based backend for the MMSDM tables instead of the incompressed SQLite tables
used in nempy.
"""

from contextlib import suppress
from functools import lru_cache
import polars as pl
import pandas as pd
import fsspec

from datetime import datetime

from tqdm import tqdm

from nemdb import log as logger
from .utils import cache_response_zip
from .nemweb import read_bids

from nemdb import Config
from nemdb.dnsp import DNSPDataSource


URL = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month:02d}010000.zip"
URL_ALT = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE%23{table}%23FILE01%23{year}{month:02d}010000.zip"

STRPTIME = "%Y/%m/%d %H:%M:%S"
DTYPES = {
    "ENTRYTYPE": pl.Categorical,
    "NORMALSTATUS": pl.String,
    "PARTICIPANTID": pl.Categorical,
    "DIRECTION": pl.Categorical,
    "DAILYENERGYCONSTRAINT": pl.Float32,
    "INTERVAL_DATETIME": pl.Datetime,
    "LASTCHANGED": pl.Datetime,
    "DUID": pl.Categorical,
    "BIDTYPE": pl.Categorical,
    "BANDAVAIL1": pl.Float32,
    "BANDAVAIL2": pl.Float32,
    "BANDAVAIL3": pl.Float32,
    "BANDAVAIL4": pl.Float32,
    "BANDAVAIL5": pl.Float32,
    "BANDAVAIL6": pl.Float32,
    "BANDAVAIL7": pl.Float32,
    "BANDAVAIL8": pl.Float32,
    "BANDAVAIL9": pl.Float32,
    "BANDAVAIL10": pl.Float32,
    "MAXAVAIL": pl.Float32,
    "ENABLEMENTMIN": pl.Float32,
    "ENABLEMENTMAX": pl.Float32,
    "LOWBREAKPOINT": pl.Float32,
    "HIGHBREAKPOINT": pl.Float32,
    "SETTLEMENTDATE": pl.Datetime,
    "PRICEBAND1": pl.Float32,
    "PRICEBAND2": pl.Float32,
    "PRICEBAND3": pl.Float32,
    "PRICEBAND4": pl.Float32,
    "PRICEBAND5": pl.Float32,
    "PRICEBAND6": pl.Float32,
    "PRICEBAND7": pl.Float32,
    "PRICEBAND8": pl.Float32,
    "PRICEBAND9": pl.Float32,
    "PRICEBAND10": pl.Float32,
    "T1": pl.Float32,
    "T2": pl.Float32,
    "T3": pl.Float32,
    "T4": pl.Float32,
    "REGIONID": pl.Categorical,
    "TOTALDEMAND": pl.Float32,
    "DEMANDFORECAST": pl.Float32,
    "INITIALSUPPLY": pl.Float32,
    "SS_SOLAR_AVAILABILITY": pl.Float32,
    "SS_WIND_AVAILABILITY": pl.Float32,
    "DISPATCHMODE": pl.Int8,
    "AGCSTATUS": pl.Int8,
    "INITIALMW": pl.Float32,
    "TOTALCLEARED": pl.Float32,
    "RAMPDOWNRATE": pl.Float32,
    "ROCUP": pl.Float32,
    "ROCDOWN": pl.Float32,
    "RAMPUPRATE": pl.Float32,
    "AVAILABILITY": pl.Float32,
    "RAISEREGENABLEMENTMAX": pl.Float32,
    "RAISEREGENABLEMENTMIN": pl.Float32,
    "LOWERREGENABLEMENTMAX": pl.Float32,
    "LOWERREGENABLEMENTMIN": pl.Float32,
    "START_DATE": pl.Date,
    "END_DATE": pl.Date,
    "DISPATCHTYPE": pl.Categorical,
    "CONNECTIONPOINTID": pl.Categorical,
    "TRANSMISSIONLOSSFACTOR": pl.Float32,
    "DISTRIBUTIONLOSSFACTOR": pl.Float32,
    "CONSTRAINTID": pl.Categorical,
    "RHS": pl.Float32,
    "GENCONID_EFFECTIVEDATE": pl.Date,
    "GENCONID_VERSIONNO": pl.Int32,
    "GENCONID": pl.Categorical,
    "GENSETID": pl.Categorical,
    "CO2E_ENERGY_SOURCE": pl.String,
    "CO2E_EMISSIONS_FACTOR": pl.Float32,
    "CO2E_DATA_SOURCE": pl.String,
    "GENSETNAME": pl.String,
    "GENSETTYPE": pl.String,
    "STARTTYPE": pl.String,
    "VOLTLEVEL": pl.Float32,
    "STATIONID": pl.String,
    "REGISTEREDMINCAPACITY": pl.Float32,
    "MINCAPACITY": pl.Float32,
    "EFFECTIVEDATE": pl.Date,
    "VERSIONNO": pl.Int32,
    "CONSTRAINTTYPE": pl.Categorical,
    "GENERICCONSTRAINTWEIGHT": pl.Float32,
    "FACTOR": pl.Float32,
    "FROMREGIONLOSSSHARE": pl.Float32,
    "LOSSCONSTANT": pl.Float32,
    "LOSSFLOWCOEFFICIENT": pl.Float32,
    "IMPORTLIMIT": pl.Float32,
    "EXPORTLIMIT": pl.Float32,
    "LOSSSEGMENT": pl.Int32,
    "MWBREAKPOINT": pl.Float32,
    "DEMANDCOEFFICIENT": pl.Float32,
    "INTERCONNECTORID": pl.Categorical,
    "REGIONFROM": pl.Categorical,
    "REGIONTO": pl.Categorical,
    "MWFLOW": pl.Float32,
    "MWLOSSES": pl.Float32,
    "MINIMUMLOAD": pl.Float32,
    "MAXCAPACITY": pl.Float32,
    "SEMIDISPATCHCAP": pl.Float32,
    "RRP": pl.Float32,
    "SCHEDULE_TYPE": pl.Categorical,
    "LOWER5MIN": pl.Float32,
    "LOWER60SEC": pl.Float32,
    "LOWER6SEC": pl.Float32,
    "LOWER1SEC": pl.Float32,
    "RAISE5MIN": pl.Float32,
    "RAISE60SEC": pl.Float32,
    "RAISE6SEC": pl.Float32,
    "RAISE1SEC": pl.Float32,
    "LOWERREG": pl.Float32,
    "RAISEREG": pl.Float32,
    "ENERGYLIMIT": pl.Float32,
    "MAX_RAMP_RATE_DOWN": pl.Float32,
    "MAX_RAMP_RATE_UP": pl.Float32,
    "MIN_RAMP_RATE_UP": pl.Float32,
    "MIN_RAMP_RATE_DOWN": pl.Float32,
    "IS_AGGREGATED": pl.Boolean,
    "RAISEREGAVAILABILITY": pl.Float32,
    "RAISE6SECACTUALAVAILABILITY": pl.Float32,
    "RAISE1SECACTUALAVAILABILITY": pl.Float32,
    "RAISE60SECACTUALAVAILABILITY": pl.Float32,
    "RAISE5MINACTUALAVAILABILITY": pl.Float32,
    "RAISEREGACTUALAVAILABILITY": pl.Float32,
    "LOWER6SECACTUALAVAILABILITY": pl.Float32,
    "LOWER1SECACTUALAVAILABILITY": pl.Float32,
    "LOWER60SECACTUALAVAILABILITY": pl.Float32,
    "LOWER5MINACTUALAVAILABILITY": pl.Float32,
    "LOWERREGACTUALAVAILABILITY": pl.Float32,
    "UIGF": pl.Float32,
    "LHS": pl.Float32,
    "VIOLATIONDEGREE": pl.Float32,
    "MARGINALVALUE": pl.Float32,
    "RAISE6SECROP": pl.Float32,
    "RAISE1SECROP": pl.Float32,
    "RAISE60SECROP": pl.Float32,
    "RAISE5MINROP": pl.Float32,
    "RAISEREGROP": pl.Float32,
    "LOWER6SECROP": pl.Float32,
    "LOWER1SECROP": pl.Float32,
    "LOWER60SECROP": pl.Float32,
    "LOWER5MINROP": pl.Float32,
    "LOWERREGROP": pl.Float32,
    "FROM_REGION_TLF": pl.Float32,
    "TO_REGION_TLF": pl.Float32,
    "ICTYPE": pl.Categorical,
    "LINKID": pl.Categorical,
    "FROMREGION": pl.Categorical,
    "TOREGION": pl.Categorical,
    "REGISTEREDCAPACITY": pl.Float32,
    "LHSFACTOR": pl.Float32,
    "ROP": pl.Float32,
    "CASESUBTYPE": pl.Categorical,
    "SOLUTIONSTATUS": pl.Int8,
    "INTERVENTION": pl.Int8,
    "TOTALOBJECTIVE": pl.Float32,
    "TOTALAREAGENVIOLATION": pl.Float32,
    "TOTALINTERCONNECTORVIOLATION": pl.Float32,
    "TOTALGENERICVIOLATION": pl.Float32,
    "TOTALRAMPRATEVIOLATION": pl.Float32,
    "TOTALUNITMWCAPACITYVIOLATION": pl.Float32,
    "TOTAL5MINVIOLATION": pl.Float32,
    "TOTALREGVIOLATION": pl.Float32,
    "TOTAL6SECVIOLATION": pl.Float32,
    "TOTAL60SECVIOLATION": pl.Float32,
    "TOTALASPROFILEVIOLATION": pl.Float32,
    "TOTALFASTSTARTVIOLATION": pl.Float32,
    "TOTALENERGYOFFERVIOLATION": pl.Float32,
    "FIXEDLOAD": pl.Float32,
}


class NEMWEBManager:
    """Interface for accessing historical inputs for NEM spot market dispatch (NEMDE).

    Populates a directory with parquet datasets.

    Examples
    --------

    >>> historical = NEMWEB(source='gs://nemweb-historical')

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

    def __init__(self, config: Config):
        self.config = config
        self._active_tables = [
            "DISPATCHREGIONSUM",
            "BIDDAYOFFER_D",
            "BIDPEROFFER_D",
            "DUDETAILSUMMARY",
            "DUDETAIL",
            "GENUNITS",
            "DISPATCHLOAD",
            "DISPATCHREGIONSUM",
            "DISPATCHPRICE",
            "MNSP_INTERCONNECTOR",
            "RESERVE",
            "ZONE_SUBSTATION",
        ]
        self.ZONE_SUBSTATION = DNSPDataSource(
            config=config,
            table_name="ZONE_SUBSTATION",
            add_partitions=["network"],
            table_primary_keys=["zss", "time"],
            table_columns=[
                "time",
                "zss",
                "MW",
                "network",
            ],
        )
        self.GENUNITS = DataSource(
            config=config,
            table_name="GENUNITS",
            table_columns=[
                "GENSETID",
                "STATIONID",
                "VOLTLEVEL",
                "DISPATCHTYPE",
                "STARTTYPE",
                "NORMALSTATUS",
                "MAXCAPACITY",
                "GENSETTYPE",
                "GENSETNAME",
                "LOWERREG",
                "CO2E_EMISSIONS_FACTOR",
                "CO2E_ENERGY_SOURCE",
                "CO2E_DATA_SOURCE",
                "MINCAPACITY",
                "REGISTEREDMINCAPACITY",
                "LASTCHANGED",
            ],
            table_primary_keys=["STATIONID", "LASTCHANGED"],
        )
        self.RESERVE = BySettlementDate(
            config=config,
            table_name="RESERVE",
            table_columns=[
                "SETTLEMENTDATE",
                "VERSIONNO",
                "REGIONID",
                "PERIODID",
                "LOWER5MIN",
                "RAISE5MIN",
                "RAISEREG",
                "LOWERREG",
            ],
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
        )
        self.DISPATCHREGIONSUM = BySettlementDate(
            config=config,
            table_name="DISPATCHREGIONSUM",
            table_columns=[
                "SETTLEMENTDATE",
                "REGIONID",
                "TOTALDEMAND",
                "DEMANDFORECAST",
                "INITIALSUPPLY",
                "SS_SOLAR_AVAILABILITY",
                "SS_WIND_AVAILABILITY",
            ],
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
        )
        self.DISPATCHLOAD = BySettlementDate(
            config=config,
            table_name="DISPATCHLOAD",
            table_columns=[
                "SETTLEMENTDATE",
                "DUID",
                "DISPATCHMODE",
                "AGCSTATUS",
                "INITIALMW",
                "TOTALCLEARED",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
                "AVAILABILITY",
                "RAISEREGENABLEMENTMAX",
                "RAISEREGENABLEMENTMIN",
                "LOWERREGENABLEMENTMAX",
                "LOWERREGENABLEMENTMIN",
                "SEMIDISPATCHCAP",
                "LOWER5MIN",
                "LOWER60SEC",
                "LOWER6SEC",
                "LOWER1SEC",
                "RAISE5MIN",
                "RAISE60SEC",
                "RAISE6SEC",
                "RAISE1SEC",
                "LOWERREG",
                "RAISEREG",
                "RAISEREGAVAILABILITY",
                "RAISE6SECACTUALAVAILABILITY",
                "RAISE1SECACTUALAVAILABILITY",
                "RAISE60SECACTUALAVAILABILITY",
                "RAISE5MINACTUALAVAILABILITY",
                "RAISEREGACTUALAVAILABILITY",
                "LOWER6SECACTUALAVAILABILITY",
                "LOWER1SECACTUALAVAILABILITY",
                "UIGF",
            ],
            table_primary_keys=["SETTLEMENTDATE", "DUID"],
            add_partitions=["DUID"],
        )
        self.DISPATCHPRICE = BySettlementDate(
            config=config,
            table_name="DISPATCHPRICE",
            table_columns=[
                "SETTLEMENTDATE",
                "REGIONID",
                "RRP",
                "ROP",
                "RAISE6SECROP",
                "RAISE1SECROP",
                "RAISE60SECROP",
                "RAISE5MINROP",
                "RAISEREGROP",
                "LOWER6SECROP",
                "LOWER1SECROP",
                "LOWER60SECROP",
                "LOWER5MINROP",
                "LOWERREGROP",
            ],
            table_primary_keys=["SETTLEMENTDATE", "REGIONID"],
        )
        self.DUDETAILSUMMARY = ByStartEnd(
            config=config,
            table_name="DUDETAILSUMMARY",
            table_columns=[
                "DUID",
                "START_DATE",
                "END_DATE",
                "DISPATCHTYPE",
                "CONNECTIONPOINTID",
                "REGIONID",
                "TRANSMISSIONLOSSFACTOR",
                "DISTRIBUTIONLOSSFACTOR",
                "SCHEDULE_TYPE",
                "MIN_RAMP_RATE_UP",
                "MIN_RAMP_RATE_DOWN",
                "MAX_RAMP_RATE_UP",
                "MAX_RAMP_RATE_DOWN",
                "IS_AGGREGATED",
            ],
            table_primary_keys=["END_DATE", "REGIONID", "DUID"],
        )
        self.DUDETAIL = ByEffectiveDateVersionNo(
            config=config,
            table_name="DUDETAIL",
            table_columns=[
                "DUID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "REGISTEREDCAPACITY",
                "MAXCAPACITY",
            ],
            table_primary_keys=["VERSIONNO", "DUID"],
        )
        self.BIDDAYOFFER_D = BySettlementDate(
            config=config,
            table_name="BIDDAYOFFER_D",
            table_columns=[
                "DUID",
                "SETTLEMENTDATE",
                "BIDTYPE",
                "DIRECTION",
                "VERSIONNO",
                "PARTICIPANTID",
                "DAILYENERGYCONSTRAINT",
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
                "MINIMUMLOAD",
                "T1",
                "T2",
                "T3",
                "T4",
                "NORMALSTATUS",
                "ENTRYTYPE",
            ],
            table_primary_keys=["VERSIONNO", "DUID"],
        )
        self.BIDPEROFFER_D = BySettlementDate(
            config=config,
            table_name="BIDPEROFFER_D",
            table_columns=[
                "DUID",
                "SETTLEMENTDATE",
                "BIDTYPE",
                "DIRECTION",
                "VERSIONNO",
                "INTERVAL_DATETIME",
                "MAXAVAIL",
                "FIXEDLOAD",
                "ROCUP",
                "ROCDOWN",
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
                "ENERGYLIMIT",
                "LASTCHANGED",
            ],
            table_primary_keys=["SETTLEMENTDATE", "DUID"],
            low_memory=True,
        )
        self.DISPATCHCONSTRAINT = BySettlementDate(
            config=config,
            table_name="DISPATCHCONSTRAINT",
            table_columns=[
                "SETTLEMENTDATE",
                "CONSTRAINTID",
                "DUID",
                "RHS",
                "GENCONID_EFFECTIVEDATE",
                "GENCONID_VERSIONNO",
                "LHS",
                "VIOLATIONDEGREE",
                "MARGINALVALUE",
            ],
            table_primary_keys=["SETTLEMENTDATE", "CONSTRAINTID"],
        )
        self.GENCONDATA = ByEffectiveDateVersionNo(
            config=config,
            table_name="GENCONDATA",
            table_columns=[
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "CONSTRAINTTYPE",
                "GENERICCONSTRAINTWEIGHT",
            ],
            table_primary_keys=["GENCONID", "EFFECTIVEDATE", "VERSIONNO"],
        )
        self.SPDREGIONCONSTRAINT = ByEffectiveDateVersionNo(
            config=config,
            table_name="SPDREGIONCONSTRAINT",
            table_columns=[
                "REGIONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "GENCONID",
                "BIDTYPE",
                "FACTOR",
            ],
            table_primary_keys=[
                "REGIONID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "BIDTYPE",
            ],
        )
        self.SPDCONNECTIONPOINTCONSTRAINT = ByEffectiveDateVersionNo(
            config=config,
            table_name="SPDCONNECTIONPOINTCONSTRAINT",
            table_columns=[
                "CONNECTIONPOINTID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "GENCONID",
                "BIDTYPE",
                "FACTOR",
            ],
            table_primary_keys=[
                "CONNECTIONPOINTID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "BIDTYPE",
            ],
        )
        self.SPDINTERCONNECTORCONSTRAINT = ByEffectiveDateVersionNo(
            config=config,
            table_name="SPDINTERCONNECTORCONSTRAINT",
            table_columns=[
                "INTERCONNECTORID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "GENCONID",
                "FACTOR",
            ],
            table_primary_keys=[
                "INTERCONNECTORID",
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
            ],
        )
        self.INTERCONNECTOR = ByEffectiveDateVersionNo(
            config=config,
            table_name="INTERCONNECTOR",
            table_columns=["INTERCONNECTORID", "REGIONFROM", "REGIONTO"],
            table_primary_keys=["INTERCONNECTORID"],
        )
        self.INTERCONNECTORCONSTRAINT = ByEffectiveDateVersionNo(
            config=config,
            table_name="INTERCONNECTORCONSTRAINT",
            table_columns=[
                "INTERCONNECTORID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "FROMREGIONLOSSSHARE",
                "LOSSCONSTANT",
                "ICTYPE",
                "LOSSFLOWCOEFFICIENT",
                "IMPORTLIMIT",
                "EXPORTLIMIT",
            ],
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
        )
        self.LOSSMODEL = ByEffectiveDateVersionNo(
            config=config,
            table_name="LOSSMODEL",
            table_columns=[
                "INTERCONNECTORID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "LOSSSEGMENT",
                "MWBREAKPOINT",
            ],
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
        )
        self.LOSSFACTORMODEL = ByEffectiveDateVersionNo(
            config=config,
            table_name="LOSSFACTORMODEL",
            table_columns=[
                "INTERCONNECTORID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "REGIONID",
                "DEMANDCOEFFICIENT",
            ],
            table_primary_keys=["INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
        )
        self.DISPATCHINTERCONNECTORRES = BySettlementDate(
            config=config,
            table_name="DISPATCHINTERCONNECTORRES",
            table_columns=["INTERCONNECTORID", "SETTLEMENTDATE", "MWFLOW", "MWLOSSES"],
            table_primary_keys=["INTERCONNECTORID", "SETTLEMENTDATE"],
        )
        self.MNSP_INTERCONNECTOR = ByEffectiveDateVersionNo(
            config=config,
            table_name="MNSP_INTERCONNECTOR",
            table_columns=[
                "INTERCONNECTORID",
                "LINKID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "FROMREGION",
                "TOREGION",
                "FROM_REGION_TLF",
                "TO_REGION_TLF",
                "LHSFACTOR",
                "MAXCAPACITY",
            ],
            table_primary_keys=[
                "INTERCONNECTORID",
                "LINKID",
                "EFFECTIVEDATE",
                "VERSIONNO",
            ],
        )

    def __repr__(self):
        source = self.config.CACHE_DIR
        return "\n".join(
            (
                f"DBManager at {source}, with tables:",
                *[f"-- {table}" for table in self.tables if table != "source"],
            )
        )

    @property
    def tables(self):
        return list(v for v in vars(self) if v != "source")

    def active_tables(self):
        """
        Retrieve the list of active tables in the database.

        This method returns a list of table names that are currently active
        and available for data processing and population in the database.
        """
        return self._active_tables

    def populate(self, date_slice: slice, force_new: bool = False):
        """Fetch data for all active tables and populate the parquet datasets."""
        logger.info(
            "Populating database with data from %s to %s",
            date_slice.start,
            date_slice.stop,
        )
        with (
            pl.StringCache()
        ):  # Ensures consistent Categorical values across all tables
            for table in tqdm(self.active_tables()):
                table_: DataSource = getattr(self, table)
                table_.populate(date_slice, force_new=force_new)

    @staticmethod
    @lru_cache(maxsize=4)
    def read_bids(year: int, month: int, day: int):
        """Read bid data for a specific date

        NOTE This method reads data for the specific date, zips are cached locally
        But not persisted in the database yet because of the size.
        """
        return read_bids(year, month, day)

    def get_unit_volume_bids(self, date: str):
        """Get unit volume bids for a specific date"""
        date = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        _, volume = self.read_bids(date.year, date.month, date.day)
        return volume.with_columns(
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

    def get_unit_price_bids(self, date):
        date = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        price, _ = self.read_bids(date.year, date.month, date.day)
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
        except ValueError:
            raise _MissingData(
                (
                    """Requested data for table: {}, year: {}, month: {}
                                not downloaded. Please check your internet connection. Also check
                                http://nemweb.com.au/#mms-data-model, to see if your requested
                                data is uploaded."""
                ).format(table_name, year, month)
            )
    return r


def _archive_to_df(
    archive: str,
    table_columns: list[str],
    year: int,
    month: int,
    low_memory: bool = False,
):
    """Downloads a zipped csv file and converts it to a pandas DataFrame, returns the DataFrame.

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
    table_dtypes = {
        k: DTYPES[k] for k in set(table_columns).intersection(available_cols)
    }

    data = pd.read_csv(
        archive,
        skiprows=1,
        usecols=table_dtypes,
        # dtype=table_dtypes,
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
    data = data.assign(**{col: None for col in missing_columns})
    date_types = [k for k in table_dtypes if table_dtypes[k] in (pl.Date, pl.Datetime)]
    for col in date_types:
        data[col] = pd.to_datetime(data[col], format=STRPTIME, errors="coerce")
    # Discard last row of DataFrame
    data = data[:-1]
    return pl.from_dataframe(data).cast({k: DTYPES[k] for k in set(table_columns)})


def read_header(file: str):
    """Returns the set of columns in the file"""
    return set(pd.read_csv(file, skiprows=1, nrows=1).columns)


class _MissingData(Exception):
    """Raise for nemweb not returning status 200 for file request."""


class DataSource:
    """Manages Market Management System (MMS) tables stored as parquet files.

    This class creates the dataset when the object is instantiated. Methods for adding
    and retrieving data are added by sub classing.
    """

    def __init__(
        self,
        config: Config,
        table_name: str,
        table_columns: list[str],
        table_primary_keys: list[str] = None,
        add_partitions: bool = None,
        low_memory: bool = False,
    ):
        """Creates a parquet dataset."""
        self.config = config
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        self.partitions = (
            add_partitions + ["year", "month"] if add_partitions else ["year", "month"]
        )
        self.low_memory = low_memory

        self.path = f"{config.CACHE_DIR}/{table_name}/"
        self.fs = fsspec.filesystem(config.FILESYSTEM)
        self.fs.makedirs(f"{config.CACHE_DIR}/{table_name}", exist_ok=True)

    def scan(self, *args, **kwargs):
        """scans the parquet dataset with polars"""
        kwargs_ = {"hive_partitioning": True, "allow_missing_columns": True}
        kwargs_.update(kwargs if kwargs is None else {})
        return pl.scan_parquet(self.path, *args, **kwargs_)

    def read(self, *args, **kwargs):
        """Reads the parquet dataset with polars

        NOTE provided for compatibility with nempy, should be avoided as it can read massive data
        in memory.
        """
        return self.scan(self, *args, **kwargs).collect()

    def populate(self, date_slice: slice, force_new: bool = False):
        """Adds data to the parquet dataset from a date range."""
        date_range = pd.date_range(
            start=date_slice.start, end=date_slice.stop, freq="MS"
        )
        logger.info(
            "Populating database with data from %s to %s", date_range[0], date_range[-1]
        )
        with (
            pl.StringCache()
        ):  # Ensures consistent Categorical values across all tables
            for date in tqdm(date_range):
                year = date.year
                month = date.month
                # Check if data already exists in tables before adding
                data_exists = False
                if not force_new:
                    with suppress(
                        pl.exceptions.ComputeError, FileNotFoundError, Exception
                    ):
                        logger.info(
                            "Checking if data already exists for %s %s / %s",
                            self.table_name,
                            year,
                            month,
                        )
                        check = (
                            self.scan()
                            .filter(pl.col("year") == year, pl.col("month") == month)
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
        partition_cols = self.partitions

        if self.low_memory:
            self._add_data_low_memory(year, month, name, **kwargs)
            return

        try:
            data = (
                self.fetch_data(year, month)
                .with_columns(
                    pl.lit(year, pl.Int32).alias("year"),
                    pl.lit(month, pl.Int8).alias("month"),
                )
                .sort(partition_cols + self.table_primary_keys)
            )
        except _MissingData:
            logger.error(
                "No data available for %s %s / %s", self.table_name, year, month
            )
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
                archive, name, self.table_columns, year, month, self.path, **kwargs
            )
            return None
        except _MissingData:
            logger.error(
                "No data available for %s %s / %s", self.table_name, year, month
            )
            return

    def _archive_to_df_low_memory(
        self, archive, name, table_columns, year, month, path, **kwargs
    ):
        partition_cols = self.partitions

        # Read the file into a DataFrame.
        available_cols = read_header(archive)
        table_dtypes = {
            k: DTYPES[k] for k in set(table_columns).intersection(available_cols)
        }
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
            usecols=table_dtypes,
            chunksize=1_000_000,
            # dtype=table_dtypes,
        )
        for j, data in enumerate(reader):
            data = data.assign(**{col: None for col in missing_columns})
            date_types = [
                k for k in table_dtypes if table_dtypes[k] in (pl.Date, pl.Datetime)
            ]
            for col in date_types:
                data[col] = pd.to_datetime(data[col], format=STRPTIME, errors="coerce")

            data = (
                pl.from_dataframe(data)
                .cast({k: DTYPES[k] for k in set(table_columns)})
                .with_columns(
                    pl.lit(year, pl.Int32).alias("year"),
                    pl.lit(month, pl.Int8).alias("month"),
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

    def fetch_data(self, year, month):
        logger.info("Fetching data for %s %s / %s", self.table_name, year, month)
        archive = _get_archive(self.table_name, year, month)
        return _archive_to_df(
            archive, self.table_columns, year, month, low_memory=self.low_memory
        )

    def get_data(self):
        return self.read()


class BySettlementDate(DataSource):
    def get_data(self, date_time):
        date_time = datetime.strptime(date_time, "%Y/%m/%d %H:%M:%S")
        return self.scan().filter(pl.col("SETTLEMENTDATE") == date_time).collect()


class ByIntervalDate(DataSource):
    def get_data(self, date_time):
        date_time = datetime.strptime(date_time, "%Y/%m/%d %H:%M:%S")
        return self.scan().filter(pl.col("INTERVAL_DATETIME") == date_time).collect()


class BySettlementDay(DataSource):
    def get_data(self, date_time):
        # Convert to datetime object
        date_time = datetime.strptime(date_time, "%Y/%m/%d")
        # Change date_time provided so any time less than 04:05:00 will have the previous days date.
        date_time = date_time - datetime.timedelta(hours=4, seconds=1)
        # Convert to date
        date_time = date_time.date()
        return self.scan().filter(pl.col("SETTLEMENTDATE") == date_time).collect()


class ByStartEnd(DataSource):
    def get_data(self, date_time):
        date_time = datetime.strptime(date_time, "%Y/%m/%d")
        return (
            self.scan()
            .filter(
                (pl.col("START_DATE") <= date_time)
                & (pl.col("END_DATE").is_null() | (pl.col("END_DATE") >= date_time))
            )
            .collect()
        )


class ByEffectiveDateVersionNo(DataSource):
    def get_data(self, date_time):
        date_time = datetime.strptime(date_time, "%Y/%m/%d")
        ids = [
            key
            for key in self.table_primary_keys
            if key not in ["EFFECTIVEDATE", "VERSIONNO"]
        ]
        return (
            self.scan()
            .filter((pl.col("EFFECTIVEDATE") <= date_time))
            .sort(self.table_primary_keys)
            .unique(subset=ids, keep="last")
            .collect()
        )
