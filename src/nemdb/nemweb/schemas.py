"""Pandera schemas for NEMWEB database tables.

These schemas document the expected column types and structure for each AEMO MMS
table as returned by the corresponding DataSource.get_data() method.

All schemas use pandera.polars.DataFrameModel for native Polars support and IDE
type hinting. Fields are marked Optional since the _archive_to_df function fills
missing columns with null values.
"""

from typing import Optional  # noqa: F401

import pandera.polars as pa  # noqa: F401
import polars as pl  # noqa: F401


# Dispatch Tables
# ===============
