import pandera as pa
import pandera.polars as papl

import polars as pl


class LoadSchema(papl.DataFrameModel):
    class Config:
        add_missing_columns = True

    zss: str = pa.Field(description="Zone Substation identifier")
    name: str = pa.Field(description="Zone Substation name", nullable=True)
    time: pl.Datetime = pa.Field(description="Time")
    mw: pl.Float32 = pa.Field(
        description="Active Load in MW", nullable=True, coerce=True
    )
    mvar: pl.Float32 = pa.Field(
        description="Reactive Load in MVAR", nullable=True, coerce=True
    )
    mva: pl.Float32 = pa.Field(
        description="Apparent Load in MVA", nullable=True, coerce=True
    )
