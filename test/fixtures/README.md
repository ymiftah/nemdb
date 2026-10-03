# Julia NEMWEB schema fixtures

`julia_nemweb_schemas.json` records the shared NEMWEB storage contract independently
of Python's Pandera models. Its provenance identifies the Julia revision and the
cache inspection date.

The `columns` mappings come from the DuckDB-written Parquet footers, checked against
Julia's explicit `COLUMN_TYPES` entries. `RESERVE`, which has no cached files, uses
the Julia table specification and type mappings. FCAS tables and additional columns
absent from the current Julia table specifications use the existing cache metadata.
The `source_columns` lists record Julia's current table projections, allowing tests
to exercise partitions containing fewer columns than the cache schema.

When the Julia definitions or shared cache schema change, update this snapshot from
those sources, not from Python's models. Tests build synthetic rows from the snapshot
and exercise validation, CSV parsing, and Parquet reads without requiring Julia or
the user's live cache.
