# NEMDB

Utilities to process data from the Australian [National Electricity Market](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb)


## Installation

Install the package with [uv](https://docs.astral.sh/uv/).


## CLI

The Command Line Interface (CLI) allows you to fetch market data from the monthly archives.

Get help:
`uv run populate --help`

Example, population all tables from 2024/01/01 to 2024/03/31:
`uv run populate --location ./nemweb_data --date_range 2024-01-01->2024-03-31`