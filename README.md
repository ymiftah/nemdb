# NEMDB

[![CI](https://github.com/ymiftah/nemdb/actions/workflows/ci.yml/badge.svg)](https://github.com/ymiftah/nemdb/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ymiftah/nemdb/branch/main/graph/badge.svg)](https://codecov.io/gh/ymiftah/nemdb)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-github%20pages-blue)](https://ymiftah.github.io/nemdb)

NEMDB is a Python package that provides a set of utilities to process and
manage data from the Australian National Electricity Market (NEM). It
simplifies the process of fetching, storing, and accessing NEMWEB data,
making it easier for researchers, analysts, and developers to work with this
valuable dataset.

## Features

- **Easy Data Fetching**: NEMDB provides a simple command-line interface
  (CLI) to fetch data from NEMWEB for a specified date range.
- **Flexible Storage**: The fetched data can be stored in a local directory
  or any other location supported by `fsspec`.
- **Selective Table Loading**: Users can choose to load all available tables
  or select specific tables to populate.

## Getting Started

### Prerequisites

- Python 3.13 or higher
- [uv](https://docs.astral.sh/uv/)

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/ymiftah/nemdb.git
    ```

2. Install the package with `uv`:

    ```bash
    uv pip install -e .
    ```

## Usage

### Command-Line Interface (CLI)

The primary way to interact with NEMDB is through its CLI. The `populate`
command allows you to fetch and store NEMWEB data.

#### Getting Help

To see a list of all available options for the `populate` command, run:

```bash
uv run populate --help
```

#### Populating All Tables

To populate all available tables for a specific date range, run the
following command:

```bash
uv run populate --location ./nemweb_data --date_range 2024-01-01->2024-03-31
```

This command will fetch all NEMWEB data from January 1, 2024, to March 31,
2024, and store it in the `./nemweb_data` directory.

#### Populating a Specific Table

To populate a single table, use the `--table` option. For example, to
populate the `DISPATCHREGIONSUM` table, run:

```bash
uv run populate --location ./nemweb_data --date_range 2024-01-01->2024-03-31 \
  --table DISPATCHREGIONSUM
```

### Available Tables

NEMDB provides access to a wide range of tables from the NEMWEB dataset.

For a full list of available tables, please refer to the
[AEMO NEMWEB documentation](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb).

## License

This project is licensed under the terms of the MIT license. See the
[LICENSE](LICENSE) file for more details. The data itself is subject to
AEMO's
[Privacy and Legal Notice](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb)
