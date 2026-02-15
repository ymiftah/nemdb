# Use ISP assumptions

nemdb bundles AEMO's Integrated System Plan (ISP) assumption workbooks and provides a convenient reader.

## Read ISP data

```python
from nemdb import ISPAssumptions

isp = ISPAssumptions()
print(isp)
```

This prints the list of available tables (Excel sheet names):

```text
ISP spreadsheet, with following tables :
 - Coal prices
 - Gas prices
 - ...
```

## List available tables

```python
print(isp.tables)
# ['Coal prices', 'Gas prices', ...]
```

## Read a specific table

```python
df = isp.read_table("Coal prices")
print(df.head())
```

Returns a Polars DataFrame with the contents of the named sheet.

## Read coal price scenarios

A convenience function parses the coal prices sheet into a tidy format:

```python
from nemdb.isp.isp import read_coal_prices

coal = read_coal_prices()
print(coal.head())
```

Output columns: `Generator`, `Coal Price Scenario`, `fy` (financial year), `price`.

## Use a different ISP file

By default, nemdb uses the bundled `ISP_2024.xlsx`. To use a different file:

```python
isp = ISPAssumptions("/path/to/ISP_2023.xlsx")
```

The bundled artefacts are located at:

- `src/nemdb/artefacts/ISP_2023.xlsx`
- `src/nemdb/artefacts/ISP_2024.xlsx`
