
import click

from nemdb.nemweb import NEMWEBManager
from datetime import datetime

@click.command()
@click.option('--location', prompt="location", help='Where to write the data.')
@click.option('--date_range', prompt='Date', help='The date to fetch data for: "%Y-%m-%d->%Y-%m-%d"')
def populate(location, date_range):
    click.echo(f"Fetching data for {date_range} to {location}")
    from_date, to_date = date_range.split("->")
    from_date = datetime.strptime(from_date.strip(), "%Y-%m-%d")
    to_date = datetime.strptime(to_date.strip(), "%Y-%m-%d")

    dbs = NEMWEBManager(location)
    dbs.populate(slice(from_date, to_date))