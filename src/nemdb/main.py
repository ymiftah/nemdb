import click


from pathlib import Path

from nemdb import Config
from nemdb.nemweb import NEMWEBManager
from datetime import datetime


@click.command()
@click.option(
    "--location",
    prompt="location",
    help="Where to write the data.",
    default=Path.home() / ".nemweb_cache",
)
@click.option("--filesystem", default="file", help="filesystem to use")
@click.option(
    "--date_range",
    prompt="Date",
    help='The date to fetch data for: "%Y-%m-%d->%Y-%m-%d"',
)
@click.option("--table", prompt="Table", help="Which table to load", default="all")
@click.option("--force_new", is_flag=True)
def populate(location, filesystem, date_range, table, force_new):
    click.echo(f"Fetching data for {date_range} to {location}")
    from_date, to_date = date_range.split("->")
    from_date = datetime.strptime(from_date.strip(), "%Y-%m-%d")
    to_date = datetime.strptime(to_date.strip(), "%Y-%m-%d")

    Config.set_cache_dir(location)
    Config.set_filesystem(filesystem)
    dbs = NEMWEBManager(Config)
    if table == "all":
        dbs.populate(slice(from_date, to_date), force_new=force_new)
    else:
        db_table = getattr(dbs, table)
        db_table.populate(slice(from_date, to_date), force_new=force_new)
