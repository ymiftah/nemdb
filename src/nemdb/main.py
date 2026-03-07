from datetime import datetime
from pathlib import Path

import click

from nemdb import Config
from nemdb.nemweb import NEMWEBManager


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
@click.option("--force_new", is_flag=False)
def populate(location, filesystem, date_range, table, force_new):
    click.echo(f"Fetching data for {date_range} to {location}")
    from_date, to_date = date_range.split("->")
    from_date = datetime.strptime(from_date.strip(), "%Y-%m-%d")
    to_date = datetime.strptime(to_date.strip(), "%Y-%m-%d")

    config = Config()
    config.set_cache_dir(location)
    config.set_filesystem(filesystem)
    dbs = NEMWEBManager(config)
    if table == "all":
        dbs.populate(slice(from_date, to_date), force_new=force_new)
    else:
        db_table = getattr(dbs, table)
        db_table.populate(slice(from_date, to_date), force_new=force_new)


if __name__ == "__main__":
    from_date, to_date = ["2025-01-01", "2025-01-02"]
    force_new = False
    config = Config()
    dbs = NEMWEBManager(config)
    dbs.populate(slice(from_date, to_date), force_new=force_new)
