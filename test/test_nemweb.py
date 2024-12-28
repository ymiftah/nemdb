import pytest
from datetime import datetime, timedelta

from nemdb import Config
from nemdb.nemweb.dbloader import NEMWEBManager


def __select_date():
    date = datetime.today() - timedelta(days=31)  # at least check a month before
    return date.year, date.month, date.day


@pytest.mark.parametrize(
    "date",
    [
        (2024, 1, 1),
        (2023, 10, 1),
        (*__select_date(),),
    ],
)
def test_db_dispatch_load(date):
    year, month, _ = date

    pds = NEMWEBManager(Config.CACHE_DIR)
    pds.DISPATCHLOAD.add_data(year, month)
    assert pds.DISPATCHLOAD.scan().head().collect().shape[0] > 0
