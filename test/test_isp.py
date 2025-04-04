import pytest
from nemdb.isp import isp


@pytest.mark.parametrize(
    "func",
    [
        isp.read_coal_prices,
    ],
)
def test_read_functions(func):
    _ = func()
    assert True
