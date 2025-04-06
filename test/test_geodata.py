import pytest
from nemdb.geodata import transformations

import shapely as shp


@pytest.mark.parametrize(
    "coords, result",
    [
        pytest.param(
            [
                (0, 0),
                (0, 1),
            ],
            (0, 0),
        ),
        pytest.param(
            [
                (0, 0),
                (0, 1),
                (2, 2),
            ],
            (2, 2),
        ),
        pytest.param(
            [
                (0, 0),
                (0, 1),
                (2, 2),
            ],
            (2, 2),
        ),
    ],
)
def test_furthest_point(coords, result):
    points = shp.points(coords)
    assert transformations._get_furthest_closest_point(points) == shp.Point(result)
