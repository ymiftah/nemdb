from functools import partial
from itertools import chain

import pandas as pd
import geopandas as gpd
import shapely as shp
from shapely.ops import linemerge
from shapely import shortest_line


def _get_furthest_closest_point(list_points):
    """
    Given a list of shapely Point objects, return the point which is the furthest from its closest neighbour.
    """
    if len(list_points) == 1:
        return list_points[0]

    dist_dict = {
        p: min(p.distance(not_p) for not_p in list_points if p != not_p)
        for p in list_points
    }
    point = pd.Series(dist_dict).idxmax()
    return point


def _get_points(geom):
    """
    Extracts and returns a list of shapely Point objects from a given geometry.

    Args:
        geom: A shapely geometry object, which can be a LineString or a collection of geometries.

    Returns:
        A list of shapely Point objects representing the coordinates of the input geometry.
    """

    if type(geom) is shp.LineString:
        points = list(shp.points(geom.coords))
    else:
        points = list(chain.from_iterable(shp.points(g.coords) for g in geom.geoms))
    return points


def force_line(geom):
    """
    Given a shapely geometry object, forces it to be a line by choosing the sequence of points to traverse.

    The algorithm works as follows:
    1. Find all points on the boundary of the envelope of the geometry (i.e. the bounding box).
    2. Choose the point which is furthest from another closest point on the boundary as the starting point of the line.
    3. Segment the line into a large number of points (currently 500).
    4. Sort the points in order of their distance from the starting point.
    5. Iterate over the sorted points, adding each one to the line and then removing it from the list.
    6. Simplify the resulting line to remove redundant points.

    Args:
        geom: A shapely geometry object, which can be a LineString or a collection of geometries.

    Returns:
        A simplified shapely LineString object representing the traversed line.
    """
    geom = geom.simplify(tolerance=100)
    base_points = _get_points(geom)
    bbox = geom.envelope.boundary
    # get points on the boundary
    points_on_boundary = [p for p in base_points if p.within(bbox)]
    # Get starting point as the point furthest from any other point on the boundary
    starting_point = _get_furthest_closest_point(points_on_boundary)

    # Now increase number of points to reconstruct a continuous line
    points = _get_points(shp.segmentize(geom, 500))
    points = sorted(points, key=lambda p: p.distance(starting_point))
    last_point = points[0]
    line = [last_point]

    def skey(a, *, b):
        return a.distance(b)

    lines = []
    while len(points) > 0:
        # get the point closest to the last point added to the line
        points = sorted(points, key=partial(skey, b=last_point))
        last_point = points.pop(0)
        if last_point.distance(line[-1]) > 1000:
            # This can happen if the line goes off to one branch of the geometry
            # we finish the line and start a new one
            lines.append(shp.LineString(line))
            # Start from the closest point in the line
            starting_point = sorted(line, key=partial(skey, b=last_point))[0]
            line = [starting_point, last_point]
            starting_point = last_point
        else:
            line.append(last_point)
    lines.append(shp.LineString(line))
    output = lines[0] if len(lines) == 1 else shp.line_merge(shp.MultiLineString(lines))
    return output.simplify(tolerance=100)


def make_continuous(geometry, tol_dist=100):
    """
    Simplifies a given multiline string continuous by merging any nearby lines that are within a certain distance.
    This ensures all lines are contiguous.

    Args:
        geometry: A shapely geometry object, which can be a LineString or MultiLineString.

    Returns:
        A simplified shapely LineString object.
    """
    if type(geometry) is not shp.MultiLineString:
        return geometry

    # first try a line merge
    new_geometry = linemerge(geometry)

    if type(new_geometry) is shp.LineString:
        return new_geometry

    # if that doesn't work, try simplifying by merging lines that are within a certain distance
    simplified_geometry = new_geometry.simplify(tolerance=tol_dist)
    geometries = list(simplified_geometry.geoms)

    # start with the first geometry
    output = []
    merged = geometries.pop(0)
    while len(geometries) > 0:
        # sort and get the closest
        geometries = sorted(geometries, key=lambda g: g.distance(merged))
        try_geom = geometries.pop(0)
        if try_geom.distance(merged) < tol_dist:
            # the new geom is close enough, we add a link
            sl = shortest_line(merged, try_geom)
            merged = linemerge(shp.union_all((merged, sl, try_geom)))
        else:
            # No geoms are close enough, we extend the lines and will return a multi line
            output.append(merged)
            merged = try_geom

    output.append(merged)
    return (shp.union_all(output)).simplify(tolerance=tol_dist)


def clean_multilines(mls):
    """
    Cleans a given MultiLineString geometry by ensuring that all lines are contiguous.

    This function works by first forcing all lines to be traversed in a single direction (i.e. removing any redundant points).
    It then merges any nearby lines that are within a certain distance.

    Args:
        mls: A shapely MultiLineString or LineString object.

    Returns:
        A simplified shapely MultiLineString object.
    """
    if type(mls) is shp.LineString:
        return mls
    else:
        return shp.line_merge(force_line(mls))


def clean_transmission_lines(lines: gpd.GeoDataFrame):
    """
    Cleans a given GeoDataFrame of transmission lines by ensuring that all lines are contiguous.

    This function first simplifies each line by merges nearby lines that are within a certain distance, and then simplifying the geometry by
    approximating a single line through all points in the geometry.

    Args:
        lines: A GeoDataFrame containing transmission line data, with a "geometry" column containing the shapely LineString objects.

    Returns:
        A new GeoDataFrame with the same columns as the input, but with the "geometry" column cleaned as described above.
    """
    lines["geometry"] = (
        lines["geometry"].line_merge().map(make_continuous).map(clean_multilines)
    )
    return lines
