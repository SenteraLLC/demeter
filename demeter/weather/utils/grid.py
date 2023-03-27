"""Public helper functions for using the weather grid network."""

from typing import (
    Any,
    List,
    Union,
)

from geo_utils.vector import reproject_shapely
from geopandas import read_postgis
from global_land_mask.globe import is_land
from pandas import DataFrame
from pyproj import CRS
from shapely.geometry import Point
from shapely.wkb import loads as wkb_loads
from sqlalchemy.engine import Connection


def get_cell_id(
    cursor: Any, geometry: Point, geometry_crs: CRS = CRS.from_epsg(4326)
) -> int:
    """For a given Point geometry, determine cell ID.

    If the point geometry somehow falls on a polygon boundary, which results in 2 cell IDs
    being returned, the smaller cell ID will always be returned for consistency.


    Args:
        cursor (Any): Connection to Demeter weather database
        geometry (shapely.geometry.Point): Point geometry for which to extract cell ID

        geometry_crs (pyproj.CRS): Coordinate reference system for `geometry`. Defaults to
            WGS 84 (EPSG=4326).

    Returns:
        cell ID (int) of weather grid pixel the point falls within
    """

    assert isinstance(geometry, Point), "`geometry` must be passed as a `Point`"

    epsg_src = geometry_crs.to_epsg()
    if epsg_src != 4326:
        epsg_dst = 4326
        geometry = reproject_shapely(
            epsg_src=epsg_src, epsg_dst=epsg_dst, geometry=geometry
        )
    stmt = """
    select ST_Value(
            raster_5km.rast_cell_id,
            ST_Transform(ST_Point(%(x)s, %(y)s, 4326), world_utm.raster_epsg)
        ) as cell_id
    from world_utm, raster_5km
    where ST_intersects(ST_Point(%(x)s, %(y)s, 4326), world_utm.geom)
    and world_utm.world_utm_id=raster_5km.world_utm_id;
    """
    args = {"x": geometry.x, "y": geometry.y}

    cursor.execute(stmt, args)
    res = DataFrame(cursor.fetchall()).sort_values(by=["cell_id"])

    return int(res.at[0, "cell_id"])


def get_centroid(cursor: Any, world_utm_id: int, cell_id: int):
    """For a given cell ID and world UTM ID, get its centroid from the database."""
    stmt = """
    with q1 as (
        select raster_5km.rast_cell_id as rast
        from world_utm, raster_5km
        where world_utm.world_utm_id= %(world_utm_id)s
        and world_utm.world_utm_id=raster_5km.world_utm_id
    ), q2 as (
        select q1.rast, (ST_PixelOfValue(q1.rast, 1, %(cell_id)s)).*
        from q1
    ), q3 as (
        select ST_ReducePrecision(ST_Transform(ST_PixelAsCentroid(q2.rast, q2.x, q2.y), 4326), 0.00001) as point
        from q2
    )
    select ST_Point(ROUND(ST_X(q3.point)::numeric,5), ROUND(ST_Y(q3.point)::numeric,5)) as centroid
    from q3;
    """
    args = {"world_utm_id": world_utm_id, "cell_id": int(cell_id)}
    cursor.execute(stmt, args)
    res = DataFrame(cursor.fetchall())["centroid"].item()
    centroid = wkb_loads(res, hex=True)

    return centroid


def get_world_utm_info_for_cell_id(cursor: Any, cell_id: int):
    """For a given cell ID, get its `world_utm_id`, `zone`, `row`, and `utc_offset` from the database."""
    stmt = """
    select q.world_utm_id, (ST_PixelOfValue(q.rast, 1, %(cell_id)s)).*, w.row, w.zone, w.utc_offset
    from (
        select world_utm_id, rast_cell_id as rast
        from raster_5km
    ) as q
    cross join world_utm as w
    where w.world_utm_id = q.world_utm_id;
    """

    args = {"cell_id": int(cell_id)}
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())[
        ["world_utm_id", "zone", "row", "utc_offset"]
    ]

    assert len(df_result) == 1, "Error: More than one raster contains this cell ID."

    return df_result


def pt_is_on_land(point: Point) -> bool:
    """Indicates whether the passed Point geometry is on land or not."""
    return is_land(lat=point.y, lon=point.x)


def get_info_for_world_utm(
    cursor_weather: Any, world_utm_id: Union[int, List[int]]
) -> DataFrame:
    """Return 'utc_offset', 'utm_zone', 'utm_row' for list of `weather.world_utm` IDs.

    Args:
        cursor (Any): Connection to Demeter weather schema
        world_utm_id (int or list of int): IDS from world_utm table to extract utc_offset for

    Returns dataframe with two columns: "world_utm_id", "utc_offset", "utm_zone", "utm_row"
    """
    if not isinstance(world_utm_id, list):
        world_utm_id = [world_utm_id]

    world_utm_id_tuple = tuple(world_utm_id)

    stmt = """
    select world_utm_id, utc_offset, zone, row
    from world_utm
    where world_utm_id in %(world_utm_id)s
    """
    args = {"world_utm_id": world_utm_id_tuple}
    cursor_weather.execute(stmt, args)
    df_result = DataFrame(cursor_weather.fetchall())

    assert len(df_result) > 0, "This `world_utm_id` does not exist in the DB."

    return df_result.rename(columns={"zone": "utm_zone", "row": "utm_row"})


def query_weather_grid(conn: Connection, point: Point, crs: CRS = CRS.from_epsg(4326)):
    """
    Queries all the relevant information of the demeter weather grid from a Point geometry.

    Args:
        conn (Connection): Active connection to the database to query.
        point (Point): Point
        crs (CRS, optional): CRS of `point`; projected to EPSG=4326 if not already in EPSG=4326. Defaults to
        CRS.from_epsg(4326).

    Returns:
        GeoDataFrame: With columns ["world_utm_id", "rast_col", "rast_row", "cell_id", "lng", "lat", "pixel_centroid"].
        `pixel_centroid` is the centroid of the 5km grid cell that intersects with `point`; `lng_centroid` and
        `lat_centroid` are the latitude and longitude of `pixel_centroid`, rounded to five (5) decimal places.
    """
    assert isinstance(point, Point), "`point` must be passed as a `Point`"

    epsg_src = crs.to_epsg()
    if epsg_src != 4326:
        epsg_dst = 4326
        point = reproject_shapely(epsg_src=epsg_src, epsg_dst=epsg_dst, geometry=point)
    precision = 5
    prec_decimal = "0." + "0" * (precision - 1) + "1"
    stmt = """
    with q1 AS (
        select raster_5km.rast_cell_id as rast,
            ST_Value(
            raster_5km.rast_cell_id,
            ST_Transform(ST_Point(%(x)s, %(y)s, 4326), world_utm.raster_epsg)
            ) as cell_id,
            world_utm.world_utm_id as world_utm_id
        from world_utm, raster_5km
        where ST_intersects(ST_Point(%(x)s, %(y)s, 4326), world_utm.geom)
        and world_utm.world_utm_id=raster_5km.world_utm_id
    ), q2 AS (
        select q1.*, ST_PixelOfValue(q1.rast, 1, q1.cell_id) as pixel
        from q1
    ), q3 AS (
        select q2.world_utm_id, (q2.pixel).x as rast_col, (q2.pixel).y as rast_row, q2.cell_id,
        ST_ReducePrecision(ST_Transform(ST_PixelAsCentroid(q2.rast, (q2.pixel).x, (q2.pixel).y), 4326), %(prec_decimal)s) as point
        from q2
    )
    select q3.world_utm_id, q3.rast_col, q3.rast_row, q3.cell_id, ROUND(ST_X(q3.point)::numeric, %(precision)s) as lng_centroid, ROUND(ST_Y(q3.point)::numeric, %(precision)s) as lat_centroid, q3.point as pixel_centroid
    from q3
    """
    args = {
        "x": point.x,
        "y": point.y,
        "precision": precision,
        "prec_decimal": prec_decimal,
    }

    # cursor.execute(stmt, args)
    # res = DataFrame(cursor.fetchall(), geometry="geometry", crs=CRS.from_epsg(4326)).sort_values(by=["cell_id"])
    return read_postgis(
        sql=stmt,
        con=conn,
        params=args,
        geom_col="pixel_centroid",
        crs=CRS.from_epsg(4326),
    )
