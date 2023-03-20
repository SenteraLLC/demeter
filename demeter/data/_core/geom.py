from typing import (
    Any,
    Optional,
    Union,
)

from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from demeter.db import TableId


def getMaybeGeomId(
    cursor: Any,
    geometry: Union[
        LineString,
        Point,
        Polygon,
        MultiLineString,
        MultiPoint,
        MultiPolygon,
        GeometryCollection,
    ],
) -> Optional[TableId]:
    """
    Because ST_QuantizeCoordinates was used for upserting geometries, it must also be used for checking for duplicates.

    ST_ReducePrecision is to be used for reducing further than was actually stored.
    """
    stmt = """
    select G.geom_id
    FROM geom G
    where ST_Equals(
        G.geom, ST_QuantizeCoordinates(
            ST_ReducePrecision(
                ST_MakeValid(
                    ST_Transform(
                        ST_SetSRID(%(geom)s::geometry, %(srid)s),
                        4326
                    )
                ), 1e-%(precision)s
            ), %(precision)s
        )
    )
    """
    # args = {"geom": geom}
    args = {"geom": geometry.wkb_hex, "srid": 4326, "precision": 7}
    cursor.execute(stmt, args)
    result = cursor.fetchall()
    assert len(result) <= 1, "Returned result is expected to be <= 1"
    if len(result) == 1:
        return TableId(result[0].geom_id)
    return None


# TODO: Warn the user when the geometry is modified by ST_MakeValid
def insertOrGetGeom(
    cursor: Any,
    geometry: Union[
        LineString,
        Point,
        Polygon,
        MultiLineString,
        MultiPoint,
        MultiPolygon,
        GeometryCollection,
    ],
) -> TableId:
    """
    Use https://postgis.net/docs/ST_QuantizeCoordinates.html for upserting geometries.
    """
    maybe_geom_id = getMaybeGeomId(cursor, geometry)
    if maybe_geom_id is not None:
        return maybe_geom_id
    stmt = """
    insert into geom(geom)
    values(
        ST_QuantizeCoordinates(
            ST_ReducePrecision(
                ST_MakeValid(
                    ST_Transform(
                        ST_SetSRID(%(geom)s::geometry, %(srid)s),
                        4326
                    )
                ), 1e-%(precision)s
            ), %(precision)s
        )
    )
    returning geom_id
    """
    args = {
        "geom": geometry.wkb_hex,
        "srid": 4326,
        "precision": 7,
    }  # 7 digits has 1.11 cm accuracy at the equator
    cursor.execute(stmt, args)
    result = cursor.fetchone()
    return TableId(result.geom_id)
