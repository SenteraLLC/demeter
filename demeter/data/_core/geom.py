from typing import (
    Any,
    Optional,
    Union,
)

from geo_utils.world import round_geometry
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.wkb import loads as wkb_loads

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
    Because ST_QuantizeCoordinates and ST_ReducePrecision were used for upserting geometries, they must also be used for
    checking for duplicates.
    """
    stmt = """
    select G.geom_id
    FROM geom G
    where ST_Equals(
        ST_QuantizeCoordinates(ST_ReducePrecision(G.geom, 1e-%(precision)s), %(precision)s),
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
    """
    precision = 7
    # args = {"geom": round_geometry(geometry, n_decimal_places=precision).wkb_hex, "srid": 4326, "precision": precision}
    args = {"geom": geometry.wkb_hex, "srid": 4326, "precision": precision}
    cursor.execute(stmt, args)
    result = cursor.fetchall()
    assert len(result) <= 1, "Returned result is expected to be <= 1"
    if len(result) == 1:
        return TableId(result[0].geom_id)
    return None


def getMaybeGeom(
    cursor: Any,
    geom_id: int,
) -> Optional[TableId]:
    stmt = """
    SELECT G.geom as geom
    FROM geom G
    WHERE G.geom_id = %(geom_id)s;
    """
    precision = 7
    args = {"geom_id": geom_id}
    cursor.execute(stmt, args)

    result = cursor.fetchall()
    assert len(result) <= 1, "Returned result is expected to be <= 1"
    if len(result) == 1:
        return round_geometry(
            wkb_loads(result[0].geom, hex=True), n_decimal_places=precision
        )


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

    ST_ReducePrecision is to be used for reducing precision further than was actually passed. Both
    ST_QuantizeCoordinates and ST_ReducePrecision are necessary for checking for equality via ST_Equals.
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
    precision = 9
    args = {
        # "geom": round_geometry(geometry, n_decimal_places=precision).wkb_hex,
        "geom": geometry.wkb_hex,
        "srid": 4326,
        "precision": precision,
    }  # 7 digits has 1.11 cm accuracy at the equator
    cursor.execute(stmt, args)
    result = cursor.fetchone()
    return TableId(result.geom_id)
