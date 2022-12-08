from typing import Any, Optional

from ... import db
from .st_types import Geom


def getMaybeGeomId(
    cursor: Any,
    geom: Geom,
) -> Optional[db.TableId]:
    """
    Use https://postgis.net/docs/ST_ReducePrecision.html for querying geometries.
    """
    stmt = """select G.geom_id
              FROM geom G
              where ST_Equals(G.geom, ST_ReducePrecision(ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)), 7)
         """
    args = {"geom": geom.geom}
    cursor.execute(stmt, args)
    result = cursor.fetchall()
    if len(result) >= 1:
        return db.TableId(result[0].geom_id)
    return None


# TODO: Warn the user when the geometry is modified by ST_MakeValid
def insertOrGetGeom(
    cursor: Any,
    geom: Geom,
) -> db.TableId:
    """
    Use https://postgis.net/docs/ST_QuantizeCoordinates.html for upserting geometries.
    """
    maybe_geom_id = getMaybeGeomId(cursor, geom)
    if maybe_geom_id is not None:
        return maybe_geom_id
    stmt = """insert into geom(geom)
            values(ST_QuantizeCoordinates(ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)), 7)
            returning geom_id"""
    cursor.execute(stmt, geom())
    result = cursor.fetchone()
    return db.TableId(result.geom_id)
