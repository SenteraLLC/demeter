from typing import Optional, Any

from ...db.generic_types import GetId, GetTable, ReturnId

from .types import Geom, TableId

def getMaybeDuplicateGeom(cursor : Any,
                          geom   : Geom,
                         ) -> Optional[TableId]:
  stmt = """select G.geom_id
              FROM geom G
              where ST_Equals(G.geom, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
         """
  args = {"geom" : geom.geom}
  cursor.execute(stmt, args)
  result = cursor.fetchall()
  if len(result) >= 1:
    return TableId(result[0].geom_id)
  return None


# TODO: Warn the user when the geometry is modified by ST_MakeValid
def insertOrGetGeom(cursor   : Any,
                    geom     : Geom,
                    container_geom_id : Optional[int] = None,
                   ) -> TableId:
  maybe_geom_id = getMaybeDuplicateGeom(cursor, geom)
  if maybe_geom_id is not None:
    return maybe_geom_id
  stmt = """insert into geom(container_geom_id, geom)
            values(%(container_geom_id)s, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
            returning geom_id"""
  cursor.execute(stmt, geom())
  result = cursor.fetchone()
  return TableId(result.geom_id)

