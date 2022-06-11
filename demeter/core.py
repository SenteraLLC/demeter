from typing import Optional, Any

from functools import partial

from .database.api_protocols import GetId, GetTable, ReturnId
from .database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getTableFunction, insertOrGetType

from .types.core import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom, Coordinates, Polygon, MultiPolygon, Point, Line

getMaybeFieldId          : GetId[Field]      = getMaybeIdFunction(Field)
getMaybeOwnerId          : GetId[Owner]      = getMaybeIdFunction(Owner)
getMaybeGrowerId         : GetId[Grower]      = getMaybeIdFunction(Grower)
getMaybeGeoSpatialKeyId  : GetId[GeoSpatialKey] = getMaybeIdFunction(GeoSpatialKey)
getMaybeTemporalKeyId    : GetId[TemporalKey] = getMaybeIdFunction(TemporalKey)

getField      : GetTable[Field]    = getTableFunction(Field)
getOwner      : GetTable[Owner]    = getTableFunction(Owner)
getGeom       : GetTable[Geom]     = getTableFunction(Geom)

insertField          : ReturnId[Field]      = getInsertReturnIdFunction(Field)
insertOwner          : ReturnId[Owner]      = getInsertReturnIdFunction(Owner)
insertGrower         : ReturnId[Grower]     = getInsertReturnIdFunction(Grower)
insertGeoSpatialKey : ReturnId[GeoSpatialKey] = getInsertReturnIdFunction(GeoSpatialKey)
insertTemporalKey : ReturnId[TemporalKey] = getInsertReturnIdFunction(TemporalKey)

insertOrGetGeoSpatialKey = partial(insertOrGetType, getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = partial(insertOrGetType, getMaybeTemporalKeyId, insertTemporalKey)
insertOrGetOwner = partial(insertOrGetType, getMaybeOwnerId, insertOwner)
insertOrGetGrower = partial(insertOrGetType, getMaybeGrowerId, insertGrower)
insertOrGetField = partial(insertOrGetType, getMaybeFieldId, insertField)


def getMaybeDuplicateGeom(cursor : Any,
                          geom   : Geom,
                         ) -> Optional[int]:
  stmt = """select G.geom_id
              FROM geom G
              where ST_Equals(G.geom, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
         """
  args = {"geom" : geom.geom}
  cursor.execute(stmt, args)
  result = cursor.fetchall()
  if len(result) >= 1:
    return int(result[0].geom_id)
  return None


def insertGeom(cursor   : Any,
               geom     : Geom,
              ) -> int:
  maybe_geom_id = getMaybeDuplicateGeom(cursor, geom)
  if maybe_geom_id is not None:
    return maybe_geom_id
  stmt = """insert into geom(container_geom_id, geom)
            values(%(container_geom_id)s, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
            returning geom_id"""
  cursor.execute(stmt, geom)
  result = cursor.fetchone()
  return int(result.geom_id)

insertOrGetGeom = insertGeom


