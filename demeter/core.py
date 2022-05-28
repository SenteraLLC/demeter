from typing import Optional, Any, Mapping

from functools import partial
import json
import datetime

from .database.api_protocols import GetId, GetTable, ReturnId
from .database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getTableFunction, insertOrGetType
from .database.types_protocols import TableEncoder

from .types.core import Field, Grower, GeoSpatialKey, TemporalKey, Owner, Geom, GeomImpl, InsertableGeom, CRS, Coordinates, Polygon, MultiPolygon, Properties

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


def makeInsertable(geom : Geom) -> InsertableGeom:
  return InsertableGeom(
           geom = json.dumps(geom.geom, cls=TableEncoder),
           container_geom_id = geom.container_geom_id,
         )


# TODO: Combine this with insertInsertableGeo
# TODO: The ST_Equals check is also done on insert, worth resolving?
def getMaybeDuplicateGeom(cursor : Any,
                          geom   : Geom,
                         ) -> Optional[int]:
  igeo = makeInsertable(geom)
  stmt = """select G.geom_id
              FROM geom G
              where ST_Equals(G.geom, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
         """
  args = {"geom" : igeo.geom}
  cursor.execute(stmt, args)
  result = cursor.fetchall()
  if len(result) >= 1:
    return result[0]["geom_id"]
  return None


def insertInsertableGeom(cursor : Any,
                         geom   : InsertableGeom,
                        ) -> int:
  stmt = """insert into geom(container_geom_id, geom)
            values(%(container_geom_id)s, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
            returning geom_id"""
  cursor.execute(stmt, geom)
  result = cursor.fetchone()
  return result["geom_id"]


# TODO: Should be called insertOrGetGeom
def insertGeom(cursor   : Any,
               geom     : Geom,
              ) -> int:
  maybe_geom_id = getMaybeDuplicateGeom(cursor, geom)
  if maybe_geom_id is not None:
    return maybe_geom_id
  igeo = makeInsertable(geom)
  return insertInsertableGeom(cursor, igeo)

insertOrGetGeom = insertGeom


