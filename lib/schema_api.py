import psycopg2
import argparse
import json

from datetime import date
from shapely.geometry import LineString # type: ignore
from functools import partial
from collections import OrderedDict

from typing import TypedDict, Any, Literal, List, Tuple, Dict, Callable, Optional, Sequence, Type, TypeVar
from typing import cast

from .generators import *
from . import types
from psycopg2 import sql, extras

T = TypeVar('T', bound=types.AnyIdTable)
ReturnId = Callable[[Any, T], int]

insertField          : ReturnId[types.Field]      = getInsertReturnIdFunction(types.Field)
insertLocalParameter : ReturnId[types.LocalParameter]      = getInsertReturnIdFunction(types.LocalParameter)
insertLocalValue     : ReturnId[types.LocalValue] = getInsertReturnIdFunction(types.LocalValue)
insertOwner          : ReturnId[types.Owner]      = getInsertReturnIdFunction(types.Owner)
insertGrower         : ReturnId[types.Grower]     = getInsertReturnIdFunction(types.Grower)
insertGeoSpatialKey : ReturnId[types.GeoSpatialKey] = getInsertReturnIdFunction(types.GeoSpatialKey)
insertTemporalKey : ReturnId[types.TemporalKey] = getInsertReturnIdFunction(types.TemporalKey)


insertUnitType   : ReturnId[types.UnitType]   = getInsertReturnIdFunction(types.UnitType)
insertLocalType  : ReturnId[types.LocalType]  = getInsertReturnIdFunction(types.LocalType)
insertCropType   : ReturnId[types.CropType]   = getInsertReturnIdFunction(types.CropType)
insertCropStage  : ReturnId[types.CropStage]  = getInsertReturnIdFunction(types.CropStage)
insertReportType : ReturnId[types.ReportType] = getInsertReturnIdFunction(types.ReportType)
insertLocalGroup : ReturnId[types.LocalGroup] = getInsertReturnIdFunction(types.LocalGroup)
insertHTTPType : ReturnId[types.HTTPType] = getInsertReturnIdFunction(types.HTTPType)


# TODO: Fix typing issues here
S = TypeVar('S', bound=types.AnyKeyTable)
SK = TypeVar('SK', bound=types.Key)
ReturnKey = Callable[[Any, S], SK]

insertPlanting     : ReturnKey[types.Planting, types.PlantingKey] = getInsertReturnKeyFunction(types.Planting) # type: ignore
insertHarvest      : ReturnKey[types.Harvest, types.HarvestKey] = getInsertReturnKeyFunction(types.Harvest) # type: ignore
insertCropProgress : ReturnKey[types.CropProgress, types.CropProgressKey] = getInsertReturnKeyFunction(types.CropProgress) # type: ignore


U = TypeVar('U', bound=types.AnyIdTable)
GetId = Callable[[Any, U], Optional[int]]

getMaybeFieldId          : GetId[types.Field]      = getMaybeIdFunction(types.Field)
getMaybeLocalParameterId : GetId[types.LocalParameter]      = getMaybeIdFunction(types.LocalParameter)
getMaybeLocalValue       : GetId[types.LocalValue] = getMaybeIdFunction(types.LocalValue)
getMaybeOwnerId          : GetId[types.Owner]      = getMaybeIdFunction(types.Owner)
getMaybeGrowerId         : GetId[types.Grower]      = getMaybeIdFunction(types.Grower)

getMaybeUnitTypeId       : GetId[types.UnitType]   = getMaybeIdFunction(types.UnitType)
getMaybeLocalTypeId  : GetId[types.LocalType]  = getMaybeIdFunction(types.LocalType)
getMaybeCropTypeId   : GetId[types.CropType]   = getMaybeIdFunction(types.CropType)
getMaybeCropStageId  : GetId[types.CropStage]  = getMaybeIdFunction(types.CropStage)
getMaybeReportTypeId : GetId[types.ReportType] = getMaybeIdFunction(types.ReportType)
getMaybeLocalGroupId : GetId[types.LocalGroup] = getMaybeIdFunction(types.LocalGroup)
getMaybeHTTPTypeId   : GetId[types.HTTPType] = getMaybeIdFunction(types.HTTPType)



V = TypeVar('V', bound=types.AnyIdTable)
GetTable = Callable[[Any, int], V]

getField : GetTable[types.Field] = getTableFunction(types.Field)
getOwner : GetTable[types.Owner] = getTableFunction(types.Owner)
getGeom  : GetTable[types.Geom]  = getTableFunction(types.Geom)
getHTTP  : GetTable[types.HTTPType] = getTableFunction(types.HTTPType)


def getHTTPByName(cursor : Any, http_type_name : str) -> Tuple[int, types.HTTPType]:
  stmt = """select * from http_type where type_name = %(name)s"""
  cursor.execute(stmt, { 'name' : http_type_name })

  results = cursor.fetchall()
  # TODO: Better exceptions
  assert(len(results) == 1)
  result_with_id = results[0]

  http_type_id = result_with_id["http_type_id"]
  result_with_id["verb"] = types.stringToHTTPVerb(result_with_id["verb"])
  del result_with_id["http_type_id"]
  http_type = cast(types.HTTPType, result_with_id)

  return http_type_id, http_type


W = TypeVar('W', bound=types.AnyTypeTable)

def insertOrGetType(get_id    : GetId[W],
                    return_id : ReturnId[W],
                    cursor    : Any,
                    some_type : W,
                   ) -> int:
  maybe_type_id = get_id(cursor, some_type)
  if maybe_type_id is not None:
    return maybe_type_id
  return return_id(cursor, some_type)

# TODO: These probably shouldn't be allowed in practice
#       There should be a separate setup script(s) for establishing types
# TODO: Build a python enum for crop stages?
# API
insertOrGetUnitType = partial(insertOrGetType, getMaybeUnitTypeId, insertUnitType)
insertOrGetLocalType = partial(insertOrGetType, getMaybeLocalTypeId, insertLocalType)
insertOrGetCropType = partial(insertOrGetType, getMaybeCropTypeId, insertCropType)
insertOrGetCropStage = partial(insertOrGetType, getMaybeCropStageId, insertCropStage)
insertOrGetLocalGroup = partial(insertOrGetType, getMaybeLocalGroupId, insertLocalGroup)


def makeInsertable(geom : types.Geom) -> types.InsertableGeom:
  return types.InsertableGeom(
           geom = json.dumps(geom["geom"]),
           container_geom_id = geom["container_geom_id"],
         )


# TODO: Combine this with insertInsertableGeo
# TODO: The ST_Equals check is also done on insert, worth resolving?
def getMaybeDuplicateGeom(cursor : Any,
                          geom   : types.Geom,
                         ) -> Optional[int]:
  igeo = makeInsertable(geom)
  stmt = """select G.geom_id
              FROM geom G
              where ST_Equals(ST_MakeValid(G.geom), ST_Transform(%(geom)s::geometry, 4326))
         """
  args = {"geom" : igeo["geom"]}
  cursor.execute(stmt, args)
  result = cursor.fetchall()
  if len(result) >= 1:
    return result[0]["geom_id"]
  return None


def insertInsertableGeom(cursor : Any,
                         geom   : types.InsertableGeom,
                        ) -> int:
  stmt = """insert into geom(container_geom_id, geom)
            values(%(container_geom_id)s, ST_MakeValid(ST_Transform(%(geom)s::geometry, 4326)))
            returning geom_id"""
  cursor.execute(stmt, geom)
  result = cursor.fetchone()
  return result["geom_id"]


def insertGeom(cursor   : Any,
               geom     : types.Geom,
              ) -> int:
  maybe_geom_id = getMaybeDuplicateGeom(cursor, geom)
  if maybe_geom_id is not None:
    return maybe_geom_id
  igeo = makeInsertable(geom)
  return insertInsertableGeom(cursor, igeo) # type: ignore


