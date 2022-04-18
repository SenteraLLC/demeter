import psycopg2
import argparse
import json
import sys

from datetime import date
from shapely.geometry import LineString # type: ignore
from functools import partial
from collections import OrderedDict

from typing import TypedDict, Any, List, Tuple, Dict, Callable, Optional, Type, TypeVar, Set, Union
from typing import cast

from .generators import *
from . import types
from psycopg2 import sql, extras

T = TypeVar('T', bound=types.AnyIdTable)
ReturnId = Callable[[Any, T], int]

insertField          : ReturnId[types.Field]      = getInsertReturnIdFunction(types.Field)
insertLocalValue     : ReturnId[types.LocalValue] = getInsertReturnIdFunction(types.LocalValue)
insertOwner          : ReturnId[types.Owner]      = getInsertReturnIdFunction(types.Owner)
insertGrower         : ReturnId[types.Grower]     = getInsertReturnIdFunction(types.Grower)
insertGeoSpatialKey : ReturnId[types.GeoSpatialKey] = getInsertReturnIdFunction(types.GeoSpatialKey)
insertTemporalKey : ReturnId[types.TemporalKey] = getInsertReturnIdFunction(types.TemporalKey)
insertS3Object : ReturnId[types.S3Object] = getInsertReturnIdFunction(types.S3Object)


insertUnitType   : ReturnId[types.UnitType]   = getInsertReturnIdFunction(types.UnitType)
insertLocalType  : ReturnId[types.LocalType]  = getInsertReturnIdFunction(types.LocalType)
insertCropType   : ReturnId[types.CropType]   = getInsertReturnIdFunction(types.CropType)
insertCropStage  : ReturnId[types.CropStage]  = getInsertReturnIdFunction(types.CropStage)
insertReportType : ReturnId[types.ReportType] = getInsertReturnIdFunction(types.ReportType)
insertLocalGroup : ReturnId[types.LocalGroup] = getInsertReturnIdFunction(types.LocalGroup)
insertHTTPType : ReturnId[types.HTTPType] = getInsertReturnIdFunction(types.HTTPType)
insertS3TypeBase   : ReturnId[types.S3Type] = getInsertReturnIdFunction(types.S3Type)
insertLocalParameter : ReturnId[types.LocalParameter] = getInsertReturnIdFunction(types.LocalParameter)
insertHTTPParameter : ReturnId[types.HTTPParameter]   = getInsertReturnIdFunction(types.HTTPParameter)
insertS3InputParameter : ReturnId[types.S3InputParameter] = getInsertReturnIdFunction(types.S3InputParameter)
insertS3OutputParameter : ReturnId[types.S3OutputParameter] = getInsertReturnIdFunction(types.S3OutputParameter)
insertFunction : ReturnId[types.Function] = getInsertReturnIdFunction(types.Function)
insertFunctionType : ReturnId[types.FunctionType] = getInsertReturnIdFunction(types.FunctionType)



# TODO: Fix typing issues here
S = TypeVar('S', bound=types.AnyKeyTable)
SK = TypeVar('SK', bound=types.Key)
ReturnKey = Callable[[Any, S], SK]

insertPlanting     : ReturnKey[types.Planting, types.PlantingKey] = getInsertReturnKeyFunction(types.Planting) # type: ignore
insertHarvest      : ReturnKey[types.Harvest, types.HarvestKey] = getInsertReturnKeyFunction(types.Harvest) # type: ignore
insertCropProgress : ReturnKey[types.CropProgress, types.CropProgressKey] = getInsertReturnKeyFunction(types.CropProgress) # type: ignore
insertS3ObjectKey : ReturnKey[types.S3ObjectKey, types.S3ObjectKey] = getInsertReturnKeyFunction(types.S3ObjectKey) # type: ignore

insertS3TypeDataFrame : ReturnKey[types.S3TypeDataFrame, types.S3TypeDataFrame] = getInsertReturnKeyFunction(types.S3TypeDataFrame) # type: ignore

s3_sub_type_insert_lookup = {
  types.S3TypeDataFrame : insertS3TypeDataFrame
}



def insertS3ObjectKeys(cursor       : Any,
                       s3_object_id : int,
                       keys         : List[types.Key],
                       s3_type_id   : int,
                      ) -> bool:
  s3_key_names = list(types.S3ObjectKey.__annotations__.keys())
  stmt = generateInsertMany("s3_object_key", s3_key_names, len(keys))
  args = []
  for k in keys:
    s3_object_key = types.S3ObjectKey(
                      s3_object_id      = s3_object_id,
                      s3_type_id        = s3_type_id,
                      geospatial_key_id = k["geospatial_key_id"],
                      temporal_key_id   = k["temporal_key_id"],
                    )
    for key_name in s3_key_names:
      args.append(s3_object_key[key_name])  # type: ignore
  results = cursor.execute(stmt, args)
  return True


U = TypeVar('U', bound=types.AnyIdTable)
GetId = Callable[[Any, U], Optional[int]]

getMaybeFieldId          : GetId[types.Field]      = getMaybeIdFunction(types.Field)
getMaybeLocalParameterId : GetId[types.LocalParameter]      = getMaybeIdFunction(types.LocalParameter)
getMaybeLocalValue       : GetId[types.LocalValue] = getMaybeIdFunction(types.LocalValue)
getMaybeOwnerId          : GetId[types.Owner]      = getMaybeIdFunction(types.Owner)
getMaybeGrowerId         : GetId[types.Grower]      = getMaybeIdFunction(types.Grower)
getMaybeGeoSpatialKeyId  : GetId[types.GeoSpatialKey] = getMaybeIdFunction(types.GeoSpatialKey)
getMaybeTemporalKeyId  : GetId[types.TemporalKey] = getMaybeIdFunction(types.TemporalKey)


getMaybeUnitTypeId   : GetId[types.UnitType]   = getMaybeIdFunction(types.UnitType)
getMaybeLocalTypeId  : GetId[types.LocalType]  = getMaybeIdFunction(types.LocalType)
getMaybeCropTypeId   : GetId[types.CropType]   = getMaybeIdFunction(types.CropType)
getMaybeCropStageId  : GetId[types.CropStage]  = getMaybeIdFunction(types.CropStage)
getMaybeReportTypeId : GetId[types.ReportType] = getMaybeIdFunction(types.ReportType)
getMaybeLocalGroupId : GetId[types.LocalGroup] = getMaybeIdFunction(types.LocalGroup)
getMaybeHTTPTypeId   : GetId[types.HTTPType]   = getMaybeIdFunction(types.HTTPType)
getMaybeS3TypeId     : GetId[types.S3Type]     = getMaybeIdFunction(types.S3Type)


V = TypeVar('V', bound=types.AnyIdTable)
GetTable = Callable[[Any, int], V]

getField      : GetTable[types.Field]    = getTableFunction(types.Field)
getOwner      : GetTable[types.Owner]    = getTableFunction(types.Owner)
getGeom       : GetTable[types.Geom]     = getTableFunction(types.Geom)
getHTTPType   : GetTable[types.HTTPType] = getTableFunction(types.HTTPType)
getS3Object   : GetTable[types.S3Object] = getTableFunction(types.S3Object)

getS3TypeBase      : GetTable[types.S3Type]   = getTableFunction(types.S3Type)
getMaybeS3TypeDataFrame : GetTable[types.S3TypeDataFrame] = getTableFunction(types.S3TypeDataFrame, "s3_type_id")

s3_sub_type_get_lookup = {
  types.S3TypeDataFrame : getMaybeS3TypeDataFrame
}


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


# TODO: Implement complex caching behavior using S3
def getS3ObjectByKey(cursor         : Any,
                     k              : types.Key,
                    ) -> types.S3Object:
  stmt = """select *
            from s3_object O, s3_object_key K
            where O.s3_object_id = K.s3_object_id and
                  K.geospatial_key_id = %(geospatial_key_id)s and
                  K.temporal_key_id = %(temporal_key_id)s
         """
  args = { k : v for k, v in k.items() if k in ["geospatial_key_id", "temporal_key_id"] }
  cursor.execute(stmt, args)
  results = cursor.fetchall()
  if len(results) <= 0:
    raise Exception("No S3 object exists for: ",k)

  result_with_id = results[0]
  del result_with_id["s3_object_id"]
  s3_object = result_with_id

  return cast(types.S3Object, s3_object)


def getS3ObjectByKeys(cursor    : Any,
                      keys      : List[types.Key],
                      type_name : str,
                     ) -> Tuple[int, types.S3Object]:
  clauses = []
  args : List[Union[str, int]] = [type_name]
  for k in keys:
    clauses.append(sql.SQL("").join(
                    [sql.SQL("("),
                     sql.SQL(" and ").join([sql.Placeholder() + sql.SQL(" = K.geospatial_key_id"),
                                            sql.Placeholder() + sql.SQL(" = K.temporal_key_id"),
                                           ]),
                     sql.SQL(")")
                    ],
                  ))
    args.append(k["geospatial_key_id"])
    args.append(k["temporal_key_id"])

  filters = sql.SQL(" or ").join(clauses)

  stmt = sql.SQL("""select O.s3_object_id, O.key, O.bucket_name, O.s3_type_id, count(*)
                    from s3_object O
                    join s3_type T on T.type_name = %s and T.s3_type_id = O.s3_type_id
                    join s3_object_key K on O.s3_object_id = K.s3_object_id
                    where {filters}
                    group by O.s3_object_id, O.key, O.bucket_name, O.s3_type_id
                    order by O.s3_object_id desc
                 """).format(filters = filters)

  cursor.execute(stmt, args)
  results = cursor.fetchall()
  if len(results) <= 0:
    sys.stderr.write(f"No S3 object found for {type_name}")

  most_recent = results[0]
  s3_object_id = most_recent["s3_object_id"]
  if len(results) > 1:
    duplicate_object_ids = [x["s3_object_id"] for x in results]
    sys.stderr.write(f"Multiple results detected for {type_name} ({duplicate_object_ids}), taking most recent '{s3_object_id}'")

  s3_object = types.S3Object(
                key = most_recent["key"],
                bucket_name = most_recent["bucket_name"],
                s3_type_id = most_recent["s3_type_id"],
              )
  return s3_object_id, s3_object


def getS3TypeIdByName(cursor : Any, type_name : str) -> int:
  stmt = """select s3_type_id from s3_type where type_name = %(type_name)s"""
  cursor.execute(stmt, {"type_name": type_name})
  results = cursor.fetchall()
  if len(results) <= 0:
    raise Exception("No type exists '%(type_name)s'",type_name)
  return results[0]["s3_type_id"]


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
insertOrGetS3Type = partial(insertOrGetType, getMaybeS3TypeId, insertS3TypeBase)
insertOrGetGeoSpatialKey = partial(insertOrGetType, getMaybeGeoSpatialKeyId, insertGeoSpatialKey)
insertOrGetTemporalKey = partial(insertOrGetType, getMaybeTemporalKeyId, insertTemporalKey)



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


def insertOrGetS3TypeDataFrame(cursor : Any,
                               s3_type : types.S3Type,
                               driver : str,
                               has_geometry : bool,
                              ) -> int:
  s3_type_id = insertOrGetS3Type(cursor, s3_type)
  stmt = """insert into s3_type_dataframe(s3_type_id, driver, has_geometry)
            values(%(s3_type_id)s, %(driver)s, %(has_geometry)s)
            on conflict do nothing"""
  args = {"s3_type_id"   : s3_type_id,
          "driver"       : driver,
          "has_geometry" : has_geometry,
         }
  cursor.execute(stmt, args)

  return s3_type_id


def insertS3Type(cursor : Any,
                 s3_type : types.S3Type,
                 s3_sub_type : Optional[types.S3SubType],
                ) -> int:
  s3_type_id = insertOrGetS3Type(cursor, s3_type)
  if s3_sub_type is not None:
    sub_type_insert_fn = s3_sub_type_insert_lookup[type(s3_sub_type)]
    s3_sub_type_key = sub_type_insert_fn(cursor, s3_sub_type)
  return s3_type_id


def getS3Type(cursor : Any,
              s3_type_id : int,
             ) -> Tuple[types.S3Type, Optional[types.TaggedS3SubType]]:
  s3_type = getS3TypeBase(cursor, s3_type_id)
  maybe_s3_sub_type = None
  for s3_sub_type_tag, s3_sub_type_get_lookup_fn in s3_sub_type_get_lookup.items():
    maybe_s3_sub_type = s3_sub_type_get_lookup_fn(cursor, s3_type_id)
    if maybe_s3_sub_type is not None:
      s3_sub_type = maybe_s3_sub_type
      s3_subtype_value = types.TaggedS3SubType(
                           # TODO: Why in the heck do I have to ignore this?
                           tag = s3_sub_type_tag,  # type: ignore
                           value = s3_sub_type,
                         )
      return s3_type, s3_subtype_value

  return s3_type, None


