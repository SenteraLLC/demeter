import psycopg2
import argparse
import json
import sys

from datetime import date
from shapely.geometry import LineString # type: ignore
from functools import partial
from collections import OrderedDict

from typing import TypedDict, Any, List, Tuple, Dict, Callable, Optional, Type, TypeVar, Set, Union, Sequence, Literal
from typing import cast

from .generators import *
from . import types
from psycopg2 import sql, extras

psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


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
insertFunctionWithMinor : ReturnId[types.Function] = getInsertReturnIdFunction(types.Function)
insertFunctionType : ReturnId[types.FunctionType] = getInsertReturnIdFunction(types.FunctionType)
insertExecution : ReturnId[types.Execution] = getInsertReturnIdFunction(types.Execution)


def insertFunction(cursor : Any,
                   function : types.Function,
                  ) -> Tuple[int, int]:
  stmt = """insert into function(function_name, major, function_type_id, created)
            values(%(function_name)s, %(major)s, %(function_type_id)s, %(created)s)
            returning function_id, minor"""
  cursor.execute(stmt, function)
  result = cursor.fetchone()
  function_id = result["function_id"]
  minor = result["minor"]
  return function_id, minor


def getLatestFunctionSignature(cursor : Any,
                               function : types.Function
                              ) -> Optional[Tuple[int, types.FunctionSignature]]:
  stmt = """
    with latest_function as (
      select *
      from function F
      where F.function_name = %(function_name)s and F.major = %(major)s and F.function_type_id = %(function_type_id)s
      order by minor desc
      limit 1

    ), local_inputs as (
      select F.function_id,
             jsonb_agg(
               jsonb_build_object('type_name', LT.type_name,
                                  'type_category', LT.type_category
                                 )
             ) as local_types
      from latest_function F
      join local_parameter LP on F.function_id = LP.function_id
      join local_type LT on LP.local_type_id = LT.local_type_id
      group by F.function_id

    ), keyword_inputs as (
      select F.function_id,
             jsonb_agg(
               jsonb_build_object('keyword_name', K.keyword_name,
                                  'keyword_type', K.keyword_type
                                 )
             ) as keyword_types
      from latest_function F
      join keyword_parameter K on K.function_id = F.function_id
      group by F.function_id

    ), s3_inputs as (
      select F.function_id,
             jsonb_agg(
               jsonb_build_object('type_name', S3T.type_name)
             ) as s3_types,
             jsonb_agg(
               jsonb_build_object('driver', S3TD.driver,
                                  'has_geometry', S3TD.has_geometry
                                 )
              ) as s3_dataframe_types
      from latest_function F
      join s3_input_parameter S3I on F.function_id = S3I.function_id
      join s3_type S3T on S3I.s3_type_id = S3T.s3_type_id
      left join s3_type_dataframe S3TD on S3T.s3_type_id = S3TD.s3_type_id
      group by F.function_id

    ), http_inputs as (
      select F.function_id,
             jsonb_agg(
               jsonb_build_object('type_name', HT.type_name,
                                  'verb', HT.verb,
                                  'uri', HT.uri,
                                  'uri_parameters', HT.uri_parameters,
                                  'request_body_schema', HT.request_body_schema
                                 )
             ) as http_types
      from latest_function F
      join http_parameter HP on F.function_id = HP.function_id
      join http_type HT on HP.http_type_id = HT.http_type_id
      group by F.function_id

    ), s3_outputs as (
      select F.function_id,
             jsonb_agg(
               jsonb_build_object('type_name', S3T.type_name)
             ) as s3_types,
             jsonb_agg(
               jsonb_build_object('driver', S3TD.driver,
                                  'has_geometry', S3TD.has_geometry
                                 )
              ) as s3_dataframe_types
      from latest_function F
      join s3_output_parameter S3O on F.function_id = S3O.function_id
      join s3_type S3T on S3O.s3_type_id = S3T.s3_type_id
      left join s3_type_dataframe S3TD on S3T.s3_type_id = S3TD.s3_type_id
      group by F.function_id

    ) select F.function_id, F.function_name, F.major, F.minor,
             coalesce(LI.local_types, '[]'::jsonb) as local_inputs,
             coalesce(K.keyword_types, '[]'::jsonb) as keyword_inputs,
             coalesce(S3I.s3_types, '[]'::jsonb) as s3_inputs,
             coalesce(S3I.s3_dataframe_types, '[]'::jsonb) as s3_dataframe_inputs,
             coalesce(HI.http_types, '[]'::jsonb) as http_inputs,
             coalesce(S3O.s3_types, '[]'::jsonb) as s3_outputs,
             coalesce(S3I.s3_dataframe_types, '[]'::jsonb) as s3_dataframe_outputs
      from latest_function F
      left join local_inputs LI on F.function_id = LI.function_id
      left join keyword_inputs K on F.function_id = K.function_id
      left join http_inputs HI on F.function_id = HI.function_id
      left join s3_inputs S3I on F.function_id = S3I.function_id
      left join s3_outputs S3O on F.function_id = S3O.function_id
"""

  cursor.execute(stmt, function)
  result = cursor.fetchone()
  if result is None:
    return None

  keyword_inputs = [types.Keyword(
                      keyword_name = k["keyword_name"],
                      keyword_type = types.KeywordType[k["keyword_type"]],
                    ) for k in result["keyword_inputs"]
                   ]

  function_id = result["function_id"]
  return (function_id,
          types.FunctionSignature(
            name = result["function_name"],
            major = result["major"],
            local_inputs = result["local_inputs"],
            keyword_inputs = keyword_inputs,
            s3_inputs = list(zip(result["s3_inputs"], result["s3_dataframe_inputs"])),
            http_inputs = result["http_inputs"],
            s3_outputs = list(zip(result["s3_outputs"], result["s3_dataframe_outputs"])),
          )
        )


def getExecutionSummaries(cursor : Any,
                          function_id : int,
                          execution_id : int,
                         ) -> List[types.ExecutionSummary]:
  stmt = """
         select json_agg(K.*) as execution_keys,
                json_agg(L.*) as local_arguments,
                json_agg(H.*) as http_arguments,
                json_agg(SI.*) as s3_input_arguments,
                json_agg(SO.*) as s3_output_arguments
         from execution E
         join execution_key K on E.execution_id = K.execution_id
         join local_argument L on E.execution_id = L.execution_id
         join http_argument H on E.execution_id = H.execution_id
         join s3_input_argument SI on E.execution_id = SI.execution_id
         join s3_output_argument SO on E.execution_id = SO.execution_id
         where E.function_id = %(function_id)s
         group by E.execution_id
         """

  cursor.execute(stmt, function_id)
  result = cursor.fetchall()

  return [cast(types.ExecutionSummary, dict(r)) for r in result]


# TODO: Fix typing issues here
S = TypeVar('S', bound=types.AnyKeyTable)
SK = TypeVar('SK', bound=types.Key)
ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]

insertPlanting     : ReturnKey[types.Planting, types.PlantingKey] = getInsertReturnKeyFunction(types.Planting) # type: ignore
insertHarvest      : ReturnKey[types.Harvest, types.HarvestKey] = getInsertReturnKeyFunction(types.Harvest) # type: ignore
insertCropProgress : ReturnKey[types.CropProgress, types.CropProgressKey] = getInsertReturnKeyFunction(types.CropProgress) # type: ignore
insertS3ObjectKey : ReturnKey[types.S3ObjectKey, types.S3ObjectKey] = getInsertReturnKeyFunction(types.S3ObjectKey) # type: ignore
insertLocalParameter : ReturnSameKey[types.LocalParameter] = getInsertReturnKeyFunction(types.LocalParameter) # type: ignore
insertHTTPParameter : ReturnSameKey[types.HTTPParameter]   = getInsertReturnKeyFunction(types.HTTPParameter) # type: ignore
insertS3InputParameter : ReturnSameKey[types.S3InputParameter] = getInsertReturnKeyFunction(types.S3InputParameter) # type: ignore
insertS3OutputParameter : ReturnSameKey[types.S3OutputParameter] = getInsertReturnKeyFunction(types.S3OutputParameter) # type: ignore
insertKeywordParameter : ReturnSameKey[types.KeywordParameter] = getInsertReturnKeyFunction(types.KeywordParameter) # type: ignore
insertLocalArgument    : ReturnSameKey[types.LocalArgument] = getInsertReturnKeyFunction(types.LocalArgument) # type: ignore
insertHTTPArgument     : ReturnSameKey[types.HTTPArgument]   = getInsertReturnKeyFunction(types.HTTPArgument) # type: ignore
insertKeywordArgument     : ReturnSameKey[types.KeywordArgument]   = getInsertReturnKeyFunction(types.KeywordArgument) # type: ignore
insertS3InputArgument  : ReturnSameKey[types.S3InputArgument] = getInsertReturnKeyFunction(types.S3InputArgument) # type: ignore
insertS3OutputArgument : ReturnSameKey[types.S3OutputArgument] = getInsertReturnKeyFunction(types.S3OutputArgument) # type: ignore
insertExecutionKey : ReturnSameKey[types.ExecutionKey] = getInsertReturnKeyFunction(types.ExecutionKey) # type: ignore


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
getMaybeLocalValue       : GetId[types.LocalValue] = getMaybeIdFunction(types.LocalValue)
getMaybeOwnerId          : GetId[types.Owner]      = getMaybeIdFunction(types.Owner)
getMaybeGrowerId         : GetId[types.Grower]      = getMaybeIdFunction(types.Grower)
getMaybeGeoSpatialKeyId  : GetId[types.GeoSpatialKey] = getMaybeIdFunction(types.GeoSpatialKey)
getMaybeTemporalKeyId    : GetId[types.TemporalKey] = getMaybeIdFunction(types.TemporalKey)


getMaybeUnitTypeId   : GetId[types.UnitType]   = getMaybeIdFunction(types.UnitType)
getMaybeLocalTypeId  : GetId[types.LocalType]  = getMaybeIdFunction(types.LocalType)
getMaybeCropTypeId   : GetId[types.CropType]   = getMaybeIdFunction(types.CropType)
getMaybeCropStageId  : GetId[types.CropStage]  = getMaybeIdFunction(types.CropStage)
getMaybeReportTypeId : GetId[types.ReportType] = getMaybeIdFunction(types.ReportType)
getMaybeLocalGroupId : GetId[types.LocalGroup] = getMaybeIdFunction(types.LocalGroup)
getMaybeHTTPTypeId   : GetId[types.HTTPType]   = getMaybeIdFunction(types.HTTPType)
getMaybeS3TypeId     : GetId[types.S3Type]     = getMaybeIdFunction(types.S3Type)
getMaybeFunctionTypeId : GetId[types.FunctionType] = getMaybeIdFunction(types.FunctionType)


V = TypeVar('V', bound=types.AnyIdTable)
GetTable = Callable[[Any, int], V]

getField      : GetTable[types.Field]    = getTableFunction(types.Field)
getOwner      : GetTable[types.Owner]    = getTableFunction(types.Owner)
getGeom       : GetTable[types.Geom]     = getTableFunction(types.Geom)
getHTTPType   : GetTable[types.HTTPType] = getTableFunction(types.HTTPType)
getS3Object   : GetTable[types.S3Object] = getTableFunction(types.S3Object)
getLocalType  : GetTable[types.LocalType] = getTableFunction(types.LocalType)

getS3TypeBase           : GetTable[types.S3Type]   = getTableFunction(types.S3Type)
getMaybeS3TypeDataFrame : GetTable[types.S3TypeDataFrame] = getTableFunction(types.S3TypeDataFrame, "s3_type_id")

s3_sub_type_get_lookup = {
  types.S3TypeDataFrame : getMaybeS3TypeDataFrame
}


def getHTTPByName(cursor : Any, http_type_name : str) -> Tuple[int, types.HTTPType]:
  stmt = """select * from http_type where type_name = %(name)s"""
  cursor.execute(stmt, { 'name' : http_type_name })

  results = cursor.fetchall()
  if len(results) != 1:
    raise Exception(f"HTTP Type does exist for {http_type_name}")
  result_with_id = results[0]

  http_type_id = result_with_id["http_type_id"]
  result_with_id["verb"] = types.stringToHTTPVerb(result_with_id["verb"])
  del result_with_id["http_type_id"]
  http_type = cast(types.HTTPType, result_with_id)

  return http_type_id, http_type


def getS3ObjectByKey(cursor         : Any,
                     k              : types.Key,
                    ) -> Optional[Tuple[int, types.S3Object]]:
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
  s3_object_id = result_with_id.pop("s3_object_id")
  s3_object = result_with_id

  return s3_object_id, cast(types.S3Object, s3_object)


def getS3ObjectByKeys(cursor    : Any,
                      keys      : List[types.Key],
                      type_name : str,
                     ) -> Optional[Tuple[int, types.S3Object]]:
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
    return None

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
                           tag = s3_sub_type_tag,  # type: ignore
                           value = s3_sub_type,
                         )
      return s3_type, s3_subtype_value

  return s3_type, None



def getExistingExecutions(cursor : Any,
                          function_id : int,
                         ) -> Sequence[types.ExecutionSummary]:
  stmt = """
    with local_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from local_argument A
      join local_type T on T.local_type_id = A.local_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), keyword_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from keyword_argument A
      join keyword_parameter P on P.keyword_name = A.keyword_name and P.function_id = A.function_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), s3_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from s3_input_argument A
      join s3_type T on T.s3_type_id = A.s3_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), http_arguments as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as arguments
      from http_argument A
      join http_type T on T.http_type_id = A.http_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), s3_outputs as (
      select execution_id,
             coalesce(
               jsonb_agg(A),
               '[]'::jsonb
             ) as outputs
      from s3_output_argument A
      join s3_type T on T.s3_type_id = A.s3_type_id
      where A.function_id = %(function_id)s
      group by A.execution_id

    ), keys as (
       select EK.execution_id,
              E.function_id,
              jsonb_agg(
                jsonb_build_object('geospatial_key_id', GK.geospatial_key_id,
                                   'temporal_key_id', TK.temporal_key_id,
                                   'geom_id', GK.geom_id,
                                   'field_id', GK.field_id,
                                   'start_date', TK.start_date,
                                   'end_date', TK.end_date
                                  )
              ) as keys
      from execution_key EK
      join (select distinct execution_id from s3_outputs) D on D.execution_id = EK.execution_id
      join execution E on E.execution_id = EK.execution_id
      join geospatial_key GK on GK.geospatial_key_id = EK.geospatial_key_id
      join temporal_key TK on TK.temporal_key_id = EK.temporal_key_id
      group by EK.execution_id, E.function_id

    ) select K.execution_id,
             K.function_id,
             jsonb_build_object('local',   coalesce(L.arguments, '[]'::jsonb),
                                'keyword', coalesce(KW.arguments, '[]'::jsonb),
                                's3',      coalesce(S3I.arguments, '[]'::jsonb),
                                'http',    coalesce(H.arguments, '[]'::jsonb),
                                'keys',    K.keys
                               ) as inputs,
             jsonb_build_object('s3', S3O.outputs) as outputs
      from s3_outputs S3O
      left join local_arguments L on S3O.execution_id = L.execution_id
      left join keyword_arguments KW on S3O.execution_id = KW.execution_id
      left join http_arguments H on S3O.execution_id = H.execution_id
      left join s3_arguments S3I on S3O.execution_id = S3I.execution_id
      join keys K on S3O.execution_id = K.execution_id
      order by L.execution_id desc
    """

  cursor.execute(stmt, {"function_id": function_id})
  results = cursor.fetchall()
  return cast(Sequence[types.ExecutionSummary], results)

