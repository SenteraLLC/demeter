from typing import Tuple, Any, List
from typing import cast

import sys
from functools import partial

from psycopg2 import sql


from ..core.types import Key
from ..local.api import getMaybeLocalValue, insertLocalValue, getLocalType
from ..database.api_protocols import GetId, GetTable, ReturnId, ReturnKey
from ..database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction, getTableFunction, generateInsertMany, insertOrGetType

from .types import *


getMaybeHTTPTypeId : GetId[HTTPType] = getMaybeIdFunction(HTTPType)
getMaybeS3TypeId   : GetId[S3Type]   = getMaybeIdFunction(S3Type)

getHTTPType             : GetTable[HTTPType]        = getTableFunction(HTTPType)
getS3Object             : GetTable[S3Object]        = getTableFunction(S3Object)
getS3TypeBase           : GetTable[S3Type]          = getTableFunction(S3Type)
getMaybeS3TypeDataFrame : GetTable[S3TypeDataFrame] = getTableFunction(S3TypeDataFrame, "s3_type_id")

s3_sub_type_get_lookup = {
  S3TypeDataFrame : getMaybeS3TypeDataFrame
}

insertS3Object   : ReturnId[S3Object] = getInsertReturnIdFunction(S3Object)
insertHTTPType   : ReturnId[HTTPType] = getInsertReturnIdFunction(HTTPType)
insertS3TypeBase : ReturnId[S3Type]   = getInsertReturnIdFunction(S3Type)

insertS3ObjectKey : ReturnKey[S3ObjectKey, S3ObjectKey] = getInsertReturnKeyFunction(S3ObjectKey) # type: ignore

insertS3TypeDataFrame : ReturnKey[S3TypeDataFrame, S3TypeDataFrame] = getInsertReturnKeyFunction(S3TypeDataFrame) # type: ignore

s3_sub_type_insert_lookup = {
  S3TypeDataFrame : insertS3TypeDataFrame
}

insertOrGetS3Type = partial(insertOrGetType, getMaybeS3TypeId, insertS3TypeBase)


def stringToHTTPVerb(s : str):
  return HTTPVerb[s.upper()]

def getHTTPByName(cursor : Any, http_type_name : str) -> Tuple[int, HTTPType]:
  stmt = """select * from http_type where type_name = %(name)s"""
  cursor.execute(stmt, { 'name' : http_type_name })

  results = cursor.fetchall()
  if len(results) != 1:
    raise Exception(f"HTTP Type does exist for {http_type_name}")
  result_with_id = results[0]

  http_type_id = result_with_id["http_type_id"]
  result_with_id["verb"] = stringToHTTPVerb(result_with_id["verb"])
  del result_with_id["http_type_id"]
  http_type = cast(HTTPType, result_with_id)

  return http_type_id, http_type


def insertS3ObjectKeys(cursor       : Any,
                       s3_object_id : int,
                       keys         : List[Key],
                       s3_type_id   : int,
                      ) -> bool:
  s3_key_names = list(S3ObjectKey.__annotations__.keys())
  stmt = generateInsertMany("s3_object_key", s3_key_names, len(keys))
  args = []
  for k in keys:
    s3_object_key = S3ObjectKey(
                      s3_object_id      = s3_object_id,
                      geospatial_key_id = k.geospatial_key_id,
                      temporal_key_id   = k.temporal_key_id,
                    )
    for key_name in s3_key_names:
      args.append(s3_object_key[key_name])  # type: ignore
  results = cursor.execute(stmt, args)
  return True


def getS3ObjectByKey(cursor         : Any,
                     k              : Key,
                    ) -> Optional[Tuple[int, S3Object]]:
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

  return s3_object_id, cast(S3Object, s3_object)


def getS3ObjectByKeys(cursor    : Any,
                      keys      : List[Key],
                      type_name : str,
                     ) -> Optional[Tuple[int, S3Object]]:
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
    args.append(k.geospatial_key_id)
    args.append(k.temporal_key_id)

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

  s3_object = S3Object(
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


def insertOrGetS3TypeDataFrame(cursor : Any,
                               s3_type : S3Type,
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
                 s3_type : S3Type,
                 s3_sub_type : Optional[S3SubType],
                ) -> int:
  s3_type_id = insertOrGetS3Type(cursor, s3_type)
  if s3_sub_type is not None:
    sub_type_insert_fn = s3_sub_type_insert_lookup[type(s3_sub_type)]
    s3_sub_type_key = sub_type_insert_fn(cursor, s3_sub_type)
  return s3_type_id


def getS3Type(cursor : Any,
              s3_type_id : int,
             ) -> Tuple[S3Type, Optional[TaggedS3SubType]]:
  s3_type = getS3TypeBase(cursor, s3_type_id)
  maybe_s3_sub_type = None
  for s3_sub_type_tag, s3_sub_type_get_lookup_fn in s3_sub_type_get_lookup.items():
    maybe_s3_sub_type = s3_sub_type_get_lookup_fn(cursor, s3_type_id)
    if maybe_s3_sub_type is not None:
      s3_sub_type = maybe_s3_sub_type
      s3_subtype_value = TaggedS3SubType(
                           tag = s3_sub_type_tag,  # type: ignore
                           value = s3_sub_type,
                         )
      return s3_type, s3_subtype_value

  return s3_type, None


