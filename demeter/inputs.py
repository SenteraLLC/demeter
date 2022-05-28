from typing import Tuple, Any

from functools import partial as __partial

from .database.api_protocols import GetId, GetTable, ReturnId, ReturnSameKey
from .database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getInsertReturnSameKeyFunction, getTableFunction, insertOrGetType

from .types.inputs import *

from .inputs_custom import getHTTPByName, \
                           insertS3ObjectKeys, \
                           getS3ObjectByKey, \
                           getS3ObjectByKeys, \
                           getS3TypeIdByName

from .local import getLocalType, getMaybeLocalTypeId, insertLocalType, insertOrGetLocalType

getMaybeHTTPTypeId : GetId[HTTPType] = getMaybeIdFunction(HTTPType)
getMaybeS3TypeId   : GetId[S3Type]   = getMaybeIdFunction(S3Type)

getHTTPType             : GetTable[HTTPType]        = getTableFunction(HTTPType)
getS3Object             : GetTable[S3Object]        = getTableFunction(S3Object)
getS3TypeBase           : GetTable[S3Type]          = getTableFunction(S3Type)
getMaybeS3TypeDataFrame : GetTable[S3TypeDataFrame] = getTableFunction(S3TypeDataFrame, "s3_type_id")

insertS3Object   : ReturnId[S3Object] = getInsertReturnIdFunction(S3Object)
insertHTTPType   : ReturnId[HTTPType] = getInsertReturnIdFunction(HTTPType)
insertS3TypeBase : ReturnId[S3Type]   = getInsertReturnIdFunction(S3Type)

insertS3ObjectKey : ReturnSameKey[S3ObjectKey] = getInsertReturnSameKeyFunction(S3ObjectKey)

insertS3TypeDataFrame : ReturnSameKey[S3TypeDataFrame] = getInsertReturnSameKeyFunction(S3TypeDataFrame)

insertOrGetS3Type = __partial(insertOrGetType, getMaybeS3TypeId, insertS3TypeBase)


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


s3_sub_type_get_lookup = {
  S3TypeDataFrame : getMaybeS3TypeDataFrame
}

s3_sub_type_insert_lookup = {
  S3TypeDataFrame : insertS3TypeDataFrame
}

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
                           tag = s3_sub_type_tag,
                           value = s3_sub_type,
                         )
      return s3_type, s3_subtype_value

  return s3_type, None


