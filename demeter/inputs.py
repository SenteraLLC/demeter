from typing import Tuple, Any, Optional, Mapping, Type, Callable, TypeVar

from functools import partial as __partial

from .database.types_protocols import TableId
from .database.api_protocols import GetId, GetTable, ReturnId, ReturnSameKey, SomeKey
from .database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getInsertReturnSameKeyFunction, getTableFunction, partialInsertOrGetId

from .types.inputs import HTTPType, S3Object, S3Type, S3TypeDataFrame, S3ObjectKey, S3SubType, TaggedS3SubType

from .inputs_custom \
import getHTTPByName      as getHTTPByName, \
       insertS3ObjectKeys as insertS3ObjectKeys, \
       getS3ObjectByKey   as getS3ObjectByKey, \
       getS3ObjectByKeys  as getS3ObjectByKeys, \
       getS3TypeIdByName  as getS3TypeIdByName \

from .local import getLocalType as getLocalType
from .local import getMaybeLocalTypeId, insertLocalType, insertOrGetLocalType

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

insertOrGetS3Type = partialInsertOrGetId(getMaybeS3TypeId, insertS3TypeBase)
insertOrGetHTTPType = partialInsertOrGetId(getMaybeHTTPTypeId, insertHTTPType)


def insertOrGetS3TypeDataFrame(cursor : Any,
                               s3_type : S3Type,
                               s3_type_dataframe : S3TypeDataFrame,
                              ) -> TableId:
  s3_type_id = insertOrGetS3Type(cursor, s3_type)
  stmt = """insert into s3_type_dataframe(s3_type_id, driver, has_geometry)
            values(%(s3_type_id)s, %(driver)s, %(has_geometry)s)
            on conflict do nothing"""
  args = {"s3_type_id"   : s3_type_id,
          "driver"       : s3_type_dataframe.driver,
          "has_geometry" : s3_type_dataframe.has_geometry,
         }
  cursor.execute(stmt, args)

  return s3_type_id

s3_sub_type_get_lookup : Mapping[Type[S3SubType], Callable[[Any, TableId], S3SubType]] = {
  S3TypeDataFrame : getMaybeS3TypeDataFrame
}

s3_sub_type_insert_lookup : Mapping[Type[S3SubType], Callable[[Any, Any], SomeKey]] = {
  S3TypeDataFrame : insertS3TypeDataFrame
}

def insertS3Type(cursor : Any,
                 s3_type : S3Type,
                 s3_sub_type : Optional[S3SubType],
                ) -> TableId:
  s3_type_id = insertOrGetS3Type(cursor, s3_type)
  if s3_sub_type is not None:
    sub_type_insert_fn = s3_sub_type_insert_lookup[type(s3_sub_type)]
    s3_sub_type_key = sub_type_insert_fn(cursor, s3_sub_type)
  return s3_type_id


def getS3Type(cursor : Any,
              s3_type_id : TableId,
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


