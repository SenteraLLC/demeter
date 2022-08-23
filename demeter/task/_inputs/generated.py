
from ...db._postgres import SQLGenerator as Generator

from . import lookups as _lookups

g = Generator("demeter.task",
              type_table_lookup = _lookups.type_table_lookup,
              data_table_lookup = _lookups.data_table_lookup,
              id_table_lookup = _lookups.id_table_lookup,
              key_table_lookup = _lookups.key_table_lookup,
             )


from .types import *
from ...db._generic_types import GetId, GetTable, ReturnId, ReturnSameKey

getMaybeHTTPTypeId : GetId[HTTPType] = g.getMaybeIdFunction(HTTPType)
getMaybeS3TypeId   : GetId[S3Type]   = g.getMaybeIdFunction(S3Type)

getHTTPType             : GetTable[HTTPType]        = g.getTableFunction(HTTPType)
getS3Object             : GetTable[S3Object]        = g.getTableFunction(S3Object)
getS3TypeBase           : GetTable[S3Type]          = g.getTableFunction(S3Type)
getMaybeS3TypeDataFrame : GetTable[S3TypeDataFrame] = g.getTableFunction(S3TypeDataFrame, "s3_type_id")


insertS3Object   : ReturnId[S3Object] = g.getInsertReturnIdFunction(S3Object)
insertHTTPType   : ReturnId[HTTPType] = g.getInsertReturnIdFunction(HTTPType)
insertS3TypeBase : ReturnId[S3Type]   = g.getInsertReturnIdFunction(S3Type)

insertS3ObjectKey : ReturnSameKey[S3ObjectKey] = g.getInsertReturnSameKeyFunction(S3ObjectKey)

insertS3TypeDataFrame : ReturnSameKey[S3TypeDataFrame] = g.getInsertReturnSameKeyFunction(S3TypeDataFrame)

insertOrGetS3Type = g.partialInsertOrGetId(getMaybeS3TypeId, insertS3TypeBase)
insertOrGetHTTPType = g.partialInsertOrGetId(getMaybeHTTPTypeId, insertHTTPType)

