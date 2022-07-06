from . import types
from .types import HTTPType, S3Type, S3Object, S3TypeDataFrame, S3ObjectKey


from .s3_type import insertOrGetS3TypeDataFrame as insertOrGetS3TypeDataFrame, \
                     insertS3Type as insertS3Type, \
                     getS3Type as getS3Type, \
                     getS3TypeIdByName as getS3TypeIdByName


from .http import getHTTPByName as getHTTPByName


from .s3_object import insertS3ObjectKeys as insertS3ObjectKeys, \
                       getS3ObjectByKey as getS3ObjectByKey, \
                       getS3ObjectByKeys as getS3ObjectByKeys


from ...db.type_to_sql import getMaybeIdFunction, getTableFunction
from ...db.generic_types import GetId, GetTable

getMaybeHTTPTypeId : GetId[HTTPType] = getMaybeIdFunction(HTTPType)
getMaybeS3TypeId   : GetId[S3Type]   = getMaybeIdFunction(S3Type)

getHTTPType             : GetTable[HTTPType]        = getTableFunction(HTTPType)
getS3Object             : GetTable[S3Object]        = getTableFunction(S3Object)
getS3TypeBase           : GetTable[S3Type]          = getTableFunction(S3Type)
getMaybeS3TypeDataFrame : GetTable[S3TypeDataFrame] = getTableFunction(S3TypeDataFrame, "s3_type_id")


from ...db.type_to_sql import getInsertReturnIdFunction, getInsertReturnSameKeyFunction
from ...db.generic_types import ReturnId, ReturnSameKey

insertS3Object   : ReturnId[S3Object] = getInsertReturnIdFunction(S3Object)
insertHTTPType   : ReturnId[HTTPType] = getInsertReturnIdFunction(HTTPType)
insertS3TypeBase : ReturnId[S3Type]   = getInsertReturnIdFunction(S3Type)

insertS3ObjectKey : ReturnSameKey[S3ObjectKey] = getInsertReturnSameKeyFunction(S3ObjectKey)

insertS3TypeDataFrame : ReturnSameKey[S3TypeDataFrame] = getInsertReturnSameKeyFunction(S3TypeDataFrame)


from ...db.type_to_sql import partialInsertOrGetId

insertOrGetS3Type = partialInsertOrGetId(getMaybeS3TypeId, insertS3TypeBase)
insertOrGetHTTPType = partialInsertOrGetId(getMaybeHTTPTypeId, insertHTTPType)


