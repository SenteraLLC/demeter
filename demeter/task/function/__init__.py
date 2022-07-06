from ...db.generic_types import GetId, ReturnId, ReturnKey, ReturnSameKey

from ..inputs.types import Keyword, KeywordType

from .types import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter

from .insert_function import insertFunction as insertFunction, \
                             getLatestFunctionSignature as getLatestFunctionSignature

from ...db.type_to_sql import getMaybeIdFunction

getMaybeFunctionTypeId : GetId[FunctionType] = getMaybeIdFunction(FunctionType)


from ...db.type_to_sql import getInsertReturnIdFunction

insertFunctionWithMinor : ReturnId[Function] = getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = getInsertReturnIdFunction(FunctionType)


from ...db.type_to_sql import getInsertReturnKeyFunction

insertLocalParameter : ReturnSameKey[LocalParameter] = getInsertReturnKeyFunction(LocalParameter)
insertHTTPParameter : ReturnSameKey[HTTPParameter]   = getInsertReturnKeyFunction(HTTPParameter)
insertS3InputParameter : ReturnSameKey[S3InputParameter] = getInsertReturnKeyFunction(S3InputParameter)
insertS3OutputParameter : ReturnSameKey[S3OutputParameter] = getInsertReturnKeyFunction(S3OutputParameter)
insertKeywordParameter : ReturnSameKey[KeywordParameter] = getInsertReturnKeyFunction(KeywordParameter)


from ...db.type_to_sql import partialInsertOrGetId

insertOrGetFunctionType = partialInsertOrGetId(getMaybeFunctionTypeId, insertFunctionType)

from .function import insertFunction, getLatestFunctionSignature

from . import types
