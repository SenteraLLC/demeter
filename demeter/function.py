from typing import Any

from .database.api_protocols import GetId, ReturnId, ReturnKey, ReturnSameKey

from .types.inputs import Keyword, KeywordType

from .types.function import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter

from .function_custom import insertFunction as insertFunction, \
                             getLatestFunctionSignature as getLatestFunctionSignature

from .database.generators import getMaybeIdFunction

getMaybeFunctionTypeId : GetId[FunctionType] = getMaybeIdFunction(FunctionType)


from .database.generators import getInsertReturnIdFunction

insertFunctionWithMinor : ReturnId[Function] = getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = getInsertReturnIdFunction(FunctionType)


from .database.generators import getInsertReturnKeyFunction

insertLocalParameter : ReturnSameKey[LocalParameter] = getInsertReturnKeyFunction(LocalParameter)
insertHTTPParameter : ReturnSameKey[HTTPParameter]   = getInsertReturnKeyFunction(HTTPParameter)
insertS3InputParameter : ReturnSameKey[S3InputParameter] = getInsertReturnKeyFunction(S3InputParameter)
insertS3OutputParameter : ReturnSameKey[S3OutputParameter] = getInsertReturnKeyFunction(S3OutputParameter)
insertKeywordParameter : ReturnSameKey[KeywordParameter] = getInsertReturnKeyFunction(KeywordParameter)


from .database.generators import insertOrGetType
from functools import partial as __partial

insertOrGetFunctionType = __partial(insertOrGetType, getMaybeFunctionTypeId, insertFunctionType)
