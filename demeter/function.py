from typing import Any

from .database.api_protocols import GetId, ReturnId, ReturnKey, ReturnSameKey
from .database.generators import getMaybeIdFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction

from .types.inputs import Keyword, KeywordType

from .types.function import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter

from .function_custom import insertFunction, \
                             getLatestFunctionSignature

getMaybeFunctionTypeId : GetId[FunctionType] = getMaybeIdFunction(FunctionType)

insertFunctionWithMinor : ReturnId[Function] = getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = getInsertReturnIdFunction(FunctionType)

insertLocalParameter : ReturnSameKey[LocalParameter] = getInsertReturnKeyFunction(LocalParameter) # type: ignore
insertHTTPParameter : ReturnSameKey[HTTPParameter]   = getInsertReturnKeyFunction(HTTPParameter) # type: ignore
insertS3InputParameter : ReturnSameKey[S3InputParameter] = getInsertReturnKeyFunction(S3InputParameter) # type: ignore
insertS3OutputParameter : ReturnSameKey[S3OutputParameter] = getInsertReturnKeyFunction(S3OutputParameter) # type: ignore
insertKeywordParameter : ReturnSameKey[KeywordParameter] = getInsertReturnKeyFunction(KeywordParameter) # type: ignore

