from .types import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter

from ...db.generic_types import GetId, GetTable, ReturnId, ReturnSameKey, ReturnKey

from . import lookups
from ...db import Generator

g = Generator(type_table_lookup = lookups.type_table_lookup,
              data_table_lookup = lookups.data_table_lookup,
              id_table_lookup = lookups.id_table_lookup,
              key_table_lookup = lookups.key_table_lookup,
             )

getMaybeFunctionTypeId : GetId[FunctionType] = g.getMaybeIdFunction(FunctionType)


insertFunctionWithMinor : ReturnId[Function] = g.getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = g.getInsertReturnIdFunction(FunctionType)


insertLocalParameter : ReturnSameKey[LocalParameter] = g.getInsertReturnKeyFunction(LocalParameter)
insertHTTPParameter : ReturnSameKey[HTTPParameter]   = g.getInsertReturnKeyFunction(HTTPParameter)
insertS3InputParameter : ReturnSameKey[S3InputParameter] = g.getInsertReturnKeyFunction(S3InputParameter)
insertS3OutputParameter : ReturnSameKey[S3OutputParameter] = g.getInsertReturnKeyFunction(S3OutputParameter)
insertKeywordParameter : ReturnSameKey[KeywordParameter] = g.getInsertReturnKeyFunction(KeywordParameter)


insertOrGetFunctionType = g.partialInsertOrGetId(getMaybeFunctionTypeId, insertFunctionType)

