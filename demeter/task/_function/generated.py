
from ...db._postgres import SQLGenerator as Generator

from . import lookups as _lookups
g = Generator("demeter.task",
              type_table_lookup = _lookups.type_table_lookup,
              data_table_lookup = _lookups.data_table_lookup,
              id_table_lookup = _lookups.id_table_lookup,
              key_table_lookup = _lookups.key_table_lookup,
             )


from ...db._generic_types import GetId, GetTable, ReturnId

from .types import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter

getMaybeFunctionTypeId : GetId[FunctionType] = g.getMaybeIdFunction(FunctionType)


insertFunctionWithMinor : ReturnId[Function] = g.getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = g.getInsertReturnIdFunction(FunctionType)


insertLocalParameter = g.getInsertReturnSameKeyFunction(LocalParameter)
insertHTTPParameter  = g.getInsertReturnSameKeyFunction(HTTPParameter)
insertS3InputParameter = g.getInsertReturnSameKeyFunction(S3InputParameter)
insertS3OutputParameter = g.getInsertReturnSameKeyFunction(S3OutputParameter)
insertKeywordParameter = g.getInsertReturnSameKeyFunction(KeywordParameter)

insertOrGetFunctionType = g.partialInsertOrGetId(getMaybeFunctionTypeId, insertFunctionType)

