from ...db import SQLGenerator as Generator
from ...db._generic_types import GetId, ReturnId
from .._function.types import (
    Function,
    FunctionType,
    HTTPParameter,
    KeywordParameter,
    ObservationParameter,
    S3InputParameter,
    S3OutputParameter,
)
from . import lookups as _lookups

g = Generator(
    "demeter.task",
    type_table_lookup=_lookups.type_table_lookup,
    data_table_lookup=_lookups.data_table_lookup,
    id_table_lookup=_lookups.id_table_lookup,
    key_table_lookup=_lookups.key_table_lookup,
)

getMaybeFunctionTypeId: GetId[FunctionType] = g.getMaybeIdFunction(FunctionType)


insertFunctionWithMinor: ReturnId[Function] = g.getInsertReturnIdFunction(Function)
insertFunctionType: ReturnId[FunctionType] = g.getInsertReturnIdFunction(FunctionType)


insertObservationParameter = g.getInsertReturnSameKeyFunction(ObservationParameter)
insertHTTPParameter = g.getInsertReturnSameKeyFunction(HTTPParameter)
insertS3InputParameter = g.getInsertReturnSameKeyFunction(S3InputParameter)
insertS3OutputParameter = g.getInsertReturnSameKeyFunction(S3OutputParameter)
insertKeywordParameter = g.getInsertReturnSameKeyFunction(KeywordParameter)

insertOrGetFunctionType = g.partialInsertOrGetId(
    getMaybeFunctionTypeId, insertFunctionType
)
