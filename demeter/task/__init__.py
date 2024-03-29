from ._function import *  # noqa: F401, F403
from ._inputs import *  # noqa: F401, F403
from ._s3_connection import getS3Connection

# from . import _function as function
# from . import _inputs as inputs


__all__ = [
    "getS3Connection",
    # function
    "FunctionType",
    "Function",
    "FunctionId",
    "Parameter",
    "ObservationParameter",
    "HTTPParameter",
    "S3InputParameter",
    "S3OutputParameter",
    "KeywordParameter",
    "S3TypeSignature",
    "FunctionSignature",
    "getMaybeFunctionTypeId",
    "insertFunctionWithMinor",
    "insertFunctionType",
    "insertObservationParameter",
    "insertHTTPParameter",
    "insertS3InputParameter",
    "insertS3OutputParameter",
    "insertKeywordParameter",
    "insertOrGetFunctionType",
    "insertFunction",
    "getLatestFunctionSignature",
    # inputs
    "HTTPVerb",
    "HTTPType",
    "KeywordType",
    "Keyword",
    "S3Type",
    "S3SubType",
    "S3TypeDataFrame",
    "S3Output",
    "S3Object",
    "S3ObjectKey",
    "TaggedS3SubType",
    "getMaybeHTTPTypeId",
    "getMaybeS3TypeId",
    "getHTTPType",
    "getS3Object",
    "getS3TypeBase",
    "getMaybeS3TypeDataFrame",
    "insertS3Object",
    "insertHTTPType",
    "insertS3TypeBase",
    "insertS3ObjectKey",
    "insertS3TypeDataFrame",
    "insertOrGetS3Type",
    "insertOrGetHTTPType",
    "insertOrGetS3TypeDataFrame",
    "insertS3Type",
    "getS3Type",
    "getS3TypeIdByName",
    "getHTTPTypeByName",
    "insertS3ObjectKeys",
    "getS3ObjectByKey",
    "getS3ObjectByKeys",
]

from ._register import register_sql_adapters

register_sql_adapters()
