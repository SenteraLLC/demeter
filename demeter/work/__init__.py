from ..db import SQLGenerator
from . import _lookups as lookups

g = SQLGenerator(
    type_table_lookup=lookups.type_table_lookup,
    data_table_lookup=lookups.data_table_lookup,
    id_table_lookup=lookups.id_table_lookup,
    key_table_lookup=lookups.key_table_lookup,
)


from ..db._generic_types import ReturnId, ReturnKey, ReturnSameKey
from ._types import (
    Argument,
    Execution,
    ExecutionKey,
    HTTPArgument,
    KeywordArgument,
    ObservationArgument,
    S3InputArgument,
    S3OutputArgument,
)

insertExecution: ReturnId[Execution] = g.getInsertReturnIdFunction(Execution)
insertExecutionKey = g.getInsertReturnSameKeyFunction(ExecutionKey)

insertObservationArgument = g.getInsertReturnSameKeyFunction(ObservationArgument)
insertHTTPArgument = g.getInsertReturnSameKeyFunction(HTTPArgument)
insertKeywordArgument = g.getInsertReturnSameKeyFunction(KeywordArgument)
insertS3InputArgument = g.getInsertReturnSameKeyFunction(S3InputArgument)
insertS3OutputArgument = g.getInsertReturnSameKeyFunction(S3OutputArgument)

from ._datasource import (
    DataSource,
    LocalFile,
    OneToManyResponseFunction,
    OneToOneResponseFunction,
    S3File,
)
from ._existing import getExecutionSummaries, getExistingExecutions
from ._transformation import Transformation
from ._types import *

__all__ = [
    "insertExecution",
    "insertExecutionKey",
    "insertObservationArgument",
    "insertHTTPArgument",
    "insertKeywordArgument",
    "insertS3InputArgument",
    "insertS3OutputArgument",
    "getExistingSummaries",
    "getExistingExecutions",
    # datasource
    "DataSource",
    "S3File",
    "LocalFile",
    "Argument",
    "ObservationArgument",
    "HTTPArgument",
    "S3InputArgument",
    "S3OutputArgument",
    "KeywordArgument",
    "Execution",
    "ExecutionKey",
    "ExecutionOutputs",
    "ExecutionArguments",
    "ExecutionSummary",
    "Transformation",
    "OneToOneResponseFunction",
    "OneToManyResponseFunction",
]
