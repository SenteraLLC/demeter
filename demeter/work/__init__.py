from .types import Execution, LocalArgument, HTTPArgument, KeywordArgument, S3InputArgument, S3OutputArgument, ExecutionKey, Argument

from ..db import ReturnId, ReturnSameKey
from ..db.type_to_sql import getInsertReturnIdFunction, getInsertReturnKeyFunction

insertExecution       : ReturnId[Execution] = getInsertReturnIdFunction(Execution)
insertExecutionKey    : ReturnSameKey[ExecutionKey] = getInsertReturnKeyFunction(ExecutionKey)

insertLocalArgument    : ReturnSameKey[LocalArgument] = getInsertReturnKeyFunction(LocalArgument)
insertHTTPArgument     : ReturnSameKey[HTTPArgument]   = getInsertReturnKeyFunction(HTTPArgument)
insertKeywordArgument  : ReturnSameKey[KeywordArgument]   = getInsertReturnKeyFunction(KeywordArgument)
insertS3InputArgument  : ReturnSameKey[S3InputArgument] = getInsertReturnKeyFunction(S3InputArgument)
insertS3OutputArgument : ReturnSameKey[S3OutputArgument] = getInsertReturnKeyFunction(S3OutputArgument)


from .existing import getExecutionSummaries  as getExecutionSummaries, \
                      getExistingExecutions as getExistingExecutions


from . import datasource
from .datasource import *
from .datasource.types import *

__all__ = [
  'insertExecution',
  'insertExecutionKey',

  'insertLocalArgument',
  'insertHTTPArgument',
  'insertKeywordArgument',
  'insertS3InputArgument',
  'insertS3OutputArgument',

  'getExistingSummaries',
  'getExistingExecutions',

  # datasource
  'DataSource',
  'DataSourceBase',
  'S3File',

  'Argument',
  'LocalArgument',
  'HTTPArgument',
  'S3InputArgument',
  'S3OutputArgument',
  'KeywordArgument',

  'Execution',
  'ExecutionKey',
  'ExecutionOutputs',
  'ExecutionArguments',
  'ExecutionSummary',
]

from typing import Union

AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

from ..db import TypeTable
class _Skip(TypeTable):
  pass
AnyTypeTable = Union[_Skip]

AnyDataTable = Union[Execution]

AnyIdTable = Union[AnyTypeTable, AnyDataTable]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable]

