from .types import Execution, LocalArgument, HTTPArgument, KeywordArgument, S3InputArgument, S3OutputArgument, ExecutionKey, Argument

from . import lookups
from ..db import Generator
from ..db.generic_types import ReturnId, ReturnSameKey, ReturnKey

g = Generator(type_table_lookup = lookups.type_table_lookup,
              data_table_lookup = lookups.data_table_lookup,
              id_table_lookup = lookups.id_table_lookup,
              key_table_lookup = lookups.key_table_lookup,
             )

insertExecution       : ReturnId[Execution] = g.getInsertReturnIdFunction(Execution)
insertExecutionKey    : ReturnSameKey[ExecutionKey] = g.getInsertReturnKeyFunction(ExecutionKey)

insertLocalArgument    : ReturnSameKey[LocalArgument] = g.getInsertReturnKeyFunction(LocalArgument)
insertHTTPArgument     : ReturnSameKey[HTTPArgument]   = g.getInsertReturnKeyFunction(HTTPArgument)
insertKeywordArgument  : ReturnSameKey[KeywordArgument]   = g.getInsertReturnKeyFunction(KeywordArgument)
insertS3InputArgument  : ReturnSameKey[S3InputArgument] = g.getInsertReturnKeyFunction(S3InputArgument)
insertS3OutputArgument : ReturnSameKey[S3OutputArgument] = g.getInsertReturnKeyFunction(S3OutputArgument)

from . import union_types
from . import lookups

from .existing import getExecutionSummaries as getExecutionSummaries, \
                      getExistingExecutions as getExistingExecutions


from . import datasource
from .datasource import *
from .datasource.types import *

__all__ = [
  'union_types',
  'lookups',

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

