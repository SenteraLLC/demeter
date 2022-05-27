from .types.execution import Execution, LocalArgument, HTTPArgument, KeywordArgument, S3InputArgument, S3OutputArgument, ExecutionKey

from .database.api_protocols import ReturnId, ReturnSameKey
from .database.generators import getMaybeIdFunction, getTableFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction

from .execution_custom import getExecutionSummaries, \
                              getExistingExecutions

insertExecution : ReturnId[Execution] = getInsertReturnIdFunction(Execution)

insertLocalArgument    : ReturnSameKey[LocalArgument] = getInsertReturnKeyFunction(LocalArgument) # type: ignore
insertHTTPArgument     : ReturnSameKey[HTTPArgument]   = getInsertReturnKeyFunction(HTTPArgument) # type: ignore
insertKeywordArgument     : ReturnSameKey[KeywordArgument]   = getInsertReturnKeyFunction(KeywordArgument) # type: ignore
insertS3InputArgument  : ReturnSameKey[S3InputArgument] = getInsertReturnKeyFunction(S3InputArgument) # type: ignore
insertS3OutputArgument : ReturnSameKey[S3OutputArgument] = getInsertReturnKeyFunction(S3OutputArgument) # type: ignore
insertExecutionKey : ReturnSameKey[ExecutionKey] = getInsertReturnKeyFunction(ExecutionKey) # type: ignore


