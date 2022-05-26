from typing import Any, Optional, Tuple, List, Sequence
from typing import cast

import os.path

from .types import Execution, ExecutionSummary, LocalArgument, HTTPArgument, KeywordArgument, S3InputArgument, S3OutputArgument, ExecutionKey

from ..database.api_protocols import GetId, GetTable, ReturnId, ReturnKey, ReturnSameKey
from ..database.generators import getMaybeIdFunction, getTableFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction


insertExecution : ReturnId[Execution] = getInsertReturnIdFunction(Execution)

insertLocalArgument    : ReturnSameKey[LocalArgument] = getInsertReturnKeyFunction(LocalArgument) # type: ignore
insertHTTPArgument     : ReturnSameKey[HTTPArgument]   = getInsertReturnKeyFunction(HTTPArgument) # type: ignore
insertKeywordArgument     : ReturnSameKey[KeywordArgument]   = getInsertReturnKeyFunction(KeywordArgument) # type: ignore
insertS3InputArgument  : ReturnSameKey[S3InputArgument] = getInsertReturnKeyFunction(S3InputArgument) # type: ignore
insertS3OutputArgument : ReturnSameKey[S3OutputArgument] = getInsertReturnKeyFunction(S3OutputArgument) # type: ignore
insertExecutionKey : ReturnSameKey[ExecutionKey] = getInsertReturnKeyFunction(ExecutionKey) # type: ignore

def openRelative(path : str):
  return open(os.path.join(os.path.dirname(__file__), path))

def getExecutionSummaries(cursor : Any,
                          function_id : int,
                          execution_id : int,
                         ) -> List[ExecutionSummary]:
  f = openRelative("./sql/getExecutionSummarires.sql")
  stmt = f.read()

  cursor.execute(stmt, function_id)
  result = cursor.fetchall()

  return [cast(ExecutionSummary, dict(r)) for r in result]


def getExistingExecutions(cursor : Any,
                          function_id : int,
                         ) -> Sequence[ExecutionSummary]:
  f = openRelative("./sql/getExistingExecutions.sql")
  stmt = f.read()

  cursor.execute(stmt, {"function_id": function_id})
  results = cursor.fetchall()
  return cast(Sequence[ExecutionSummary], results)

