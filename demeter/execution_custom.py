from typing import List, Sequence, Any
from typing import cast

import os.path

from .types.execution import ExecutionSummary

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

