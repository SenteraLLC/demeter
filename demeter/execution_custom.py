from typing import List, Sequence, Any
from typing import cast

from .util.sql import openRelative

from .types.execution import ExecutionSummary, Key
from .types.execution import ExecutionArguments, LocalArgument, HTTPArgument, S3InputArgument, KeywordArgument, S3OutputArgument
from .types.execution import ExecutionOutputs, S3OutputArgument

from .database.types_protocols import TableId

def getExecutionSummaries(cursor : Any,
                          function_id : TableId,
                          execution_id : TableId,
                         ) -> List[ExecutionSummary]:
  f = openRelative("getExecutionSummaries.sql")
  stmt = f.read()

  cursor.execute(stmt, function_id)
  result = cursor.fetchall()

  return [cast(ExecutionSummary, dict(r)) for r in result]


def getExistingExecutions(cursor : Any,
                          function_id : TableId,
                         ) -> Sequence[ExecutionSummary]:
  f = openRelative("getExistingExecutions.sql")
  stmt = f.read()

  cursor.execute(stmt, {"function_id": function_id})
  results = cursor.fetchall()

  return [ExecutionSummary(
            execution_id = r.execution_id,
            function_id = r.function_id,
            inputs = ExecutionArguments(
                       local   = [LocalArgument(
                                    **l,
                                  ) for l in r.inputs["local"]
                                 ],
                       keyword = [KeywordArgument(
                                    **k,
                                  ) for k in r.inputs["keyword"]
                                 ],
                       http    = [HTTPArgument(
                                    **h,
                                  ) for h in r.inputs["http"]
                                 ],
                       s3      = [S3InputArgument(
                                    **s,
                                  ) for s in r.inputs["s3"]
                                 ],
                       keys    = [Key(
                                    **k,
                                  ) for k in r.inputs["keys"]
                                 ],
                      ),
            outputs = ExecutionOutputs(
              s3 = [S3OutputArgument(
                      **s
                    ) for s in r.outputs["s3"]
                   ]
            ),
          ) for r in results
         ]

