import os
from io import TextIOWrapper
from typing import (Any, Callable, List, Mapping, Optional, Sequence, Tuple,
                    cast)

from .. import data, db
from ._types import (ExecutionArguments, ExecutionOutputs, ExecutionSummary,
                     HTTPArgument, KeywordArgument, LocalArgument,
                     S3InputArgument, S3OutputArgument)

this_dir = os.path.dirname(__file__)
open_sql: Callable[[str], TextIOWrapper] = lambda filename: open(
    os.path.join(this_dir, filename)
)


# TODO: Add more ways to tune "matching"
#         There could be some keyword meta-arguments for tuning
#         the duplicate detection heuristics
def getExistingDuplicate(
    existing_executions: Sequence[ExecutionSummary],
    execution_summary: ExecutionSummary,
) -> Optional[ExecutionSummary]:
    # Allow matching on minor versions of a function
    blacklist = ["function_id", "execution_id"]

    def eq(
        left: Any,
        right: Any,
    ) -> bool:
        for left_key, left_value in left.items():
            if left_key in blacklist:
                continue
            try:
                right_value = right.get(left_key)
            except KeyError:
                right_value = None
            if all(type(v) == dict for v in [left_value, right_value]):
                nested_eq = eq(left_value, right_value)
                if not nested_eq:
                    return nested_eq
            elif all(type(v) == list for v in [left_value, right_value]):
                if len(left_value) != len(right_value):
                    return False
                left_sorted = sorted(left_value)
                right_sorted = sorted(right_value)

                for l, r in zip(left_sorted, right_sorted):
                    nested_eq = eq(l, r)
                    if not nested_eq:
                        return nested_eq
            elif left_value != right_value:
                return False
        return True

    for e in existing_executions:
        inputs = e.inputs
        if eq(inputs, execution_summary.inputs):
            return e
    return None


def getExecutionSummaries(
    cursor: Any,
    function_id: db.TableId,
    execution_id: db.TableId,
) -> List[ExecutionSummary]:
    f = open_sql("getExecutionSummaries.sql")
    stmt = f.read()

    cursor.execute(stmt, {"function_id": function_id})
    result = cursor.fetchall()

    return [cast(ExecutionSummary, dict(r)) for r in result]


def getExistingExecutions(
    cursor: Any,
    function_id: db.TableId,
) -> Sequence[ExecutionSummary]:
    f = open_sql("getExistingExecutions.sql")
    stmt = f.read()

    cursor.execute(stmt, {"function_id": function_id})
    results = cursor.fetchall()

    return [
        ExecutionSummary(
            execution_id=r.execution_id,
            function_id=r.function_id,
            inputs=ExecutionArguments(
                local=[
                    LocalArgument(
                        **l,
                    )
                    for l in r.inputs["local"]
                ],
                keyword=[
                    KeywordArgument(
                        **k,
                    )
                    for k in r.inputs["keyword"]
                ],
                http=[
                    HTTPArgument(
                        **h,
                    )
                    for h in r.inputs["http"]
                ],
                s3=[
                    S3InputArgument(
                        **s,
                    )
                    for s in r.inputs["s3"]
                ],
                keys=[
                    data.Key(
                        **k,
                    )
                    for k in r.inputs["keys"]
                ],
            ),
            outputs=ExecutionOutputs(
                s3=[S3OutputArgument(**s) for s in r.outputs["s3"]]
            ),
        )
        for r in results
    ]
