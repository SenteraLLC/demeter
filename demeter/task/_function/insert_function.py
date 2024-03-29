from typing import (
    Any,
    Optional,
    Tuple,
)

from ... import db
from .._inputs import KeywordType
from .types import (
    Function,
    FunctionSignature,
    Keyword,
)


def insertFunction(
    cursor: Any,
    function: Function,
) -> Tuple[db.TableId, int]:
    stmt = """with minor as (
              select coalesce(max(minor), -1) + 1 as version
              from function
              where function_name = %(function_name)s and
                    major = %(major)s and
                    function_type_id = %(function_type_id)s
            ) insert into function(function_name, major, minor, function_type_id, created)
              select %(function_name)s, %(major)s, minor.version as minor, %(function_type_id)s, %(created)s
              from minor
              returning function_id, minor
         """
    cursor.execute(stmt, function())
    result = cursor.fetchone()
    function_id = result.function_id
    minor = result.minor
    return function_id, minor


def getLatestFunctionSignature(
    cursor: Any, function: Function
) -> Optional[Tuple[db.TableId, FunctionSignature]]:
    from .._function.getLatestFunctionSignature import stmt

    # stmt = sql.getLatestFunctionSignatureSQL
    cursor.execute(stmt, function())
    result = cursor.fetchone()
    if result is None:
        return None

    keyword_inputs = [
        Keyword(
            keyword_name=k["keyword_name"],
            keyword_type=KeywordType[k["keyword_type"]],
        )
        for k in result.keyword_inputs
    ]
    function_id = result.function_id
    return (
        function_id,
        FunctionSignature(
            name=result.function_name,
            major=result.major,
            observation_inputs=result.observation_inputs,
            keyword_inputs=keyword_inputs,
            s3_inputs=list(zip(result.s3_inputs, result.s3_dataframe_inputs)),
            http_inputs=result.http_inputs,
            s3_outputs=list(zip(result.s3_outputs, result.s3_dataframe_outputs)),
        ),
    )
