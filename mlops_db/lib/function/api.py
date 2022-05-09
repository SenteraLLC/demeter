from typing import Any, Tuple, Optional

import os.path

from ..util.api_protocols import GetId, GetTable, ReturnId, ReturnKey, ReturnSameKey
from ..util.generators import getMaybeIdFunction, getInsertReturnIdFunction, getInsertReturnKeyFunction

from ..inputs.types import Keyword, KeywordType

from .types import Function, FunctionSignature, FunctionType, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter


getMaybeFunctionTypeId : GetId[FunctionType] = getMaybeIdFunction(FunctionType)

insertFunctionWithMinor : ReturnId[Function] = getInsertReturnIdFunction(Function)
insertFunctionType : ReturnId[FunctionType] = getInsertReturnIdFunction(FunctionType)

insertLocalParameter : ReturnSameKey[LocalParameter] = getInsertReturnKeyFunction(LocalParameter) # type: ignore
insertHTTPParameter : ReturnSameKey[HTTPParameter]   = getInsertReturnKeyFunction(HTTPParameter) # type: ignore
insertS3InputParameter : ReturnSameKey[S3InputParameter] = getInsertReturnKeyFunction(S3InputParameter) # type: ignore
insertS3OutputParameter : ReturnSameKey[S3OutputParameter] = getInsertReturnKeyFunction(S3OutputParameter) # type: ignore
insertKeywordParameter : ReturnSameKey[KeywordParameter] = getInsertReturnKeyFunction(KeywordParameter) # type: ignore


def insertFunction(cursor : Any,
                   function : Function,
                  ) -> Tuple[int, int]:
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
  cursor.execute(stmt, function)
  result = cursor.fetchone()
  function_id = result["function_id"]
  minor = result["minor"]
  return function_id, minor


def openRelative(path : str):
  return open(os.path.join(os.path.dirname(__file__), path))


def getLatestFunctionSignature(cursor : Any,
                               fn : Function
                              ) -> Optional[Tuple[int, FunctionSignature]]:
  f = openRelative("./sql/getLatestFunctionSignature.sql")
  stmt = f.read()

  cursor.execute(stmt, fn)
  result = cursor.fetchone()
  if result is None:
    return None

  keyword_inputs = [Keyword(
                      keyword_name = k["keyword_name"],
                      keyword_type = KeywordType[k["keyword_type"]],
                    ) for k in result["keyword_inputs"]
                   ]

  function_id = result["function_id"]
  return (function_id,
          FunctionSignature(
            name = result["function_name"],
            major = result["major"],
            local_inputs = result["local_inputs"],
            keyword_inputs = keyword_inputs,
            s3_inputs = list(zip(result["s3_inputs"], result["s3_dataframe_inputs"])),
            http_inputs = result["http_inputs"],
            s3_outputs = list(zip(result["s3_outputs"], result["s3_dataframe_outputs"])),
          )
        )


