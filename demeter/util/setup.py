from typing import Any, List, Optional, Sequence, Mapping, Dict, Tuple, Type

import inspect

from ..constants import NOW

from ..database.types_protocols import TableId
from ..function import getMaybeFunctionTypeId
from ..types.function import Function, FunctionType
from ..inputs import getS3TypeIdByName
from ..types.execution import ExecutionSummary, ExecutionKey

from .wrapper_types import WrappableFunction

def createFunction(cursor : Any,
                   name   : str,
                   major  : int,
                  ) -> Function:
  function_type = FunctionType(
                    function_type_name = "transformation",
                    function_subtype_name = None,
                  )
  function_type_id = getMaybeFunctionTypeId(cursor, function_type)
  if function_type_id is None:
    raise Exception(f"Function type does not exist: {function_type}")

  f = Function(
        function_name = name,
        major = major,
        function_type_id = function_type_id,
        created = NOW,
        last_updated = NOW,
        details = None,
      )
  return f


def getKeywordParameterTypes(fn : WrappableFunction) -> Dict[str, Type[Any]]:
  signature = inspect.signature(fn)
  first_argument = list(signature.parameters.keys())[0]
  # Always remove first argumnet
  annotations = fn.__annotations__
  blacklist = ["return", first_argument]
  keyword_types : Dict[str, Type[Any]] = {}
  for parameter_name, _type in annotations.items():
    if parameter_name not in blacklist:
      keyword_types[parameter_name] = _type
  return keyword_types


def getOutputTypes(cursor : Any,
                   output_to_type_name : Mapping[str, str],
                  ) -> Dict[str, Tuple[str, TableId]]:
      output_types : Dict[str, Tuple[str, TableId]] = {}
      for output_name, output_type_name in output_to_type_name.items():
        s3_type_id = getS3TypeIdByName(cursor, output_type_name)
        output_types[output_name] = (output_type_name, s3_type_id)
      return output_types


