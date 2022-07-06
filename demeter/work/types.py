from typing import Sequence, Union, Optional, List, Generator, TypeVar, Any

from ..db import Table, SelfKey, TableId

from ..task.function.types import FunctionId

from dataclasses import dataclass
from collections import OrderedDict


@dataclass(frozen=True, order=True)
class Argument(FunctionId):
  execution_id      : TableId

@dataclass(frozen=True)
class LocalArgument(Argument, SelfKey):
  local_type_id : TableId
  number_of_observations : int

@dataclass(frozen=True)
class HTTPArgument(Argument, SelfKey):
  http_type_id : TableId
  number_of_observations : int

@dataclass(frozen=True)
class S3InputArgument(Argument, SelfKey):
  s3_type_id   : TableId
  s3_object_id : TableId

@dataclass(frozen=True)
class S3OutputArgument(Argument, SelfKey):
  s3_output_parameter_name : str
  s3_type_id               : TableId
  s3_object_id             : TableId

@dataclass(frozen=True)
class KeywordArgument(Argument, SelfKey):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]

@dataclass(frozen=True)
class Execution(Table):
  function_id  : TableId


from ..data.core.types import _KeyIds

@dataclass(frozen=True)
class ExecutionKey(_KeyIds, SelfKey):
  execution_id : TableId


from typing import TypedDict
from ..data.core.types import Key as Key

class ExecutionOutputs(TypedDict):
  s3 : List[S3OutputArgument]

class ExecutionArguments(TypedDict):
  local   : List[LocalArgument]
  keyword : List[KeywordArgument]
  http    : List[HTTPArgument]
  s3      : List[S3InputArgument]
  keys    : Sequence[Key]

@dataclass
class ExecutionSummary:
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id         : TableId
  execution_id        : TableId

