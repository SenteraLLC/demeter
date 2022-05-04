from typing import TypedDict, List, Union, Optional

from ..util.types_protocols import Table

from ..function.types import FunctionId
from ..core.types import Key


class Argument(FunctionId):
  execution_id      : int

class LocalArgument(Argument):
  local_type_id : int
  number_of_observations : int

class HTTPArgument(Argument):
  http_type_id : int
  number_of_observations : int

class S3InputArgument(Argument):
  s3_type_id   : int
  s3_object_id : int

class S3OutputArgument(Argument):
  s3_output_parameter_name : str
  s3_type_id               : int
  s3_object_id             : int

class KeywordArgument(Argument):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]


class Execution(Table):
  function_id  : int

class ExecutionKey(TypedDict):
  execution_id      : int
  geospatial_key_id : int
  temporal_key_id   : int

class ExecutionArguments(TypedDict):
  local   : List[LocalArgument]
  keyword : List[KeywordArgument]
  http    : List[HTTPArgument]
  s3      : List[S3InputArgument]
  keys    : List[Key]

class ExecutionOutputs(TypedDict):
  s3 : List[S3OutputArgument]

class ExecutionSummary(TypedDict):
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id         : int
  execution_id        : int


AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

