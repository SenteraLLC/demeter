from typing import List, Union, Optional

from ..util.types_protocols import Table

from ..function.types import FunctionId
from ..core.types import Key

from dataclasses import dataclass


@dataclass
class Argument(FunctionId):
  execution_id      : int

@dataclass
class LocalArgument(Argument):
  local_type_id : int
  number_of_observations : int

@dataclass
class HTTPArgument(Argument):
  http_type_id : int
  number_of_observations : int

@dataclass
class S3InputArgument(Argument):
  s3_type_id   : int
  s3_object_id : int

@dataclass
class S3OutputArgument(Argument):
  s3_output_parameter_name : str
  s3_type_id               : int
  s3_object_id             : int

@dataclass
class KeywordArgument(Argument):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]


@dataclass
class Execution(Table):
  function_id  : int

@dataclass
class ExecutionKey():
  execution_id      : int
  geospatial_key_id : int
  temporal_key_id   : int

@dataclass
class ExecutionArguments():
  local   : List[LocalArgument]
  keyword : List[KeywordArgument]
  http    : List[HTTPArgument]
  s3      : List[S3InputArgument]
  keys    : List[Key]

@dataclass
class ExecutionOutputs():
  s3 : List[S3OutputArgument]

@dataclass
class ExecutionSummary():
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id         : int
  execution_id        : int


AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

