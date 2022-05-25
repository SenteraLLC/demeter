from typing import Sequence, Union, Optional

from ..util.types_protocols import Table

from ..function.types import FunctionId
from ..core.types import Key

from dataclasses import dataclass


@dataclass(frozen=True)
class Argument(FunctionId):
  execution_id      : int

@dataclass(frozen=True)
class LocalArgument(Argument):
  local_type_id : int
  number_of_observations : int

@dataclass(frozen=True)
class HTTPArgument(Argument):
  http_type_id : int
  number_of_observations : int

@dataclass(frozen=True)
class S3InputArgument(Argument):
  s3_type_id   : int
  s3_object_id : int

@dataclass(frozen=True)
class S3OutputArgument(Argument):
  s3_output_parameter_name : str
  s3_type_id               : int
  s3_object_id             : int

@dataclass(frozen=True)
class KeywordArgument(Argument):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]


@dataclass(frozen=True)
class Execution(Table):
  function_id  : int

@dataclass(frozen=True)
class ExecutionKey():
  execution_id      : int
  geospatial_key_id : int
  temporal_key_id   : int

@dataclass(frozen=True)
class ExecutionArguments():
  local   : Sequence[LocalArgument]
  keyword : Sequence[KeywordArgument]
  http    : Sequence[HTTPArgument]
  s3      : Sequence[S3InputArgument]
  keys    : Sequence[Key]

@dataclass(frozen=True)
class ExecutionOutputs():
  s3 : Sequence[S3OutputArgument]

@dataclass(frozen=True)
class ExecutionSummary():
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id         : int
  execution_id        : int


AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

