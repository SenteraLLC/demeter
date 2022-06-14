from typing import Sequence, Union, Optional, List, Generator, TypeVar

from ..database.types_protocols import Table, SelfKey
from ..database.types_protocols import TableId as TableId


from .function import FunctionId
from .core import GeoSpatialKey, TemporalKey

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
class _KeyIds(Table):
  geospatial_key_id : TableId
  temporal_key_id   : TableId

@dataclass(frozen=True, order=True)
class Key(_KeyIds, GeoSpatialKey, TemporalKey):
  pass

KeyGenerator = Generator[Key, None, None]


@dataclass(frozen=True)
class Execution(Table):
  function_id  : TableId

@dataclass(frozen=True)
class ExecutionKey(_KeyIds, SelfKey):
  execution_id : TableId

from typing import TypedDict

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

AnyKeyTable = Union[LocalArgument, HTTPArgument, S3InputArgument, S3OutputArgument, KeywordArgument, ExecutionKey]

