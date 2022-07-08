from typing import Sequence, Union, Optional, List, Generator, TypeVar, Any

from .. import db
from .. import task

from dataclasses import dataclass
from collections import OrderedDict


@dataclass(frozen=True, order=True)
class Argument(task.FunctionId):
  execution_id : db.TableId

@dataclass(frozen=True)
class LocalArgument(Argument, db.SelfKey):
  local_type_id : db.TableId
  number_of_observations : int

@dataclass(frozen=True)
class HTTPArgument(Argument, db.SelfKey):
  http_type_id : db.TableId
  number_of_observations : int

@dataclass(frozen=True)
class S3InputArgument(Argument, db.SelfKey):
  s3_type_id   : db.TableId
  s3_object_id : db.TableId

@dataclass(frozen=True)
class S3OutputArgument(Argument, db.SelfKey):
  s3_output_parameter_name : str
  s3_type_id               : db.TableId
  s3_object_id             : db.TableId

@dataclass(frozen=True)
class KeywordArgument(Argument, db.SelfKey):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]

@dataclass(frozen=True)
class Execution(db.Table):
  function_id  : db.TableId


from .. import data

@dataclass(frozen=True)
class ExecutionKey(data.core.types._KeyIds, db.SelfKey):
  execution_id : db.TableId


from typing import TypedDict
from .. import data

class ExecutionOutputs(TypedDict):
  s3 : List[S3OutputArgument]

class ExecutionArguments(TypedDict):
  local   : List[LocalArgument]
  keyword : List[KeywordArgument]
  http    : List[HTTPArgument]
  s3      : List[S3InputArgument]
  keys    : Sequence[data.Key]

@dataclass
class ExecutionSummary:
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id  : db.TableId
  execution_id : db.TableId

