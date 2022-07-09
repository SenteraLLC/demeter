from typing import Sequence, Union, Optional, List, Generator, TypeVar, Any

from ... import db
from ... import task

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




