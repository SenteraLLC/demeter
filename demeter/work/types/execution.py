from typing import List, TypedDict, Sequence

from .arguments import *

class ExecutionOutputs(TypedDict):
  s3 : List[S3OutputArgument]

from ... import data

class ExecutionArguments(TypedDict):
  local   : List[LocalArgument]
  keyword : List[KeywordArgument]
  http    : List[HTTPArgument]
  s3      : List[S3InputArgument]
  keys    : Sequence[data.Key]


from ... import db

from dataclasses import dataclass

@dataclass(frozen=True)
class ExecutionKey(data.core.types._KeyIds, db.SelfKey):
  execution_id : db.TableId

@dataclass
class ExecutionSummary:
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id  : db.TableId
  execution_id : db.TableId


