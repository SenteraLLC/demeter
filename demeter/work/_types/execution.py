from typing import List, Sequence, TypedDict

from .arguments import *


class ExecutionOutputs(TypedDict):
    s3: List[S3OutputArgument]


from ... import data


class ExecutionArguments(TypedDict):
    observation: List[ObservationArgument]
    keyword: List[KeywordArgument]
    http: List[HTTPArgument]
    s3: List[S3InputArgument]
    keys: Sequence[data.Key]


from dataclasses import dataclass

from ... import db



@dataclass(frozen=True)
class ExecutionKey(data.KeyIds, db.SelfKey):
    execution_id: db.TableId


@dataclass
class ExecutionSummary:
    inputs: ExecutionArguments
    outputs: ExecutionOutputs
    function_id: db.TableId
    execution_id: db.TableId
