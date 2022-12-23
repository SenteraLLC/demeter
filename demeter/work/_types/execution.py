from typing import List, Sequence, TypedDict

from ... import data
from ... import db
from .arguments import (
    HTTPArgument,
    KeywordArgument,
    ObservationArgument,
    S3InputArgument,
    S3OutputArgument,
)
from dataclasses import dataclass


class ExecutionOutputs(TypedDict):
    s3: List[S3OutputArgument]


class ExecutionArguments(TypedDict):
    observation: List[ObservationArgument]
    keyword: List[KeywordArgument]
    http: List[HTTPArgument]
    s3: List[S3InputArgument]
    keys: Sequence[data.Key]


@dataclass(frozen=True)
class ExecutionKey(data.KeyIds, db.SelfKey):
    execution_id: db.TableId


@dataclass
class ExecutionSummary:
    inputs: ExecutionArguments
    outputs: ExecutionOutputs
    function_id: db.TableId
    execution_id: db.TableId
