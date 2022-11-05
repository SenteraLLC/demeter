from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Generator, List, Optional, Sequence, TypeVar, Union

from ... import db, task


@dataclass(frozen=True, order=True)
class Argument(task.FunctionId):
    execution_id: db.TableId


@dataclass(frozen=True)
class ObservationArgument(Argument, db.SelfKey):
    observation_type_id: db.TableId
    number_of_observations: int


@dataclass(frozen=True)
class HTTPArgument(Argument, db.SelfKey):
    http_type_id: db.TableId
    number_of_observations: int


@dataclass(frozen=True)
class S3InputArgument(Argument, db.SelfKey):
    s3_type_id: db.TableId
    s3_object_id: db.TableId


@dataclass(frozen=True)
class S3OutputArgument(Argument, db.SelfKey):
    s3_output_parameter_name: str
    s3_type_id: db.TableId
    s3_object_id: db.TableId


@dataclass(frozen=True)
class KeywordArgument(Argument, db.SelfKey):
    keyword_name: str
    value_number: Optional[float]
    value_string: Optional[str]


@dataclass(frozen=True)
class Execution(db.Table):
    function_id: db.TableId
