from typing import Optional, Sequence, Tuple, Union, Protocol

from enum import Enum
from datetime import datetime

from ..database.types_protocols import TypeTable, Table, Detailed
from ..types.inputs import S3Type, S3TypeDataFrame, HTTPType, Keyword
from ..types.local import LocalType

from dataclasses import dataclass

@dataclass(frozen=True)
class FunctionType(TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

@dataclass(frozen=True)
class Function(Detailed):
  function_name    : str
  major            : int
  function_type_id : int
  created          : datetime


@dataclass(frozen=True)
class FunctionId(Table):
  function_id : int

@dataclass(frozen=True)
class Parameter(FunctionId):
  pass

@dataclass(frozen=True)
class LocalParameter(Parameter):
  local_type_id       : int

@dataclass(frozen=True)
class HTTPParameter(Parameter, Detailed):
  http_type_id        : int

@dataclass(frozen=True)
class S3InputParameter(Parameter, Detailed):
  s3_type_id              : int

@dataclass(frozen=True)
class S3OutputParameter(Parameter, Detailed):
  s3_output_parameter_name : str
  s3_type_id               : int

@dataclass(frozen=True)
class KeywordParameter(Keyword, Parameter):
  pass


S3TypeSignature = Tuple[S3Type, Optional[S3TypeDataFrame]]

@dataclass(frozen=True)
class FunctionSignature(object):
  name           : str
  major          : int
  local_inputs   : Sequence[LocalType]
  keyword_inputs : Sequence[Keyword]
  s3_inputs      : Sequence[S3TypeSignature]
  http_inputs    : Sequence[HTTPType]
  s3_outputs     : Sequence[S3TypeSignature]


AnyKeyTable = Union[LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter]


