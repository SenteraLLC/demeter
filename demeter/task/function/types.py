from typing import Optional, Sequence, Tuple, Union

from enum import Enum
from datetime import datetime

from ...db.base_types import TypeTable, Table, Detailed, SelfKey
from ...db.base_types import TableId as TableId

from ..inputs.types import S3Type, S3TypeDataFrame, HTTPType, Keyword
from ...data.local.types import LocalType

from dataclasses import dataclass

@dataclass(frozen=True)
class FunctionType(TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

#reveal_type(Detailed)
@dataclass(frozen=True)
class Function(Detailed):
  function_name    : str
  major            : int
  function_type_id : TableId
  created          : datetime
#reveal_type(Function)

@dataclass(frozen=True)
class FunctionId(Table):
  function_id : TableId

@dataclass(frozen=True)
class Parameter(FunctionId):
  pass

@dataclass(frozen=True)
class LocalParameter(Parameter, SelfKey):
  local_type_id       : TableId

@dataclass(frozen=True)
class HTTPParameter(Parameter, SelfKey, Detailed):
  http_type_id        : TableId

@dataclass(frozen=True)
class S3InputParameter(Parameter, SelfKey, Detailed):
  s3_type_id              : TableId

@dataclass(frozen=True)
class S3OutputParameter(Parameter, SelfKey, Detailed):
  s3_output_parameter_name : str
  s3_type_id               : TableId

@dataclass(frozen=True)
class KeywordParameter(Keyword, Parameter, SelfKey):
  pass

AnyParameter = Union[LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter]

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

AnyDataTable = Union[Function, AnyParameter]
AnyTypeTable = Union[FunctionType]
AnyKeyTable = Union[AnyParameter]
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]

