from typing import Optional, List, TypedDict, Tuple, Union

from enum import Enum
from datetime import datetime

from ..util.types_protocols import TypeTable, Table, Detailed
from ..inputs.types import S3Type, S3TypeDataFrame, HTTPType, Keyword
from ..local.types import LocalType


class FunctionType(TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

class Function(Detailed):
  function_name    : str
  major            : int
  function_type_id : int
  created          : datetime

class FunctionId(Table):
  function_id : int


class Parameter(FunctionId):
  pass

class LocalParameter(Parameter):
  local_type_id       : int

class HTTPParameter(Parameter, Detailed):
  http_type_id        : int

class S3InputParameter(Parameter, Detailed):
  s3_type_id              : int

class S3OutputParameter(Parameter, Detailed):
  s3_output_parameter_name : str
  s3_type_id               : int

class KeywordParameter(Keyword, Parameter):
  pass


S3TypeSignature = Tuple[S3Type, Optional[S3TypeDataFrame]]

class FunctionSignature(TypedDict):
  name           : str
  major          : int
  local_inputs   : List[LocalType]
  keyword_inputs : List[Keyword]
  s3_inputs      : List[S3TypeSignature]
  http_inputs    : List[HTTPType]
  s3_outputs     : List[S3TypeSignature]


AnyKeyTable = Union[LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter]


