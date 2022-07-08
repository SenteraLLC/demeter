from typing import Optional, Sequence, Tuple, Union

from enum import Enum
from datetime import datetime

from ... import db
from ... import data

from .. import inputs

from dataclasses import dataclass

@dataclass(frozen=True)
class FunctionType(db.TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

#reveal_type(Detailed)
@dataclass(frozen=True)
class Function(db.Detailed):
  function_name    : str
  major            : int
  function_type_id : db.TableId
  created          : datetime
#reveal_type(Function)

@dataclass(frozen=True)
class FunctionId(db.Table):
  function_id : db.TableId

@dataclass(frozen=True)
class Parameter(FunctionId):
  pass

@dataclass(frozen=True)
class LocalParameter(Parameter, db.SelfKey):
  local_type_id : db.TableId

@dataclass(frozen=True)
class HTTPParameter(Parameter, db.SelfKey, db.Detailed):
  http_type_id : db.TableId

@dataclass(frozen=True)
class S3InputParameter(Parameter, db.SelfKey, db.Detailed):
  s3_type_id              : db.TableId

@dataclass(frozen=True)
class S3OutputParameter(Parameter, db.SelfKey, db.Detailed):
  s3_output_parameter_name : str
  s3_type_id               : db.TableId


from ..inputs.types import Keyword as Keyword

@dataclass(frozen=True)
class KeywordParameter(Keyword, Parameter, db.SelfKey):
  pass

AnyParameter = Union[LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter, KeywordParameter]

S3TypeSignature = Tuple[inputs.S3Type, Optional[inputs.S3TypeDataFrame]]

@dataclass(frozen=True)
class FunctionSignature(object):
  name           : str
  major          : int
  local_inputs   : Sequence[data.LocalType]
  keyword_inputs : Sequence[Keyword]
  s3_inputs      : Sequence[S3TypeSignature]
  http_inputs    : Sequence[inputs.HTTPType]
  s3_outputs     : Sequence[S3TypeSignature]

AnyDataTable = Union[Function, AnyParameter]
AnyTypeTable = Union[FunctionType]
AnyKeyTable = Union[AnyParameter]
AnyTable = Union[AnyDataTable, AnyTypeTable, AnyKeyTable]

