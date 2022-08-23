from typing import Optional, Sequence, Tuple, Union

from enum import Enum
from datetime import datetime

from ... import db
from ... import data

from .._inputs import HTTPType, S3Type, S3TypeDataFrame

from dataclasses import dataclass

@dataclass(frozen=True)
class FunctionType(db.TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

@dataclass(frozen=True)
class Function(db.Detailed):
  function_name    : str
  major            : int
  function_type_id : db.TableId

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


from .._inputs.types import Keyword as Keyword

@dataclass(frozen=True)
class KeywordParameter(Keyword, Parameter, db.SelfKey):
  pass

S3TypeSignature = Tuple[S3Type, Optional[S3TypeDataFrame]]

@dataclass(frozen=True)
class FunctionSignature(object):
  name           : str
  major          : int
  local_inputs   : Sequence[data.LocalType]
  keyword_inputs : Sequence[Keyword]
  s3_inputs      : Sequence[S3TypeSignature]
  http_inputs    : Sequence[HTTPType]
  s3_outputs     : Sequence[S3TypeSignature]

