from typing import Union, Any, Optional, Type, Sequence, Mapping
from typing import cast

from enum import Enum

from ..types.local import LocalType as LocalType

from ..database.types_protocols import TypeTable, Table, SelfKey
from ..database.types_protocols import TableId as TableId
from ..database.details import JSON

from dataclasses import dataclass


# HTTP

class HTTPVerb(Enum):
  GET    = 1
  PUT    = 2
  POST   = 3
  DELETE = 4

@dataclass(frozen=True)
class HTTPType(TypeTable):
  type_name           : str
  verb                : HTTPVerb
  uri                 : str
  uri_parameters      : Optional[Sequence[str]]
  request_body_schema : Optional[JSON]


# Keyword

class KeywordType(Enum):
  STRING  = 1
  INTEGER = 2
  FLOAT   = 3
  JSON    = 4

@dataclass(frozen=True)
class Keyword:
  keyword_name : str
  keyword_type : KeywordType


# S3

@dataclass(frozen=True)
class S3Type(TypeTable):
  type_name : str


@dataclass(frozen=True)
class S3SubType(TypeTable):
  pass

@dataclass(frozen=True)
class S3TypeDataFrame(S3SubType, SelfKey):
  driver       : str
  has_geometry : bool

@dataclass(frozen=True)
class TaggedS3SubType:
  tag   : Type[S3SubType]
  value : S3SubType

@dataclass(frozen=True)
class S3Output(Table):
  function_id : TableId
  s3_type_id  : TableId

@dataclass(frozen=True)
class S3Object(Table):
  key         : str
  bucket_name : str
  s3_type_id  : TableId

@dataclass(frozen=True)
class S3ObjectKey(SelfKey):
  s3_object_id      : TableId
  geospatial_key_id : TableId
  temporal_key_id   : TableId


# Aggregate Type
#   See '.api_protocol.py'
AnyTypeTable = Union[HTTPType, S3Type, LocalType, S3TypeDataFrame]


