from typing import Union, Any, Optional, Type, Sequence

from enum import Enum

import jsonschema

from ..types.local import LocalType
from ..database.types_protocols import TypeTable, Table, SelfKey

from dataclasses import dataclass

class HTTPVerb(Enum):
  GET    = 1
  PUT    = 2
  POST   = 3
  DELETE = 4

class RequestBodySchema(object):
  def __init__(self : Any, schema : Any):
    jsonschema.Draft7Validator.check_schema(schema)
    self.schema = schema

@dataclass(frozen=True)
class HTTPType(TypeTable):
  type_name           : str
  verb                : HTTPVerb
  uri                 : str
  uri_parameters      : Optional[Sequence[str]]
  request_body_schema : Optional[RequestBodySchema]


class KeywordType(Enum):
  STRING  = 1
  INTEGER = 2
  FLOAT   = 3
  JSON    = 4

@dataclass(frozen=True)
class Keyword(object):
  keyword_name : str
  keyword_type : KeywordType


@dataclass(frozen=True)
class S3Type(TypeTable):
  type_name : str

@dataclass(frozen=True)
class S3TypeDataFrame(TypeTable, SelfKey):
  driver       : str
  has_geometry : bool

S3SubType = Union[S3TypeDataFrame]

@dataclass(frozen=True)
class TaggedS3SubType(object):
  tag   : Type[S3SubType]
  value : S3SubType

s3_subtype_table_lookup = {
  S3TypeDataFrame : "s3_type_dataframe",
}


@dataclass(frozen=True)
class S3Output(Table):
  function_id : int
  s3_type_id  : int

@dataclass(frozen=True)
class S3Object(Table):
  key         : str
  bucket_name : str
  s3_type_id  : int

@dataclass(frozen=True)
class S3ObjectKey(SelfKey):
  s3_object_id      : int
  geospatial_key_id : int
  temporal_key_id   : int


AnyTypeTable = Union[HTTPType, S3Type, LocalType, S3TypeDataFrame]
