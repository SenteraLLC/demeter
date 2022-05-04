from typing import Union, Any, TypedDict, Optional, Type, List

from enum import Enum

import jsonschema

from ..local.types import LocalType
from ..util.types_protocols import TypeTable, Table


class HTTPVerb(Enum):
  GET    = 1
  PUT    = 2
  POST   = 3
  DELETE = 4

class RequestBodySchema(object):
  def __init__(self : Any, schema : Any):
    jsonschema.Draft7Validator.check_schema(schema)
    self.schema = schema

class HTTPType(TypeTable):
  type_name           : str
  verb                : HTTPVerb
  uri                 : str
  uri_parameters      : Optional[List[str]]
  request_body_schema : Optional[RequestBodySchema]

class S3Type(TypeTable):
  type_name : str

class S3TypeDataFrame(Table):
  driver       : str
  has_geometry : bool

S3SubType = Union[S3TypeDataFrame]

class TaggedS3SubType(TypedDict):
  tag   : Type[S3SubType]
  value : S3SubType

s3_subtype_table_lookup = {
  S3TypeDataFrame : "s3_type_dataframe",
}

class KeywordType(Enum):
  STRING  = 1
  INTEGER = 2
  FLOAT   = 3
  JSON    = 4

class Keyword(TypedDict):
  keyword_name : str
  keyword_type : KeywordType

class S3Output(Table):
  function_id : int
  s3_type_id  : int

class S3Object(Table):
  key         : str
  bucket_name : str
  s3_type_id  : int

class S3ObjectKey(Table):
  s3_object_id      : int
  geospatial_key_id : int
  temporal_key_id   : int

AnyTypeTable = Union[HTTPType, S3Type, LocalType, S3TypeDataFrame]
