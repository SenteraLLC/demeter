from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Sequence, Type, Union, cast

from ... import data, db
from ...data import ObservationType

# HTTP


class HTTPVerb(Enum):
    GET = 1
    PUT = 2
    POST = 3
    DELETE = 4


@dataclass(frozen=True)
class HTTPType(db.TypeTable):
    type_name: str
    verb: HTTPVerb
    uri: str
    uri_parameters: Optional[Sequence[str]]
    request_body_schema: Optional[db.JSON]


# Keyword


class KeywordType(Enum):
    STRING = 1
    INTEGER = 2
    FLOAT = 3
    JSON = 4


@dataclass(frozen=True)
class Keyword:
    keyword_name: str
    keyword_type: KeywordType


# S3


@dataclass(frozen=True)
class S3Type(db.TypeTable):
    type_name: str


@dataclass(frozen=True)
class S3SubType(db.TypeTable):
    pass


@dataclass(frozen=True)
class S3TypeDataFrame(S3SubType, db.SelfKey):
    driver: str
    has_geometry: bool


@dataclass(frozen=True)
class TaggedS3SubType:
    tag: Type[S3SubType]
    value: S3SubType


@dataclass(frozen=True)
class S3Output(db.Table):
    function_id: db.TableId
    s3_type_id: db.TableId


@dataclass(frozen=True)
class S3Object(db.Table):
    key: str
    bucket_name: str
    s3_type_id: db.TableId


@dataclass(frozen=True)
class S3ObjectKey(db.SelfKey):
    s3_object_id: db.TableId
    geospatial_key_id: db.TableId
    temporal_key_id: db.TableId
