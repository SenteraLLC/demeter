import itertools
import jsonschema

from io import BytesIO

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any, Dict, Set, BinaryIO

import psycopg2
import requests

from datetime import date

from . import schema_api
from .connections import *
from .types import GeoSpatialKey, TemporalKey, LocalType, LocalValue, UnitType, HTTPVerb
from . import http
from . import local

# TODO: Stubs?
from shapely import wkb  # type: ignore


class DataSource(object):
  def __init__(self,
               geospatial_keys : List[GeoSpatialKey],
               temporal_keys   : List[TemporalKey],
               pg_connection   : PGConnection,
               s3_connection   : Any,
              ):
    self.geospatial_keys = geospatial_keys
    self.temporal_keys = temporal_keys
    self.pg_connection = pg_connection
    self.s3_connection = s3_connection

    self.cursor = self.pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)



  # TODO: Can we always assume cartesian products?
  def keys(self) -> Generator[Tuple[GeoSpatialKey, TemporalKey], None, None]:
    geospatial = (g for g in self.geospatial_keys)
    temporal = (t for t in self.temporal_keys)

    for g, t in itertools.product(geospatial, temporal):

      yield g, t

  def key(self) -> Any:
    return {}


  def local(self, local_type : LocalType) -> List[Tuple[LocalValue, UnitType]]:
    results : List[Tuple[LocalValue, UnitType]] = []
    for g, t in self.keys():

      partial_results = local._load(self.cursor, g, t, local_type)
      results.extend(partial_results)
    return results


  def __checkHTTPParams(self,
                        params : Dict[str, Any],
                        expected_params :  Set[str]
                       ) -> None:
    try:
      missing = set(expected_params) - set(params.keys())
      # TODO: Allow optional params?
      if len(missing):
        raise Exception(f"Missing args: {missing}")
      extraneous = set(params.keys()) - set(expected_params)
      if len(extraneous):
        raise Exception(f"Extraneous args: {extraneous}")
    except KeyError:
      pass # no params
    return

  def __checkHTTPRequestBody(self,
                             request_body : Any,
                             request_schema : Any,
                            ) -> None:
    validator = jsonschema.Draft7Validator(request_schema)
    is_valid = validator.is_valid(request_body)

  def http(self, http_type_name, *args, **kwargs):
    http_type_id, http_type = schema_api.getHTTPByName(self.cursor, http_type_name)

    expected_params = http_type["uri_parameters"]
    if expected_params is not None:
      params = kwargs["params"]
      self.__checkHTTPParams(params, expected_params)

    request_schema = http_type["request_body_schema"]
    if request_schema is not None:
      request_body = kwargs["json"]
      self.__checkHTTPRequestBody(request_body, request_schema)

    verb = http_type["verb"]
    func = {HTTPVerb.GET    : requests.get,
            HTTPVerb.POST   : requests.post,
            HTTPVerb.PUT    : requests.put,
            HTTPVerb.DELETE : requests.delete,
           }[verb]
    uri = http_type["uri"]
    wrapped = http.wrap_requests_fn(func, self.cursor)
    response = wrapped(uri, *args, **kwargs)
    return response


  # TODO: S3 Namespace?
  def download(self, bucket_name : str, key : str) -> BytesIO:
    dst = BytesIO()
    self.s3_connection.Bucket(bucket_name).download_fileobj(Key=key, Fileobj=dst)
    dst.seek(0)
    return dst


  #def upload(self, bucket_name : str, key : str, filename : BytesIO) -> bool:
  #  self.s3_connection.Bucket(bucket_name).upload_fileobj(Key=key, Fileobj = filename)
  #  return True


  #def upload_file(self, bucket_name : str, key : str, file : BinaryIO) -> bool:
  #  as_bytes = BytesIO(file.read())
  #  return self.upload(bucket_name, key, as_bytes)


class OutputObject(object):
  pass

T = TypeVar('T')

class S3(Generic[T]):
  def __init__(self, v : T):
    self.value = v
