import itertools
import jsonschema

from io import BytesIO

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any, Dict, Set, BinaryIO, Iterator

import psycopg2
import requests
import geopandas as gpd

from datetime import date

from . import schema_api
from .connections import *
from .types import GeoSpatialKey, TemporalKey, Key, KeyGenerator, LocalType, LocalValue, UnitType, HTTPVerb, Key, S3Object
from . import http
from . import local
from . import ingest
from .generators import generateInsertMany

# TODO: Stubs?
from shapely import wkb  # type: ignore


class DataSource(object):
  def __init__(self,
               keys            : KeyGenerator,
               cursor          : Any,
               s3_connection   : Any,
              ):
    self.s3_connection = s3_connection
    self.cursor = cursor
    self.keys = list(keys)


  def local(self, local_type : LocalType) -> List[Tuple[LocalValue, UnitType]]:
    results : List[Tuple[LocalValue, UnitType]] = []
    for k in self.keys:

      partial_results = local._load(self.cursor, k, local_type)
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


  def download(self,
               type_name   : str,
               key         : Key,
              ) -> BytesIO:
    s3_object = schema_api.getS3ObjectByKey(self.cursor, key)
    s3_key = s3_object["key"]
    bucket_name = s3_object["bucket_name"]
    return ingest.download(self.s3_connection, bucket_name, s3_key)


  def upload(self,
             s3_type_id  : int,
             bucket_name : str,
             key         : str,
             blob        : BytesIO,
            ) -> bool:
    ingest.upload(self.s3_connection, bucket_name, key, blob)
    s3_object = S3Object(
                  s3_type_id = s3_type_id,
                  key = key,
                  bucket_name = bucket_name,
                )
    s3_object_id = schema_api.insertS3Object(self.cursor, s3_object)
    schema_api.insertS3ObjectKeys(self.cursor, s3_object_id, self.keys)
    return True


  def upload_file(self,
                  s3_type_id   : int,
                  bucket_name  : str,
                  s3_file_meta : ingest.S3FileMeta,
                  keys         : List[Key],
                 ) -> bool:
    s3_filename_on_disk = s3_file_meta["filename_on_disk"]
    f = open(s3_filename_on_disk, "rb")
    as_bytes = BytesIO(f.read())
    s3_key = s3_file_meta["key"]
    return self.upload(s3_type_id, bucket_name, s3_key, as_bytes)

  def get_geometry(self) -> gpd.GeoDataFrame:
    geo_ids = [k["geom_id"] for k in self.keys]
    stmt = """
             select geom from geom where geom_id = any(%(geo_ids)s)
           """
    result = gpd.read_postgis(stmt, self.cursor.connection, "geom", params = {"geo_ids": geo_ids})
    return result

