import itertools

from io import BytesIO

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any, Dict, BinaryIO, Iterator

import psycopg2
import requests
import geopandas as gpd # type: ignore

from datetime import date

from . import schema_api
from .connections import *
from .types import GeoSpatialKey, TemporalKey, KeyGenerator, LocalType, LocalValue, UnitType, HTTPVerb, S3Object, RequestBodySchema, HTTPType
from . import http
from . import local
from . import ingest
from .generators import generateInsertMany

# TODO: Stubs?
from shapely import wkb # type: ignore


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




  # TODO: Assuming JSON response for now
  def _http(self,
            http_type    : HTTPType,
            param_fn     : Optional[http.KeyToArgsFunction] = None,
            json_fn      : Optional[http.KeyToArgsFunction] = None,
            http_options : Dict[str, Any] = {}
           ) -> List[Dict[str, Any]]:
    verb = http_type["verb"]
    func = {HTTPVerb.GET    : requests.get,
            HTTPVerb.POST   : requests.post,
            HTTPVerb.PUT    : requests.put,
            HTTPVerb.DELETE : requests.delete,
           }[verb]
    uri = http_type["uri"]

    responses : List[Any] = []
    for k in self.keys:
      expected_params = http_type["uri_parameters"]
      if expected_params is not None:
        http_options["params"] = http.parseHTTPParams(expected_params, param_fn, k)

      request_body_schema = http_type["request_body_schema"]
      if request_body_schema is not None:
        http_options["json"] = http.parseRequestSchema(request_body_schema, json_fn, k)

      wrapped = http.wrap_requests_fn(func, self.cursor)
      response = wrapped(uri, **http_options)
      responses.append(response.json())
    return responses

  def http(self,
           type_name : str,
           param_fn     : Optional[http.KeyToArgsFunction] = None,
           json_fn      : Optional[http.KeyToArgsFunction] = None,
           http_options : Dict[str, Any] = {}
          ) -> List[Dict[str, Any]]:
    http_type_id, http_type = schema_api.getHTTPByName(self.cursor, type_name)
    return self._http(http_type, param_fn, json_fn, http_options)

  def s3(self,
         type_name   : str,
        ) -> gpd.GeoDataFrame:
    s3_object_id, s3_object = schema_api.getS3ObjectByKeys(self.cursor, self.keys, type_name)
    s3_type = schema_api.getS3Type(self.cursor, s3_object["s3_type_id"])
    if s3_object is None:
      raise Exception(f"Failed to find S3 object '{type_name}' associated with keys")
    s3_key = s3_object["key"]
    bucket_name = s3_object["bucket_name"]
    driver = s3_type["driver"]
    f = ingest.download(self.s3_connection, bucket_name, s3_key)
    df = gpd.read_file(f, driver = driver)
    return df


  def upload(self,
             s3_type_id  : int,
             bucket_name : str,
             s3_key      : str,
             blob        : BytesIO,
            ) -> bool:
    ingest.upload(self.s3_connection, bucket_name, s3_key, blob)
    s3_object = S3Object(
                  s3_type_id = s3_type_id,
                  key = s3_key,
                  bucket_name = bucket_name,
                )
    s3_object_id = schema_api.insertS3Object(self.cursor, s3_object)
    schema_api.insertS3ObjectKeys(self.cursor, s3_object_id, self.keys)
    return True


  def upload_file(self,
                  s3_type_id   : int,
                  bucket_name  : str,
                  s3_file_meta : ingest.S3FileMeta,
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

  def getMatrix(self) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame()



