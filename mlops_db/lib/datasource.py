import itertools

from io import BytesIO

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any, Dict, BinaryIO, Iterator, Union
from typing import cast

import psycopg2
import requests
import pandas as pd
import geopandas as gpd # type: ignore

from datetime import date

from . import schema_api
from .connections import *
from . import types
from . import http
from . import local
from . import ingest
from .generators import generateInsertMany


OneToOneResponseFunction : http.ResponseFunction = lambda r : [cast(Dict[str, Any], r.json())]
OneToManyResponseFunction : http.ResponseFunction = lambda rs : [cast(Dict[str, Any], r) for r in rs.json()]

# TODO: Memoize or throw error?

class DataSource(object):
  def __init__(self,
               keys            : types.KeyGenerator,
               cursor          : Any,
               s3_connection   : Any,
              ):
    self.s3_connection = s3_connection
    self.cursor = cursor
    self.keys = list(keys)


  def local_raw(self, local_type : types.LocalType) -> List[Tuple[types.LocalValue, types.UnitType]]:
    return local.load(self.cursor, self.keys, local_type)


  def local(self, local_type : types.LocalType) -> pd.DataFrame:
    raw = self.local_raw(local_type)
    rows = []
    for local_value, unit_type in raw:
      rows.append(dict(**local_value, **unit_type))
    return pd.DataFrame(rows)


  # TODO: Assuming JSON response for now
  def _http(self,
            http_type    : types.HTTPType,
            param_fn     : Optional[http.KeyToArgsFunction],
            json_fn      : Optional[http.KeyToArgsFunction],
            response_fn  : http.ResponseFunction,
            http_options : Dict[str, Any] = {},
           ) -> List[Tuple[types.Key, Dict[str, Any]]]:
    verb = http_type["verb"]
    func = {types.HTTPVerb.GET    : requests.get,
            types.HTTPVerb.POST   : requests.post,
            types.HTTPVerb.PUT    : requests.put,
            types.HTTPVerb.DELETE : requests.delete,
           }[verb]
    uri = http_type["uri"]

    responses : List[Tuple[types.Key, Dict[str, Any]]] = []
    for k in self.keys:
      expected_params = http_type["uri_parameters"]
      if expected_params is not None:
        http_options["params"] = http.parseHTTPParams(expected_params, param_fn, k)

      request_body_schema = http_type["request_body_schema"]
      if request_body_schema is not None:
        http_options["json"] = http.parseRequestSchema(request_body_schema, json_fn, k)

      wrapped = http.wrap_requests_fn(func, self.cursor)
      raw_response = wrapped(uri, **http_options)
      response_rows = response_fn(raw_response)

      for row in response_rows:
        responses.append((k, row))

    return responses


  def http_raw(self,
               type_name : str,
               param_fn     : Optional[http.KeyToArgsFunction] = None,
               json_fn      : Optional[http.KeyToArgsFunction] = None,
               response_fn  : http.ResponseFunction = OneToOneResponseFunction,
               http_options : Dict[str, Any] = {}
              ) -> List[Tuple[types.Key, Dict[str, Any]]]:
    http_type_id, http_type = schema_api.getHTTPByName(self.cursor, type_name)
    http_result = self._http(http_type, param_fn, json_fn, response_fn, http_options)
    return http_result


  def http(self,
           type_name : str,
           param_fn     : Optional[http.KeyToArgsFunction] = None,
           json_fn      : Optional[http.KeyToArgsFunction] = None,
           response_fn  : http.ResponseFunction = OneToOneResponseFunction,
           http_options : Dict[str, Any] = {}
          ) -> pd.DataFrame:
    raw_results = self.http_raw(type_name, param_fn, json_fn, response_fn, http_options)
    results = []
    for key, row in raw_results:
      results.append(dict(**key, **row))
    return pd.DataFrame(results)


  def s3_raw(self,
             type_name   : str,
            ) -> Tuple[BytesIO, Optional[types.TaggedS3SubType]]:
    s3_object_id, s3_object = schema_api.getS3ObjectByKeys(self.cursor, self.keys, type_name)
    s3_type, maybe_tagged_s3_subtype = schema_api.getS3Type(self.cursor, s3_object["s3_type_id"])
    if s3_object is None:
      raise Exception(f"Failed to find S3 object '{type_name}' associated with keys")
    s3_key = s3_object["key"]
    bucket_name = s3_object["bucket_name"]
    f = ingest.download(self.s3_connection, bucket_name, s3_key)
    return f, maybe_tagged_s3_subtype


  def s3(self,
         type_name   : str,
        ) -> ingest.SupportedS3DataType:
    raw_results, maybe_tagged_s3_subtype = self.s3_raw(type_name)
    if maybe_tagged_s3_subtype is not None:
      tagged_s3_subtype = maybe_tagged_s3_subtype
      tag = tagged_s3_subtype["tag"]
      subtype = tagged_s3_subtype["value"]
      if tag == types.S3TypeDataFrame:
        dataframe_subtype = subtype
        driver = dataframe_subtype["driver"]
        has_geometry = dataframe_subtype["has_geometry"]
        if has_geometry:
          return gpd.read_file(raw_results, driver=driver)
        else:
          pandas_file_type = ingest.toPandasFileType(driver)
          pandas_driver_fn = ingest.FILETYPE_TO_PANDAS_READ_FN[pandas_file_type]
          return pandas_driver_fn(raw_results)


    return gpd.read_file(raw_results, driver=driver)


  def upload(self,
             s3_type_id  : int,
             bucket_name : str,
             s3_key      : str,
             blob        : BytesIO,
            ) -> bool:
    ingest.upload(self.s3_connection, bucket_name, s3_key, blob)
    s3_object = types.S3Object(
                  s3_type_id = s3_type_id,
                  key = s3_key,
                  bucket_name = bucket_name,
                )
    s3_object_id = schema_api.insertS3Object(self.cursor, s3_object)
    print("KEYS: ",self.keys)
    schema_api.insertS3ObjectKeys(self.cursor, s3_object_id, self.keys, s3_type_id)
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
             select G.geom_id, G.geom, G.container_geom_id, CONTAINER.geom as container_geom
             from geom G
             join geom CONTAINER
               on G.geom_id = CONTAINER.geom_id
             where G.geom_id = any(%(geo_ids)s)
           """
    result = gpd.read_postgis(stmt, self.cursor.connection, "geom", params = {"geo_ids": geo_ids})
    return result

  def getMatrix(self) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame()



