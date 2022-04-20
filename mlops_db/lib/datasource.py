import itertools

from io import BytesIO
from collections import ChainMap

from typing import List, Optional, TypedDict, Tuple, Generator, TypeVar, Generic, Any, Dict, BinaryIO, Iterator, Union, Callable, Set
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

AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]

class DataSourceTypes(TypedDict):
  s3_type_ids  : Set[int]
  local_type_ids     : Set[int]
  http_type_ids      : Set[int]


class DataSourceStub(object):
  def __init__(self,
               cursor : Any,
              ):
    self.LOCAL = "__LOCAL"
    self.GEOM = "__PRIMARY_GEOMETRY"

    self.cursor = cursor
    self.types = DataSourceTypes(
                   s3_type_ids  = set(),
                   local_type_ids     = set(),
                   http_type_ids      = set(),
                 )


  def s3(self,
         type_name   : str,
        ) -> ingest.SupportedS3DataType:
    s3_type_id = schema_api.getS3TypeIdByName(self.cursor, type_name)
    self.types["s3_type_ids"].add(s3_type_id)
    return pd.DataFrame()


  def local(self, local_types : List[types.LocalType]) -> pd.DataFrame:
    for t in local_types:
      maybe_local_type_id = schema_api.getMaybeLocalTypeId(self.cursor, t)
      if maybe_local_type_id is None:
        raise Exception(f"Local Type does not exist: {t}")
      else:
        local_type_id = maybe_local_type_id
        self.types["local_type_ids"].add(local_type_id)
    return pd.DataFrame()


  def http(self,
           type_name    : str,
           param_fn     : Optional[http.KeyToArgsFunction] = None,
           json_fn      : Optional[http.KeyToArgsFunction] = None,
           response_fn  : http.ResponseFunction = OneToOneResponseFunction,
           http_options : Dict[str, Any] = {}
          ) -> pd.DataFrame:
    http_type_id, http_type = schema_api.getHTTPByName(self.cursor, type_name)
    self.types["http_type_ids"].add(http_type_id)
    return pd.DataFrame()

  def getMatrix(self) -> gpd.GeoDataFrame:
    return pd.DataFrame()

  def join(self,
           left_type_name : str,
           right_type_name : str,
           join_fn : Optional[Callable[..., Any]] = None,
           **kwargs : Any,
          ) -> None:
    return None




# TODO: Memoize or throw error?

class DataSource(DataSourceStub):
  def __init__(self,
               keys            : List[types.Key],
               cursor          : Any,
               s3_connection   : Any,
              ):
    self.s3_connection = s3_connection
    self.cursor = cursor
    self.keys = keys

    self.dataframes : Dict[str, pd.DataFrame] = {}
    self.geodataframes : Dict[str, gpd.GeoDataFrame] = {}
    self.joins : Dict[Tuple[str, str], Tuple[Optional[Callable[..., AnyDataFrame]], Dict[str, Any]]] = {}


  def _local_raw(self, local_types : List[types.LocalType]) -> List[Tuple[types.LocalValue, types.UnitType]]:
    return local.load(self.cursor, self.keys, local_types)


  def _typesToDict(*tables   : types.AnyIdTable,
                  ) -> Dict[str, Any]:
    blacklist : Set[str] = {types.id_table_lookup[t] + "_id" for t in tables} # type: ignore
    return {k : v for k , v in ChainMap(*tables).items() if k not in blacklist} # type: ignore


  def local(self, local_types : List[types.LocalType]) -> pd.DataFrame:
    if self.LOCAL in self.dataframes:
      # TODO: Local data aquisitions should be deferred in the future
      raise Exception("Local data can only be acquired once.")
    raw = self._local_raw(local_types)
    rows = []
    for local_value, unit_type in raw:
      rows.append(dict(**local_value, **unit_type))
    df = pd.DataFrame(rows)
    self.dataframes[self.LOCAL] = df
    return df


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


  # TODO: Support HTTP GeoDataFrames
  def http(self,
           type_name    : str,
           param_fn     : Optional[http.KeyToArgsFunction] = None,
           json_fn      : Optional[http.KeyToArgsFunction] = None,
           response_fn  : http.ResponseFunction = OneToOneResponseFunction,
           http_options : Dict[str, Any] = {}
          ) -> pd.DataFrame:
    raw_results = self.http_raw(type_name, param_fn, json_fn, response_fn, http_options)
    results = []
    for key, row in raw_results:
      results.append(dict(**key, **row))
    df = pd.DataFrame(results)
    self.dataframes[type_name] = df
    return df


  def s3_raw(self,
             type_name   : str,
            ) -> Tuple[BytesIO, Optional[types.TaggedS3SubType]]:
    maybe_s3_object = schema_api.getS3ObjectByKeys(self.cursor, self.keys, type_name)
    if maybe_s3_object is None:
      raise Exception(f"Failed to find S3 object '{type_name}' associated with keys")
    s3_object = maybe_s3_object
    s3_type, maybe_tagged_s3_subtype = schema_api.getS3Type(self.cursor, s3_object["s3_type_id"])
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
          gdf = gpd.read_file(raw_results, driver=driver)
          self.geodataframes[type_name] = gdf
          return gdf
        else:
          pandas_file_type = ingest.toPandasFileType(driver)
          pandas_driver_fn = ingest.FILETYPE_TO_PANDAS_READ_FN[pandas_file_type]
          df = pandas_driver_fn(raw_results)
          self.dataframes[type_name] = df
          return df

    # TODO: This should be reading binary data, not geodataframes
    gdf = gpd.read_file(raw_results, driver=driver)
    self.geodataframes[type_name] = gdf
    return gdf


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
             select G.geom_id, G.geom, G.container_geom_id
             from geom G
             join geom CONTAINER
               on G.geom_id = CONTAINER.geom_id
             where G.geom_id = any(%(geo_ids)s)
           """
    result = gpd.read_postgis(stmt, self.cursor.connection, "geom", params = {"geo_ids": geo_ids})
    return result


  def join(self,
           left_type_name : str,
           right_type_name : str,
           join_fn : Optional[Callable[..., Any]] = None,
           **kwargs : Any,
          ) -> None:
    dataframe_names = set(self.dataframes.keys()).union(self.geodataframes.keys())
    dataframe_names.add(self.LOCAL)
    dataframe_names.add(self.GEOM)

    if left_type_name not in dataframe_names:
      raise Exception(f"No typename {left_type_name} found.")
    if right_type_name not in dataframe_names:
      raise Exception(f"No typename {right_type_name} found.")

    self.joins[(left_type_name, right_type_name)] = (join_fn, kwargs)


  def popDataFrame(self, type_name : str) -> Optional[AnyDataFrame]:
    maybe_gdf = self.geodataframes.get(type_name)
    if maybe_gdf is not None:
      del self.geodataframes[type_name]
      gdf = maybe_gdf
      return gdf
    maybe_df = self.dataframes.get(type_name)
    if maybe_df is not None:
      del self.dataframes[type_name]
      df = maybe_df
      return df
    return None


  # TODO: Doesn't handle joins between non-geom geodataframes
  def getMatrix(self) -> gpd.GeoDataFrame:
    all_dataframe_names = set(self.dataframes.keys()).union(self.geodataframes.keys())
    joined_dataframe_names : Set[str] = set()

    out : gpd.GeoDataFrame = self.get_geometry()

    # Do explicit joins first
    for (left_type_name, right_type_name), (join_fn, kwargs) in self.joins.items():
      left : Optional[AnyDataFrame] = None
      right : Optional[AnyDataFrame] = None

      maybe_left = self.popDataFrame(left_type_name)
      maybe_right = self.popDataFrame(right_type_name)
      if maybe_left is None and left_type_name == self.GEOM:
        left = out
        right = maybe_right
      elif maybe_right is None and right_type_name == self.GEOM:
        right = out
        left = maybe_left
      elif maybe_left is None and maybe_right is None:
        raise Exception(f"Unknown join error on '{left_type_name}' and '{right_type_name}'")
      elif maybe_left is None or maybe_right is None:
        if maybe_left is None:
          left = out
          right = maybe_right
        elif maybe_right is None:
          left = maybe_left
          right = out
      else:
        left = maybe_left
        right = maybe_right

      if join_fn is None:
        if isinstance(left, gpd.GeoDataFrame) and isinstance(right, gpd.GeoDataFrame):
          join_fn = gpd.GeoDataFrame.sjoin
        else:
          join_fn = pd.DataFrame.merge
      result = join_fn(left, right, **kwargs)

    for k, df in self.dataframes.items():
      out = out.merge(df, on="geom_id", how="inner")

    for k, gdf in self.geodataframes.items():
      out = out.sjoin(df, **kwargs, rsuffix=k)

    return out



