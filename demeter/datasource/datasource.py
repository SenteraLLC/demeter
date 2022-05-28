from typing import List, Optional, Tuple, Any, Dict, Callable, Set, Type

from typing import cast

from io import BytesIO
import uuid

from ..types.execution import Key
from ..types.inputs import S3TypeDataFrame, S3Object
from ..inputs import insertS3Object, insertS3ObjectKeys
from ..types.local import LocalType


from .base import DataSourceBase
from .util import createKeywordArguments
from .local import getLocalRows
from .http import getHTTPRows
from .s3 import getRawS3, rawToDataFrame
from .s3_file import SupportedS3DataType, AnyDataFrame, S3FileMeta
from .types import KeyToArgsFunction, OneToOneResponseFunction, ResponseFunction

import pandas as pd
import geopandas as gpd # type: ignore



# TODO: Defer S3 downloads until joining or manually altered
class DataSource(DataSourceBase):
  def __init__(self,
               keys               : List[Key],
               function_id        : int,
               execution_id       : int,
               cursor             : Any,
               s3_connection      : Any,
               keyword_arguments  : Dict[str, Any],
               keyword_types      : Dict[str, Type],
              ):
    super().__init__(cursor, function_id, execution_id)

    self.s3_connection = s3_connection
    self.cursor = cursor
    self.keys = self.execution_summary.inputs.keys = keys
    self.execution_summary.inputs.keyword = createKeywordArguments(keyword_arguments, keyword_types, execution_id, function_id)

    self.dataframes : Dict[str, pd.DataFrame] = {}
    self.geodataframes : Dict[str, gpd.GeoDataFrame] = {}
    self.joins : Dict[Tuple[str, str], Tuple[Optional[Callable[..., AnyDataFrame]], Dict[str, Any]]] = {}



  def _local(self, local_types : List[LocalType]) -> pd.DataFrame:
    if self.LOCAL in self.dataframes:
      raise Exception("Local data can only be acquired once.")

    rows = getLocalRows(self.cursor, self.keys, local_types, self.execution_summary)
    df = pd.DataFrame(rows)
    self.dataframes[self.LOCAL] = df
    return df


  # Does not support GeoDataFrames via http
  def _http(self,
            type_name    : str,
            param_fn     : Optional[KeyToArgsFunction] = None,
            json_fn      : Optional[KeyToArgsFunction] = None,
            response_fn  : ResponseFunction = OneToOneResponseFunction,
            http_options : Dict[str, Any] = {}
           ) -> pd.DataFrame:
    raw_results = getHTTPRows(self.cursor, self.keys, self.execution_summary, type_name, param_fn, json_fn, response_fn, http_options)
    results : List[Dict[str, Any]] = []
    for key, row in raw_results:
      results.append(dict(**key, **row))

    df = pd.DataFrame(results)
    self.dataframes[type_name] = df
    return df


  def _s3(self,
          type_name   : str,
         ) -> SupportedS3DataType:
    raw_results, maybe_tagged_s3_subtype = getRawS3(self.cursor, self.s3_connection, self.keys, type_name, self.execution_summary)
    if maybe_tagged_s3_subtype is not None:
      tagged_s3_subtype = maybe_tagged_s3_subtype
      tag = tagged_s3_subtype.tag
      subtype = tagged_s3_subtype.value
      if tag == S3TypeDataFrame:
        dataframe_subtype = cast(S3TypeDataFrame, subtype)
        maybe_df, maybe_gdf = rawToDataFrame(raw_results, dataframe_subtype)
        if maybe_df is not None:
          df = self.dataframes[type_name] = maybe_df
          return df
        elif maybe_gdf is not None:
          gdf = self.geodataframes[type_name] = maybe_gdf
          return gdf
        else:
          raise Exception("Unknown dataframe.")

    return raw_results


  def upload(self,
             s3_type_id  : int,
             bucket_name : str,
             blob        : BytesIO,
             s3_key      : str = str(uuid.uuid4()),
            ) -> int:
    self.s3_connection.Bucket(bucket_name).upload_fileobj(Key=s3_key, Fileobj=blob)
    s3_object = S3Object(
                  s3_type_id = s3_type_id,
                  key = s3_key,
                  bucket_name = bucket_name,
                )
    s3_object_id = insertS3Object(self.cursor, s3_object)
    insertS3ObjectKeys(self.cursor, s3_object_id, self.keys, s3_type_id)
    return s3_object_id


  def upload_file(self,
                  s3_type_id   : int,
                  bucket_name  : str,
                  s3_file_meta : S3FileMeta,
                 ) -> int:
    s3_filename_on_disk = s3_file_meta["filename_on_disk"]
    f = open(s3_filename_on_disk, "rb")
    as_bytes = BytesIO(f.read())

    s3_key = s3_file_meta["key"]
    return self.upload(s3_type_id, bucket_name, as_bytes, s3_key)


  def get_geometry(self) -> gpd.GeoDataFrame:
    geo_ids = [k.geom_id for k in self.keys]
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
      left_suffix = str(uuid.uuid4())
      out = out.sjoin(gdf, lsuffix=left_suffix, rsuffix=k)

      ljoin = lambda s : "_".join([s, left_suffix])
      to_rename = {"geom_id", "container_geom_id"}
      out.rename(columns={ljoin(s) : s for s in to_rename}, inplace=True)

    return out

