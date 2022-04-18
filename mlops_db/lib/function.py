from .datasource import DataSource
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from . import temporary

from .connections import getS3Connection

from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union, TypeVar, Protocol, Optional, Mapping
import psycopg2
import pandas as pd
# TODO: Write a stub for GeoDataFrame
import geopandas as gpd # type: ignore
import argparse
from enum import Enum
import inspect

from functools import wraps

class ExecutionMode(Enum):
  DRY = 1
  CLI = 2
  RUN = 3

class ExecutionOptions(TypedDict):
  mode : ExecutionMode

def parseCLIArguments(fn, name, major) -> Dict[str, Any]:
  # Always remove first argumnet
  signature = inspect.signature(fn)
  first_argument = list(signature.parameters.keys())[0]
  # TODO: Generically dispose of first argumenrgt
  parser = argparse.ArgumentParser(f"CLI Arguments for '{name}' version {major}")
  annotations = fn.__annotations__
  blacklist = ["return", first_argument]
  for a, t in annotations.items():
    if a not in blacklist:
      parser.add_argument(f"--{a}", type=t, required=True)
  return vars(parser.parse_args())


T = TypeVar('T')
AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]
OutputLoadFunction = Callable[[DataSource, Any], gpd.GeoDataFrame]

WrappableFunctionOutputs = Mapping[str, Union[S3File, LocalFile]]

WrappableFunction = Callable[..., WrappableFunctionOutputs]

class AddDataSourceWrapper(Protocol):
  def __call__(self, datasource : DataSource, **kwargs: Any) -> Mapping[str, Any]: ...

class AddGeoDataFrameWrapper(Protocol):
    def __call__(self, **kwargs: Any) -> Mapping[str, Any]: ...

# TODO: Add some deferral objects (like shared ptrs) that allow some modification of values in Load section

# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

def Function(name                : str,
             major               : int,
             output_to_type_name : Mapping[str, str],
             load_fn             : OutputLoadFunction,
            ) -> Callable[[WrappableFunction], AddGeoDataFrameWrapper]:
  def setup_datasource(fn : WrappableFunction) -> AddGeoDataFrameWrapper:
    # TODO: How to handle aws credentials and role assuming?
    s3_connection : Any             = getS3Connection(temporary.S3_ROLE_ARN)

    cursor = temporary.mlops_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    keys       : Generator[types.Key, None, None] = temporary.load_keys(cursor)
    datasource : DataSource      = DataSource(keys, cursor, s3_connection)

    # TODO: Only allow primitive values in kwargs?
    @wraps(fn)
    def add_datasource(*args : Any, **kwargs : Any) -> Mapping[str, AnyDataFrame]:
      if len(args):
        raise Exception(f"All arguments must be named. Found unnamed arguments: {args}")
      mode = ExecutionMode.RUN
      maybe_mode = kwargs.get("mode")
      if maybe_mode is not None:
        mode = maybe_mode
        del kwargs["mode"]

      if mode == ExecutionMode.CLI:
        kwargs = parseCLIArguments(fn, name, major)

      load_fn(datasource, **kwargs) # type: ignore
      m = datasource.getMatrix()

      outputs = fn(m, **kwargs)
      for output_name, output in outputs.items():
        output_type = output_to_type_name[output_name]
        s3_type_id = schema_api.getS3TypeIdByName(cursor, output_type)
        s3_type, maybe_tagged_s3_sub_type = schema_api.getS3Type(cursor, s3_type_id)
        if isinstance(output, S3File):
          s3_file_meta = output.to_file(maybe_tagged_s3_sub_type)

          s3_type_name = s3_type["type_name"]
          datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)
        else:
          raise Exception(f"Bad output provided: {output_name}")
      temporary.mlops_db_connection.commit()
      return outputs

    return add_datasource
  return setup_datasource


# Irrigation Status, Review Data for Yield Quantity, Protein Quantity
# Grower Field, Region (Country),
