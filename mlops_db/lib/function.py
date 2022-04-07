from .datasource import DataSource
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from . import temporary

from .connections import getS3Connection

from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union, TypeVar, Protocol, Optional
import psycopg2
import pandas as pd
# TODO: Write a stub for GeoDataFrame
import geopandas as gpd # type: ignore

from functools import wraps


T = TypeVar('T')
LoadFunction = Callable[[DataSource, Any], T]
AnyDataFrame = Union[gpd.GeoDataFrame, pd.DataFrame]
InputLoadFunction = LoadFunction[AnyDataFrame]
OutputLoadFunction = LoadFunction[gpd.GeoDataFrame]


# TODO: Add some deferral objects (like shared ptrs) that allow some modification of values in Load section

def Load(fn : InputLoadFunction) -> OutputLoadFunction:
  def wrapped(datasource : DataSource, *args, **kwargs) -> DataSource:
    fn(datasource, *args, **kwargs)
    return datasource.getMatrix()
  return wrapped


# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

#WrappableFunction = Callable[[DataSource, Any], Dict[str, Union[S3File, LocalFile]]]
WrappableFunctionOutputs = Union[Dict[str, Union[S3File, LocalFile]]]
#WrappableFunctionOutputs = Dict[str, S3File]
#class WrappableFunction(Protocol):
#  def __call__(self, datasource : DataSource, **kwargs : Dict[str, Any]) -> WrappableFunctionOutputs: ...

WrappableFunction = Callable[..., WrappableFunctionOutputs]

class AddDataSourceWrapper(Protocol):
  def __call__(self, **kwargs: Dict[str, Any]) -> None: ...


#WrappedFunction = Callable[[WrappableFunction], None]

def Function(name    : str,
             major   : int,
             load_fn : Optional[OutputLoadFunction],
            ) -> Callable[[WrappableFunction], AddDataSourceWrapper]:
  def setup_datasource(fn : WrappableFunction) -> AddDataSourceWrapper:

    # TODO: How to handle aws credentials and role assuming?
    s3_connection : Any             = getS3Connection(temporary.S3_ROLE_ARN)

    cursor = temporary.mlops_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    keys          : Generator[types.Key, None, None] = temporary.load_keys(cursor)
    datasource    : DataSource      = DataSource(keys, cursor, s3_connection)

    # TODO: Warn if non-keyword arguments exist?
    # TODO: Only allow primitive values in kwargs?
    @wraps(fn)
    def add_datasource(**kwargs : Dict[str, Any]) -> None:
      outputs = fn(datasource, **kwargs)
      for output_name, output in outputs.items():
        s3_type_id = schema_api.getS3TypeIdByName(cursor, output.type_name)
        s3_type, maybe_tagged_s3_sub_type = schema_api.getS3Type(cursor, s3_type_id)
        if isinstance(output, S3File):
          s3_file_meta = output.to_file(maybe_tagged_s3_sub_type)

          s3_type_name = s3_type["type_name"]
          datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)

      temporary.mlops_db_connection.commit()

    return add_datasource

  return setup_datasource




