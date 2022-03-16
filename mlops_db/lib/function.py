from .datasource import DataSource
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from . import temporary

from .connections import getS3Connection

from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union
import psycopg2

from functools import wraps


# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

WrappableFunction = Callable[[DataSource, Any], Union[S3File, LocalFile]]
WrappedFunction = Callable[[WrappableFunction], Any]

def Function(name  : str,
             major : int,
            ) -> WrappedFunction:
  def setup_datasource(fn : WrappableFunction):

    # TODO: How to handle aws credentials and role assuming?
    s3_connection : Any             = getS3Connection(temporary.S3_ROLE_ARN)

    cursor = temporary.mlops_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    keys          : Generator[types.Key, None, None] = temporary.load_keys(cursor)
    datasource    : DataSource      = DataSource(keys, cursor, s3_connection)

    # TODO: Warn if non-keyword arguments exist?
    @wraps(fn)
    def add_datasource(**kwargs):
      output = fn(datasource, **kwargs)
      s3_type_id = schema_api.getS3TypeIdByName(cursor, output.type_name)
      s3_type = schema_api.getS3Type(cursor, s3_type_id)

      driver = s3_type["driver"]
      s3_file_meta = output.to_file(driver)

      s3_type_name = s3_type["type_name"]
      datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)

      temporary.mlops_db_connection.commit()

    return add_datasource

  return setup_datasource

