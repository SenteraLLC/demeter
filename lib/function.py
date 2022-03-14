from .datasource import DataSource
from .ingest import S3File, LocalFile

from . import schema_api
from . import types

from .connections import PGConnection
from .connections import getS3Connection

import json
import os
from typing import List, TypedDict, Dict, Any, Callable, Tuple, Generator, Union
import psycopg2

from functools import wraps

class PostgresConfig(TypedDict):
  host     : str
  port     : int
  database : str
  options  : str


from psycopg2.extensions import connection as PGConnection


# TODO: Get this information via Airflow XCOM
my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))

# TODO: Temporary, will get from XCOM / CLI at some point
def _load_keys(cursor : Any) -> Generator[types.Key, None, None]:
  for g in GEO_KEYS:
    geospatial_key_id = schema_api.insertOrGetGeoSpatialKey(cursor, g)

    for t in TEMPORAL_KEYS:
      temporal_key_id = schema_api.insertOrGetTemporalKey(cursor, t)

      yield types.Key(
              geospatial_key_id = geospatial_key_id,
              temporal_key_id = temporal_key_id,
              **g,
              **t,
            )   # type: ignore


# TODO: Get this information from ENV variables
mlops_db_connection : PGConnection = psycopg2.connect(host="localhost", options="-c search_path=test_mlops,public", database="postgres")
S3_ROLE_ARN = "arn:aws:iam::832393361842:role/service-role/AmazonSageMaker-ExecutionRole-20220210T101825"
BUCKET_NAME = "sentera-sagemaker-dev"


# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

WrappableFunction = Callable[[DataSource, Any], Union[S3File, LocalFile]]
WrappedFunction = Callable[[WrappableFunction], Any]

def Function(name  : str,
             major : int,
            ) -> WrappedFunction:
  def setup_datasource(fn : WrappableFunction):

    # TODO: How to handle aws credentials and role assuming?
    s3_connection : Any             = getS3Connection(S3_ROLE_ARN)

    cursor = mlops_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # TODO: Temporary
    keys          : Generator[types.Key, None, None] = _load_keys(cursor)
    datasource    : DataSource      = DataSource(keys, cursor, s3_connection)

    # TODO: Warn if non-keyword arguments exist?
    @wraps(fn)
    def add_datasource(**kwargs):
      output = fn(datasource, **kwargs)
      s3_type_id = schema_api.getS3TypeIdByName(cursor, output.type_name)
      s3_type = schema_api.getS3Type(cursor, s3_type_id)
      driver = s3_type["driver"]
      s3_file_meta = output.to_file(driver)

      filename = s3_file_meta["filename_on_disk"]
      datasource.upload_file(s3_type_id, BUCKET_NAME, s3_file_meta, datasource.keys)
      mlops_db_connection.commit()

    return add_datasource

  return setup_datasource

