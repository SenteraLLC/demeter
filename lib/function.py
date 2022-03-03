from .datasource import DataSource, OutputObject

from .connections import PGConnection
from .connections import getS3Connection

import json
import os
from typing import List, TypedDict, Dict, Any, Callable
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
print("PATH: ",my_path)
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))

# TODO: Get this information from ENV variables
mlops_db_connection : PGConnection = psycopg2.connect(host="localhost", options="-c search_path=test_mlops,public", database="postgres")
S3_ROLE_ARN = "arn:aws:iam::832393361842:role/service-role/AmazonSageMaker-ExecutionRole-20220210T101825"


# TODO: Function types limit function signatures, argument types
#       Transformation (S3, HTTP, Local) -> (S3, Local)

# TODO: Replace typing 'Any's
def Function(name  : str,
             major : int,
            ) -> Callable[[Any], Any]:
  def setup_datasource(fn : Callable[[DataSource, Any], OutputObject]):

    # TODO: How to handle aws credentials and role assuming?
    s3_connection       : Any = getS3Connection(S3_ROLE_ARN)
    datasource          : DataSource = DataSource(GEO_KEYS, TEMPORAL_KEYS, mlops_db_connection, s3_connection)

    # TODO: Warn if non-keyword arguments exist?
    @wraps(fn)
    def add_datasource(**kwargs):
      return fn(datasource, **kwargs)
    return add_datasource

  return setup_datasource

