import json
import os

from . import schema_api
from . import types

from typing import Any, Generator, TypedDict

import psycopg2
from psycopg2.extensions import connection as PGConnection


# TODO: Get this information via Airflow XCOM
my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))


# TODO: Temporary, will get from XCOM / CLI at some point
def load_keys(cursor : Any) -> Generator[types.Key, None, None]:
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


class PostgresConfig(TypedDict):
  host     : str
  port     : int
  database : str
  options  : str


# TODO: Get this information from ENV variables
mlops_db_connection : PGConnection = psycopg2.connect(host="localhost", options="-c search_path=test_mlops,public", database="postgres")
S3_ROLE_ARN = "arn:aws:iam::832393361842:role/service-role/AmazonSageMaker-ExecutionRole-20220210T101825"
BUCKET_NAME = "sentera-sagemaker-dev"


