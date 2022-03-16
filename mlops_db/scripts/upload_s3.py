import json
import uuid

import pandas as pd
import geopandas as gpd # type: ignore
import psycopg2
import psycopg2.extras

from typing import Any

from ..lib.connections import getS3Connection
from ..lib.datasource import DataSource

from ..lib import temporary
from ..lib import schema_api
from ..lib.types import S3Type
from ..lib.ingest import S3File


if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  s3_connection       : Any = getS3Connection(temporary.S3_ROLE_ARN)

  type_name = "my_test_type"
  driver = "GeoJSON"
  s3_type = S3Type(
              type_name = type_name,
              driver    = driver,
            )
  s3_type_id = schema_api.insertOrGetS3Type(cursor, s3_type)

  test_keys = temporary.load_keys(cursor)
  print("KEYS: ",test_keys)

  datasource = DataSource(test_keys, cursor, s3_connection)

  geoms = datasource.get_geometry()
  print("GEOMS: ",geoms)
  geoms = geoms.assign(N = pd.Series(range(0, len(geoms))))
  print("Geoms now: ",geoms)

  bucket_name = "sentera-sagemaker-dev"
  key_name = str(uuid.uuid4()) + "_test_upload_s3"

  to_upload = S3File(type_name, geoms)
  s3_file_meta = to_upload.to_file(driver)

  datasource.upload_file(s3_type_id, temporary.BUCKET_NAME, s3_file_meta)

  connection.commit()
