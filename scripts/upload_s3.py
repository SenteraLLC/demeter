import json
import uuid
import os

import pandas as pd
import geopandas as gpd # type: ignore
import psycopg2
import psycopg2.extras
from scoping import scoping # type: ignore

from typing import Any, Tuple, Union

from demeter.connections import getS3Connection
from demeter.datasource.datasource import DataSource
from demeter import temporary

from demeter.datasource.s3_file import S3File

from demeter import S3Type, TaggedS3SubType, S3TypeDataFrame
from demeter import insertOrGetS3TypeDataFrame


if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  s3_role_arn = os.environ['S3_ROLE_ARN']

  bucket_name = os.environ['BUCKET_NAME']

  s3_connection : Any = getS3Connection()

  test_keys = list(temporary.load_keys(cursor))

  datasource = DataSource(test_keys, 0, 0, cursor, s3_connection, {}, {})

  def newS3TypeDataFrame(type_name : str,
                         driver : str,
                         has_geometry : bool
                        ) -> Tuple[int, TaggedS3SubType]:
    s3_type = S3Type(
                type_name = type_name,
              )
    s3_type_data_frame = S3TypeDataFrame(
                           driver = driver,
                           has_geometry = has_geometry,
                         )
    s3_type_id = insertOrGetS3TypeDataFrame(cursor, s3_type, driver, has_geometry)

    tagged_s3_subtype = TaggedS3SubType(
                          tag = types.S3TypeDataFrame,  # type: ignore
                          value = s3_type_data_frame,
                        )

    return s3_type_id, tagged_s3_subtype

  newS3TypeDataFrame("test_geojson_type", "GeoJSON", True)

  geoms = datasource.get_geometry()

  ns = pd.Series(range(0, len(geoms)))

  geoms_df = geoms.assign(N = ns)

  test_file_prefix = "s3_test_script"

  def uploadTestFile(value : Union[pd.DataFrame, gpd.GeoDataFrame],
                     type_name : str,
                     driver : str,
                     has_geometry : bool
                    ) -> None:
    s3_type_id, s3_type_data_frame = newS3TypeDataFrame(type_name, driver, has_geometry)

    to_upload = S3File(value, test_file_prefix)
    s3_file_meta = to_upload.to_file(s3_type_data_frame)
    datasource.upload_file(s3_type_id, bucket_name, s3_file_meta)


  # Geometric type
  with scoping():
    type_name = "my_test_geo_type"
    driver = "GeoJSON"
    has_geometry = True
    uploadTestFile(geoms_df, type_name, driver, has_geometry)

  geom_id_column = geoms_df["geom_id"]
  nongeoms_df = pd.DataFrame().assign(geom_id = geom_id_column, N = ns)

  # Non-Geometric type
  with scoping():
    type_name = "my_test_nongeo_type"
    driver = "json"
    has_geometry = False
    uploadTestFile(nongeoms_df, type_name, driver, has_geometry)

  connection.commit()
