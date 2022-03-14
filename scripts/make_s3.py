from typing import Callable

import psycopg2
import psycopg2.extras

from ..lib.types import S3Type
from ..lib.schema_api import insertS3Type


if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  csv_type = S3Type(
              type_name = "test_csv_type",
              driver    = "CSV"
            )
  csv_id = insertS3Type(cursor, csv_type)
  print("CSV type id: ",csv_id)


  gj_type = S3Type(
              type_name = "test_geojson_type",
              driver    = "GeoJSON"
            )
  gj_id = insertS3Type(cursor, gj_type)
  print("GJ type id: ",gj_id)


  connection.commit()
