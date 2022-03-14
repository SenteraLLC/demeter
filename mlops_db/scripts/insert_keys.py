import psycopg2
import datetime

from ..lib.types import GeoSpatialKey, TemporalKey

from ..lib.schema_api import insertGeoSpatialKey, insertTemporalKey

if __name__ == "__main__":
  connection = psycopg2.connect(host="localhost", options="-c search_path=test_mlops,public", database="postgres")

  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


  geo_keys = [
              GeoSpatialKey(
                geom_id = 1,
                field_id = 1,
              ),
              GeoSpatialKey(
                geom_id = 2,
                field_id = 2,
              ),
              GeoSpatialKey(
                geom_id = 3,
                field_id = 3,
              )
             ]

  for g in geo_keys:
    insertGeoSpatialKey(cursor, g)

  temporal_keys = [TemporalKey(
                     start_date = datetime.date(2018, 1, 1),
                     end_date = datetime.date(2020, 12, 31)
                   )
                  ]

  connection.commit()
