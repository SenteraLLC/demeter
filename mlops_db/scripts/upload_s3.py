import os
import json
import uuid
import itertools

import psycopg2

from typing import Any
from typing import cast

from ..lib.ingest import upload
from ..lib.connections import getS3Connection

from ..lib.schema_api import insertGeoSpatialKey, insertTemporalKey
from ..lib.types import GeoSpatialKey, TemporalKey

# TODO: Move to ENV
from ..lib.function import S3_ROLE_ARN, BUCKET_NAME

my_path = os.path.dirname(os.path.realpath(__file__))
print("PATH: ",my_path)
GEO_KEYS = json.load(open(os.path.join(my_path, "./test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "./test_data/temporal.json")))

if __name__ == "__main__":
  hostname = "localhost"
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host=hostname, dbname="postgres", options=options)
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  s3_connection       : Any = getS3Connection(S3_ROLE_ARN)

  geospatial = (g for g in GEO_KEYS)
  temporal   = (t for t in TEMPORAL_KEYS)
  for i, (g, t) in enumerate(itertools.product(geospatial, temporal)):
    gkey = cast(GeoSpatialKey, g)
    gkey_id = insertGeoSpatialKey(cursor, gkey)
    tkey = cast(TemporalKey, t)
    tkey_id = insertTemporalKey(cursor, tkey)

    bucket_name = "sentera-sagemaker-dev"
    key_name = str(i) + "_testkey"

    x = hash(str(gkey_id) + str(tkey_id))
    tmp_filename = os.path.join("/tmp", str(uuid.uuid4()))
    with open(tmp_filename, "w") as f:
      f.write(str(x))
      print(f"Wrote '{x}' for {i}")

    with open(tmp_filename, "rb") as g:
      upload(s3_connection, bucket_name, key_name, g)



