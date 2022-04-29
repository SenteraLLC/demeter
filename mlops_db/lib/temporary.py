import json
import os

from . import schema_api
from . import types

from typing import Any, Generator, List

# TODO: Use xcom or environment variables

my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))

def load_keys(cursor : Any,
              geo_keys : List[types.GeoSpatialKey],
              temporal_keys : List[types.TemporalKey],
             ) -> Generator[types.Key, None, None]:
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
