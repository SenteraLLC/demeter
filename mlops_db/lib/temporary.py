from typing import Any, Generator, List

import json
import os

from .core.types import GeoSpatialKey, TemporalKey, Key
from .core.api import insertOrGetGeoSpatialKey, insertOrGetTemporalKey

# TODO: Use xcom or environment variables

my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/geospatial.json")))
TEMPORAL_KEYS = json.load(open(os.path.join(my_path, "../scripts/test_data/temporal.json")))

def load_keys(cursor : Any,
              geo_keys : List[GeoSpatialKey],
              temporal_keys : List[TemporalKey],
             ) -> Generator[Key, None, None]:
  for g in GEO_KEYS:
    geospatial_key_id = insertOrGetGeoSpatialKey(cursor, g)

    for t in TEMPORAL_KEYS:
      temporal_key_id = insertOrGetTemporalKey(cursor, t)

      yield Key(
              geospatial_key_id = geospatial_key_id,
              temporal_key_id = temporal_key_id,
              **g,
              **t,
            )   # type: ignore
