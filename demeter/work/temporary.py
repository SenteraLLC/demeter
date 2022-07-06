from typing import Any, Generator, List

import json
import os

from ..data.core.types import GeoSpatialKey, TemporalKey
from ..data import Key
from ..data.core import insertOrGetGeoSpatialKey, insertOrGetTemporalKey

# TODO: Use xcom or environment variables

my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = [ GeoSpatialKey(geom_id=g["geom_id"], field_id=g["field_id"]) for g in json.load(open(os.path.join(my_path, "./temp_data/geospatial.json"))) ]
TEMPORAL_KEYS = [ TemporalKey(start_date=d["start_date"], end_date=d["end_date"]) for d in json.load(open(os.path.join(my_path, "./temp_data/temporal.json"))) ]

def load_keys(cursor : Any,
              geo_keys : List[GeoSpatialKey] = GEO_KEYS,
              temporal_keys : List[TemporalKey] = TEMPORAL_KEYS,
             ) -> Generator[Key, None, None]:
  for g in geo_keys:
    geospatial_key_id = insertOrGetGeoSpatialKey(cursor, g)

    for t in temporal_keys:
      temporal_key_id = insertOrGetTemporalKey(cursor, t)

      yield Key(
              geospatial_key_id = geospatial_key_id,
              temporal_key_id = temporal_key_id,
              geom_id = g.geom_id,
              field_id = g.field_id,
              start_date = t.start_date,
              end_date = t.end_date,
            )
