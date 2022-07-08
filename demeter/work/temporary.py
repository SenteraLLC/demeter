from typing import Any, Generator, List

import json
import os

from .. import data

# TODO: Use xcom or environment variables

my_path = os.path.dirname(os.path.realpath(__file__))
GEO_KEYS = [ data.GeoSpatialKey(geom_id=g["geom_id"], field_id=g["field_id"]) for g in json.load(open(os.path.join(my_path, "./temp_data/geospatial.json"))) ]
TEMPORAL_KEYS = [ data.TemporalKey(start_date=d["start_date"], end_date=d["end_date"]) for d in json.load(open(os.path.join(my_path, "./temp_data/temporal.json"))) ]

def load_keys(cursor : Any,
              geo_keys : List[data.GeoSpatialKey] = GEO_KEYS,
              temporal_keys : List[data.TemporalKey] = TEMPORAL_KEYS,
             ) -> Generator[data.Key, None, None]:
  for g in geo_keys:
    geospatial_key_id = data.insertOrGetGeoSpatialKey(cursor, g)

    for t in temporal_keys:
      temporal_key_id = data.insertOrGetTemporalKey(cursor, t)

      yield data.Key(
              geospatial_key_id = geospatial_key_id,
              temporal_key_id = temporal_key_id,
              geom_id = g.geom_id,
              field_id = g.field_id,
              start_date = t.start_date,
              end_date = t.end_date,
            )

