from typing import Sequence, Any, Mapping, Iterable

import json

from ... import data

def toGeoSpatialKeys(filename :str) -> Sequence[data.GeoSpatialKey]:
  f = open(filename)
  return [data.GeoSpatialKey(geom_id=g["geom_id"], field_id=g["field_id"])
          for g in json.load(f)
         ]

def toTemporalKeys(filename :str) -> Sequence[data.TemporalKey]:
  f = open(filename)
  return [data.TemporalKey(start_date=d["start_date"], end_date=d["end_date"])
          for d in json.load(f)
         ]

def loadKeys(cursor : Any,
             geo_keys : Sequence[data.GeoSpatialKey],
             temporal_keys : Sequence[data.TemporalKey],
            ) -> Iterable[data.Key]:
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

