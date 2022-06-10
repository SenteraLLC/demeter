from typing import Dict, Type, Tuple, Any, List
from typing import cast

import json
import argparse

from ..types.core import GeoSpatialKey, TemporalKey

def parseCLIArguments(name : str,
                      major : int,
                      keyword_types : Dict[str, Type[Any]]
                     ) -> Tuple[Dict[str, Any], Dict[str, Any]]:

  def toGeoSpatialKeys(filename :str) -> List[GeoSpatialKey]:
    f = open(filename)
    return [GeoSpatialKey(geom_id=g["geom_id"], field_id=g["field_id"])
            for g in json.load(f)
           ]

  def toTemporalKeys(filename :str) -> List[TemporalKey]:
    f = open(filename)
    return [TemporalKey(start_date=d["start_date"], end_date=d["end_date"])
            for d in json.load(f)
           ]


  default_cli_types : Dict[str, Any] = {
    "geospatial_key_file": toGeoSpatialKeys,
     "temporal_key_file": toTemporalKeys,
  }
  keyword_types.update(default_cli_types)

  parser = argparse.ArgumentParser(f"CLI Arguments for '{name}' version {major}")
  for parameter_name, _type in keyword_types.items():
    parser.add_argument(f"--{parameter_name}", type=_type, required=True)

  all_args = vars(parser.parse_args())
  default_cli_args = { k : all_args.pop(k) for k in default_cli_types }
  args = all_args

  return args, default_cli_args


