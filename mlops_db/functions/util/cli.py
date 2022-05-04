from typing import Dict, Type, Tuple, Any, List
from typing import cast

import json
import argparse

from ...lib.core.types import GeoSpatialKey, TemporalKey

def parseCLIArguments(name : str, major : int, keyword_types : Dict[str, Type]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
  default_cli_types : Dict[str, Any] = {
    "geospatial_key_file": lambda f : cast(List[GeoSpatialKey], json.load(open(f))),
     "temporal_key_file": lambda f : cast(List[TemporalKey], json.load(open(f))),
  }
  keyword_types.update(default_cli_types)

  parser = argparse.ArgumentParser(f"CLI Arguments for '{name}' version {major}")
  for parameter_name, _type in keyword_types.items():
    parser.add_argument(f"--{parameter_name}", type=_type, required=True)

  all_args = vars(parser.parse_args())
  default_cli_args = { k : all_args.pop(k) for k in default_cli_types }
  args = all_args

  return args, default_cli_args


