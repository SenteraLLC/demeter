from typing import cast

import requests

import os

example_url = 'https://api.meteomatics.com/2022-08-23T00:00:00Z--2022-08-26T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804,50.00,13.00/json'


meteomatics_token = os.getenv("METEOMATICS_TOKEN")

headers = {"Authorization": meteomatics_token}

url_template = 'https://api.meteomatics.com/{dates}/{stats}/{coords}/{typ}'

# Go to fifth decimal with lat long
#  https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

from datetime import datetime

import json
from typing import Sequence, Tuple, Dict, Any, Set

def req(dates : Sequence[datetime],
        stats : Set[str],
        coords : Sequence[Tuple[float, float]],
        typ    : str = "json"
       ) -> Dict[str, Any]:
  dates_str = ",".join([d.strftime("%Y-%m-%dZ") for d in dates])

  stats_str = ",".join(stats)
  coords_str = "+".join(["{:.5f},{:.5f}".format(cx, cy) for cx, cy in coords])

  url = url_template.format(dates=dates_str, stats=stats_str, coords=coords_str, typ="json")
  x = requests.get(url, headers=headers) # type: ignore
  return cast(Dict[str, Any], json.loads(x.text))


