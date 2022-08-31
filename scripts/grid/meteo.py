
import requests
url = 'https://api.meteomatics.com/2022-08-23T00:00:00Z--2022-08-26T00:00:00Z:PT1H/t_2m:C/52.520551,13.461804,50.00,13.00/json'
#x = requests.get("https://api.meteomatics.com/user_stats", headers = {"Authorization": "Basic c2VudGVyYTprUjFLckl5SWJnTU0="})
#print("X: ",x.text)

headers = {"Authorization": "Basic c2VudGVyYTprUjFLckl5SWJnTU0="}
#x = requests.get(url, )

url_template = 'https://api.meteomatics.com/{dates}/{stats}/{coords}/{typ}'

# Go to fifth decimal with lat long
#  https://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude/8674#8674

from datetime import datetime
today = datetime.now().strftime("%Y-%m-%dZ")
temp = "t_2m:C"

import json
from typing import Sequence, Tuple, Dict, Any

def req(dates : Sequence[datetime],
        stats : Sequence[str],
        coords : Sequence[Tuple[float, float]],
        typ    : str = "json"
       ) -> Dict[str, Any]:
  dates_str = ",".join([d.strftime("%Y-%m-%dZ") for d in dates])

  stats_str = ",".join(stats)
  coords_str = "+".join(["{:.5f},{:.5f}".format(cx, cy) for cx, cy in coords])
  #print("COORDS ARE: ",coords_str)

  url = url_template.format(dates=dates_str, stats=stats_str, coords=coords_str, typ="json")
  #print("URL IS: ",url)
  x = requests.get(url, headers=headers)
  #print("X: ",x)
  #print("X: ",x.text)
  #print("X: ",dir(x))
  return json.loads(x.text)


