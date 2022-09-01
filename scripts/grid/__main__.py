from typing import Dict, Iterator, Optional

import json
import argparse
import asyncio
from datetime import datetime, timezone, timedelta

from demeter.db import TableId
from demeter.db import getConnection

from .search import findRootByPoint
from .example import getStartingGeoms, insertTree, do_stop, getPoints, pointsToBound, getLocalType
from . import main_loop

from .spatial_utils import getKey

SHORT = "%Y-%m-%dZ"
LONG = " ".join([SHORT, "%H-%M-%SZ"])
DATE_FORMATS = [SHORT, LONG]
DATE_FORMATS_STR = " || ".join([s.replace('%', '%%') for s in DATE_FORMATS])

def parseTime(s : str) -> datetime:
  for f in DATE_FORMATS:
    try:
      return datetime.strptime(s, f)
    except ValueError:
      pass
  raise ValueError(f"Invalid date string '{s}' does not match format:\n{DATE_FORMATS_STR}")

import json
from shapely.geometry import Point, Polygon as Poly # type: ignore
def parsePoly(s : str) -> Poly:
  coords = json.loads(s)
  return Poly(coords)


from copy import deepcopy

def yieldTimeRange(a : datetime,
                   b : datetime,
                   delta : timedelta,
                  ) -> Iterator[datetime]:
  x = deepcopy(a)
  while x < b:
    yield x
    x += delta

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Store meteomatics data with dynamic spatial-resolution using nested polygons')
  parser.add_argument('--stats', action='extend', help='List of meteomatic stats on which to query', required=True, nargs="+")

  parser.add_argument('--start_time', type=parseTime, help=f'Time on which to start data collection.\n{DATE_FORMATS_STR}', default=datetime.now(timezone.utc))
  parser.add_argument('--end_time', type=parseTime, help=f'Time on which to end data collection.\n{DATE_FORMATS_STR}', default=datetime.now(timezone.utc))
  parser.add_argument('--time_delta', type=json.loads, help=f'Python datetime.timedelta keyword arguments as json', default=timedelta(days=1))

  parser.add_argument('--bounds', type=parsePoly, help="Polygon bounds on which to build a new root. By default, try to use an existing root.")
  parser.add_argument('--points', type=Point, nargs="+", help="A list of points on which to collect data. Temporarily, this parameter is not required. When left blank it will default to random points from the demeter DB")
  # TODO: Allow a user to supply 'bounds' or 'points' alone
  #       Only bounds -> Do not consider points of interest, exhaustive
  #       Only points -> Auto-generate bounds from points if it does not exist

  parser.add_argument('--keep_unused', action='store_true', help='Save leaf nodes even if they do not contain a point of interest', default=False)

  args = parser.parse_args()
  start = args.start_time
  end = args.end_time
  delta = args.time_delta
  if start > end:
    raise Exception(f'End time "{end}" cannot occur before start date "{start}".')
  if delta.total_seconds() <= 0:
    raise Exception(f'Invalid time delta "{delta}". Must be greater than zero.')

  connection = getConnection()
  cursor = connection.cursor()

#  print("START.")
#  from shapely.geometry import Point
#  from .search import getTree
#  points = [Point(42.843863, -111.27541), Point(43.77503, -111.96389), Point(47.616118, -111.2754108)]
#  leaves = getTree(cursor, TableId(123), points)
#  print("LEN LEAVES: ",len(leaves))
#  for l in leaves:
#    print(l)
#  print("LEN LEAVES: ",len(leaves))
#  print("END.")
#  import sys
#  sys.exit(1)

  U = args.keep_unused

  # TODO: Remove the 'or getPoints' expr when there is a better means of testing
  points = args.points or getPoints(cursor)

  start_polygon = args.bounds or pointsToBound(points)

  for d in yieldTimeRange(start, end, delta):

    for s in args.stats:
      local_type_id = getLocalType(cursor, s)
      existing_roots = findRootByPoint(cursor, points, local_type_id)
      if not len(existing_roots):
        root_id, root_node_id = getStartingGeoms(cursor, points, start_polygon, U, d, s)
      elif len(existing_roots) > 1:
        root_id,

      node_id_lookup : Dict[str, TableId] = {}
      k = getKey(start_polygon)
      node_id_lookup[k] = root_node_id

      branches, leaves = asyncio.run(main_loop(start_polygon, points, do_stop, U, d, s))
      insertTree(cursor, branches, leaves, node_id_lookup, root_id)

  connection.commit()


