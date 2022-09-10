# TODO: Resolve flake8 errors
# flake8: noqa

from typing import Dict, Iterator, Optional, Tuple, Any

import json
import argparse
import asyncio
from datetime import datetime, timezone, timedelta

from demeter.db import TableId
from demeter.db import getConnection
from demeter.grid import getRoot, getNodePolygon

from shapely import wkb # type: ignore

from .search import findRootByPoint, getTree
from .example import getStartingGeoms, insertTree, do_stop, getPoints, pointsToBound, getLocalType
from .valuer import Valuer, Value

from . import main_loop

from .spatial_utils import getKey, getNodeKey

DATE = "%Y-%m-%d"
SHORT = DATE + "Z"
LONG = " ".join([DATE, "T%H-%M-%SZ"])
DATE_FORMATS = [SHORT, LONG]
DATE_FORMATS_STR = " || ".join([s.replace('%', '%%') for s in DATE_FORMATS])

def parseTime(s : str) -> datetime:
  for f in DATE_FORMATS:
    try:
      return datetime.strptime(s, f)
    except ValueError:
      pass
  raise ValueError(f"Invalid date string '{s}' does not match format: {DATE_FORMATS_STR}")

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


from typing import List

async def main(cursor : Any,
               stat : str,
               date : datetime,
               points : List[Point],
              ) -> None:
  s = stat
  local_type_id = getLocalType(cursor, s)
  existing_roots = findRootByPoint(cursor, points, local_type_id)

  d = date
  v = Valuer(d, s)
  running = asyncio.create_task(v.request_loop())

  node_id_lookup : Dict[str, TableId] = {}
  seed_polys : List[Tuple[Poly, List[Poly], List[Point]]] = []

  if not len(existing_roots):
    root_id, root_node_id = getStartingGeoms(cursor, points, start_polygon, U, d, s)
    k = getNodeKey(start_polygon, 0)
    node_id_lookup[k] = root_node_id

    root_value, _, __, root_points = await v.get_value(start_polygon, [], points)
    stmt = "update node set value = %(v)s where node_id = %(n)s returning true as success"
    args = {"v": root_value, "n": root_node_id}
    cursor.execute(stmt, args)
    results = cursor.fetchall()
    success = results[0].success
    if not success:
      print("Failed to update root node value.")

    if len(points) != len(root_points):
      print("Some points did not fall in starting range: ",len(points) - len(root_points))
    if not len(root_points):
      # TODO: Add this functionality
      # print("No valid points of interest were provided. Performing exhaustive query. This may take awhile.")
      raise Exception("No points of interest is not currently supported.")
    seed_polys.append((start_polygon, [], root_points))

  elif len(existing_roots) > 1:
    raise Exception("Too many")

  else:
    # TODO: Where to store time delta
    #import sys
    root_id, points = existing_roots.popitem()
    root = getRoot(cursor, root_id)
    root_node_id = root.root_node_id
    poly = getNodePolygon(cursor, root_node_id)

   # TODO FIXME Fix time delta
    existing = getTree(cursor, root_id, points, d, timedelta(days=1, minutes=-1))

    # TODO: Handle key calc here or in 'toTree' sql?
    # TODO: psycopg3 should let us skip directly to shapely::Polygon

    # TODO: Could be smart about not loading every possible result
    #v.load_existing({ getKey(wkb.loads(m["bounds"], hex=True)) : Value(m["value"]) for node_id, m in existing })
    v.load_existing(m[1] for m in existing)

    for node_id, n in existing.leaves.items():
      # The leaf nodes may be looked up as parents in 'insertTree'
      k = getNodeKey(wkb.loads(n["bounds"], hex=True), n["level"])
      node_id_lookup[k] = node_id

      # We need to store metadata for the nodes in the Valuer
      # TODO: Do in SQL
      my_ancestry = [ getNodePolygon(cursor, node_id) for node_id in n["ancestry"] ]
      p = wkb.loads(n["bounds"], hex=True)
      points = [wkb.loads(z, hex=True) for z in n["points"]]
      if getNodeKey(p, n["level"]) not in v.existing_keys:
        seed_polys.append((p, my_ancestry, points))

  branches, leaves = await main_loop(start_polygon, do_stop, U, v, seed_polys)

  # TODO: Child geoms need to be clipped to parent, then we can add constraint
  insertTree(cursor, branches, leaves, node_id_lookup, root_id)
  running.cancel()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Store meteomatics data with dynamic spatial-resolution using nested polygons')
  parser.add_argument('--stats', action='extend', help='List of meteomatic stats on which to query', required=True, nargs="+")

  parser.add_argument('--start_time', type=parseTime, help=f'Time on which to start data collection.\n{DATE_FORMATS_STR}', default=datetime.now(timezone.utc))
  parser.add_argument('--end_time', type=parseTime, help=f'Time on which to end data collection.\n{DATE_FORMATS_STR}', default=datetime.now(timezone.utc))
  parser.add_argument('--step_delta', type=json.loads, help=f'Python datetime.timedelta on which to step', default=timedelta(days=1))

  parser.add_argument('--bounds', type=parsePoly, help="Polygon bounds on which to build a new root. By default, try to use an existing root.")
  parser.add_argument('--points', type=Point, nargs="+", help="A list of points on which to collect data. Temporarily, this parameter is not required. When left blank it will default to random points from the demeter DB")
  # TODO: Allow a user to supply 'bounds' or 'points' alone
  #       Only bounds -> Do not consider points of interest, exhaustive
  #       Only points -> Auto-generate bounds from points if it does not exist

  parser.add_argument('--keep_unused', action='store_true', help='Save leaf nodes even if they do not contain a point of interest', default=False)

  args = parser.parse_args()
  start = args.start_time
  end = args.end_time
  delta = args.step_delta
  if start > end:
    raise Exception(f'End time "{end}" cannot occur before start date "{start}".')
  if delta.total_seconds() <= 0:
    raise Exception(f'Invalid time delta "{delta}". Must be greater than zero.')

  connection = getConnection()
  cursor = connection.cursor()

  U = args.keep_unused

  # TODO: Remove the 'or getPoints' expr when there is a better means of testing
  points = args.points or getPoints(cursor)

  start_polygon = args.bounds or pointsToBound(points)

  for d in yieldTimeRange(start, end, delta):

    for s in args.stats:
      asyncio.run(main(cursor, s, d, points))


  connection.commit()


