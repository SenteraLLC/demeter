from typing import Dict, Iterator

import json
import argparse
import asyncio
from datetime import datetime, timezone, timedelta

from demeter.db import TableId
from demeter.db import getConnection

from . import main_loop
from .example import getStartingGeoms, insertTree, do_stop
from .spatial_utils import getKey

SHORT = "%Y-%m-%dZ"
LONG = " ".join([SHORT, "%H-%M-%SZ"])
DATE_FORMATS = [SHORT, LONG]

def parseTime(s : str) -> datetime:
  for f in DATE_FORMATS:
    try:
      return datetime.strptime(s, f)
    except ValueError:
      pass
  raise ValueError(f"Invalid date string '{s}' does not match format: {DATE_FORMATS}")


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
  parser.add_argument('--keep_unused', action='store_true', help='Save leaf nodes even if they do not contain a point of interest', default=False)
  parser.add_argument('--stats', action='extend', help='List of meteomatic stats on which to query', required=True, nargs="+")
  parser.add_argument('--start_time', type=parseTime, help=f'Time on which to start data collection. {DATE_FORMATS}', default=datetime.now(timezone.utc))
  parser.add_argument('--end_time', type=parseTime, help=f'Time on which to end data collection. {DATE_FORMATS}', default=datetime.now(timezone.utc))
  parser.add_argument('--time_delta', type=json.loads, help=f'Python datetime.timedelta keyword arguments as json', default=timedelta(days=1))
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

  U = args.keep_unused

  for d in yieldTimeRange(start, end, delta):

    for s in args.stats:
      start_polygon, start_points, root_id, root_node_id = getStartingGeoms(cursor, U, d, s)
      node_id_lookup : Dict[str, TableId] = {}
      k = getKey(start_polygon)
      node_id_lookup[k] = root_node_id


      branches, leaves = asyncio.run(main_loop(start_polygon, start_points, do_stop, U, d, s))
      insertTree(cursor, branches, leaves, node_id_lookup, root_id)

  connection.commit()

