from typing import Dict

import argparse
import asyncio

from demeter.db import TableId
from demeter.db import getConnection

from . import main_loop
from .example import getStartingGeoms, insertTree, do_stop
from .spatial_utils import getKey

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Store meteomatics data with dynamic spatial-resolution using nested polygons')
  parser.add_argument('--keep_ancestry', action='store_true', help='Track branch nodes in database', default=False)
  parser.add_argument('--keep_unused', action='store_true', help='Save leaf nodes even if they do not contain a point of interest', default=False)
  args = parser.parse_args()

  connection = getConnection()
  cursor = connection.cursor()

  U = args.keep_unused
  A = args.keep_ancestry

  start_polygon, start_points, root_id, maybe_root_node_id = getStartingGeoms(cursor, U, A)
  node_id_lookup : Dict[str, TableId] = {}
  if (r := maybe_root_node_id):
    k = getKey(start_polygon)
    node_id_lookup[k] = r

  branches, leaves = asyncio.run(main_loop(start_polygon, start_points, do_stop, U, A))

  insertTree(cursor, branches, leaves, node_id_lookup, root_id, A)

  connection.commit()
