from typing import List, Tuple, Dict

import asyncio
from asyncio import Task

from typing import Optional
from collections import deque
from enum import IntEnum

from shapely.geometry import Polygon as Poly, Point # type: ignore

from .valuer import Valuer, Value
from .spatial_utils import getKey, split


class StopState(IntEnum):
  NO_POINTS = -1

def do_stop(p : Poly,
            v : Value,
            ancestry : List[Poly],
            valuer : Valuer,
            my_points : List[Point],
           ) -> float:
  if len(my_points) <= 0:
    return StopState.NO_POINTS
  if len(ancestry) < 2:
    return False

  parent = ancestry[-1]
  grandparent = ancestry[-2]
  pv = valuer.get_value_nowait(parent)
  gpv = valuer.get_value_nowait(grandparent)
  total_diff = abs(pv - v) + abs(gpv - v) + abs(pv - gpv)
  if total_diff > 1:
    return False
  return total_diff or 1e-4


from typing import Set

from time import time

async def main(root : Poly,
               points_of_interest : List[Point],
               keep_unused : bool,
               keep_ancestry : bool,
              ) -> Tuple[List[Tuple[float, Poly, Poly]], List[Tuple[float, Poly, Poly]]]:
  v = Valuer()
  branches : List[Poly] = []
  leaves : List[Poly] = []

  running = asyncio.create_task(v.request_loop())
  tasks : Dict[Task[Tuple[Value, List[Poly], List[Point]]], Poly] = {}

  _value, _ancestry, my_points = await v.get_value(root, [], points_of_interest)

  if len(points_of_interest) != len(my_points):
    print("Some points did not fall in starting range: ",len(points_of_interest) - len(my_points))
  if not len(my_points):
    print("No valid points of interest were provided. Performing exhaustive query. This may take awhile.")

  q : deque[Tuple[Poly, List[Poly], List[Point]]] = deque(((root, [], my_points), ))

  counter = 0
  pending : Set[Task[Tuple[Value, List[Poly], List[Point]]]] = set()
  print("KEEP UNUSED IS: ",keep_unused)
  while len(q) or len(pending):
    start = int(time())
    while len(q) and int(time()) - start < 5:
      parent, parent_ancestry, parent_points = q.pop()
      (my_ancestry := parent_ancestry.copy()).append(parent)
      parts = split(parent)
      tasks.update({asyncio.create_task(v.get_value(p, my_ancestry, parent_points)) : p for p in parts})
    completed, pending = await asyncio.wait(tasks, timeout=1)

    #print("\nLEAVE COUNT: ",len(leaves))
    #print("Completed: ",len(completed))
    #print("PENDING: ",len(pending))
    #print("Q SIZE: ",len(q))

    for c in completed:
      p = tasks[c]
      value, ancestry, my_points = c.result()
      del tasks[c]
      stop = do_stop(p, value, ancestry, v, my_points)
      try:
        parent = ancestry[-1]
      except IndexError:
        parent = None
      if stop:
        if keep_unused or stop is not StopState.NO_POINTS:
          leaves.append((value, p, parent))
      else:
        if keep_ancestry:
          branches.append((value, p, parent))
        q.appendleft((p, ancestry, my_points))
    counter += 1

  return branches, leaves

from demeter.db import getConnection

from typing import Any
from typing import cast
from demeter.data import Geom
from shapely import wkb # type: ignore

def getPoints(cursor : Any) -> List[Point]:
  cursor.execute("select G.* from geom G, field F where F.owner_id = 2 and F.geom_id = G.geom_id limit 100")

  out : List[Point] = []
  rows = cursor.fetchall()
  for r in rows:
    g = cast(Geom, r)
    some_shape = wkb.loads(g.geom, hex=True)

    try:
      (lat, long) = some_shape.centroid.coords[0]
      out.append(Point(lat, long))
    except TypeError:
      continue

  return out

from demeter.data import insertOrGetGeom, insertOrGetLocalType
from demeter.data import LocalType
from demeter.db import TableId

from demeter.grid import Root, Node, Ancestry
from demeter.grid import insertRoot, insertNode, insertAncestry
from shapely.geometry import MultiPoint

def insertNodes(ps : List[Tuple[float, Poly, Poly]],
                node_id_lookup : Dict[str, TableId],
                root_id : TableId,
                keep_ancestry : bool,
               ) -> Tuple[List[Tuple[float, Poly, Poly]],
                          List[Tuple[TableId, Optional[TableId]]]
                         ]:
  still_pending : List[Tuple[float, Poly, Poly]] = []
  table_ids : List[Tuple[TableId, Optional[TableId]]] = []
  for value, x, parent in ps:
    n = Node(
          polygon = x,
          value = value,
        )
    node_id = insertNode(cursor, n)

    maybe_parent_id : Optional[TableId] = None
    if keep_ancestry and parent is not None:
      parent_key = getKey(parent)
      if parent_key in node_id_lookup:
        #print("FETCHING: ",parent_key)
        parent_node_id = maybe_parent_id = node_id_lookup[parent_key]
        a = Ancestry(
              root_id = root_id,
              parent_node_id = parent_node_id,
              node_id = node_id,
            )
        maybe_ancestor_key = insertAncestry(cursor, a)
      else:
        still_pending.append((value, x, parent))
    table_ids.append((node_id, maybe_parent_id))

    k = getKey(x)
    #print("STORING: ",k)
    node_id_lookup[k] = node_id

  return still_pending, table_ids


from demeter.data import Polygon
from shapely.geometry import CAP_STYLE, JOIN_STYLE

def pointsToBound(points : List[Point]) -> Poly:
  mp = MultiPoint(points)
  x1, y1, x2, y2 = bounds =  mp.bounds
  unbuffered_polygon_bounds = ((x1, y1), (x2, y1), (x2, y2), (x1, y2))
  unbuffered_polygon = Poly(unbuffered_polygon_bounds)
  b = unbuffered_polygon.buffer(0.00001, cap_style=CAP_STYLE.square, join_style=JOIN_STYLE.mitre)
  return b


import argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Store meteomatics data with dynamic spatial-resolution using nested polygons')
  parser.add_argument('--keep_ancestry', action='store_true', help='Track branch nodes in database', default=False)
  parser.add_argument('--keep_unused', action='store_true', help='Save leaf nodes even if they do not contain a point of interest', default=False)
  args = parser.parse_args()

  connection = getConnection()
  cursor = connection.cursor()

  points = getPoints(cursor)
  start_polygon = pointsToBound(points)

  root_node = Node(
        polygon = start_polygon,
        value = float("nan"),
      )
  root_node_id = insertNode(cursor, root_node)

  polygon_bounds = tuple(start_polygon.exterior.coords)

  geom = Geom(
           crs_name = "urn:ogc:def:crs:EPSG::4326",
           type = 'Polygon',
           coordinates = (polygon_bounds, ),
         )
  bound_geom_id = insertOrGetGeom(cursor, geom)

  l = LocalType(
    type_name = "abi grid test 8-29",
    type_category = "abi test category",
  )
  local_type_id = insertOrGetLocalType(cursor, l)

  r = Root(
        geom_id = bound_geom_id,
        local_type_id = local_type_id,
  )
  root_id = insertRoot(cursor, r)

  start_points = points
  branches, leaves = asyncio.run(main(start_polygon, start_points, args.keep_unused, args.keep_ancestry))

  node_id_lookup : Dict[str, TableId] = {getKey(start_polygon) : root_node_id}
  print("NUM BRANCHES: ",len(branches))
  print("NUM LEAVES: ",len(leaves))
  branches_to_insert = branches
  leaves_to_insert = leaves
  while len(branches_to_insert) > 0 or len(leaves_to_insert) > 0:
    branches_to_insert, branch_nodes = insertNodes(branches_to_insert, node_id_lookup, root_id, args.keep_ancestry)
    leaves_to_insert, leaf_nodes = insertNodes(leaves_to_insert, node_id_lookup, root_id, args.keep_ancestry)

    # TODO: Options for:
    #       Pick up from existing points
    #       Delete node when it is replaced with finer resolutions
    #       Logging

  connection.commit()


