from typing import List, Tuple, Dict, Any
from typing import cast

import asyncio
from datetime import datetime, timezone

from typing import Optional

from shapely.geometry import Polygon as Poly, Point # type: ignore

from .valuer import Valuer, Value
from .spatial_utils import getKey
from . import StopState
from . import main_loop


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


from demeter.data import Geom
from shapely import wkb # type: ignore

def getPoints(cursor : Any) -> List[Point]:
  cursor.execute("select G.* from geom G, field F where F.owner_id = 2 and F.geom_id = G.geom_id limit 10")

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
from demeter.grid import insertOrGetRoot, insertNode, insertAncestry
from shapely.geometry import MultiPoint

def insertNodes(cursor : Any,
                ps : List[Tuple[float, Poly, Poly]],
                node_id_lookup : Dict[str, TableId],
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
    if parent is not None:
      parent_key = getKey(parent)
      if parent_key in node_id_lookup:
        parent_node_id = maybe_parent_id = node_id_lookup[parent_key]
        a = Ancestry(
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

THIS_SCRIPT_TOKEN = "meteo-grid-example"


def getLocalTypeName(stat : str) -> str:
  return "_".join([THIS_SCRIPT_TOKEN, stat.lower()])


def getLocalType(cursor : Any, stat : str) -> TableId:
  type_category = "meteomatics grid test category"
  type_name = getLocalTypeName(stat)
  l = LocalType(
    type_name = type_name,
    type_category = type_category,
  )
  local_type_id = insertOrGetLocalType(cursor, l)
  return local_type_id


def getStartingGeoms(cursor : Any,
                     points : List[Point],
                     start_polygon : Poly,
                     keep_unused : bool,
                     time          : datetime,
                     stat          : str,
                    ) -> Tuple[TableId, TableId]:
  root_node = Node(
                polygon = start_polygon,
                # TODO: Find a way to populate this value from the main loop
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
  local_type_id = getLocalType(cursor, stat)
  r = Root(
        local_type_id = local_type_id,
        time = time,
        root_node_id = root_node_id,
      )
  root_id = insertOrGetRoot(cursor, r)

  return root_id, root_node_id


def insertTree(cursor : Any,
               branches : List[Tuple[float, Poly, Poly]],
               leaves : List[Tuple[float, Poly, Poly]],
               node_id_lookup : Dict[str, TableId],
               root_node_id : TableId,
              ) -> None:
  print("# BRANCH NODES: ",len(branches))
  print("# LEAF NODES: ",len(leaves))
  branches_to_insert = branches
  leaves_to_insert = leaves
  while len(branches_to_insert) > 0 or len(leaves_to_insert) > 0:
    branches_to_insert, branch_nodes = insertNodes(cursor, branches_to_insert, node_id_lookup)
    leaves_to_insert, leaf_nodes = insertNodes(cursor, leaves_to_insert, node_id_lookup)

    # TODO: Options for:
    #       Pick up from existing points
    #       Logging
    #       Config for default do_stop function
    #       Custom do_stop functions?
    #       Custom splitter
    #       Find proper "root"
    #       Option to specifiy root geom, dont generate from points
    #       Should it be possible to change the size/geom_id of a root geom?
    #         Hmm, does the root node geom even need to be in "geom" ?
    #         I don't think so.


  return None


