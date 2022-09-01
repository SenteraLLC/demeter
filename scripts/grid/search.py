from typing import Dict, Any, Set, List, Tuple, Optional

from demeter.db import TableId
from shapely.geometry import Polygon as Poly, Point # type: ignore
from shapely import wkb # type: ignore

from .spatial_utils import getKey

def getNodeIdLookup(cursor : Any,
                    root_id : TableId,
                    ) -> Dict[str, TableId]:
  stmt = """
    with recursive node_and_geom as (
      select N.node_id, N.geom
      from root R, node N
      where R.root_id = %(root_id)s and
            R.node_id= N.node_id
      UNION ALL
      select N.node_id, N.geom
      from node_and_geom P
      join node_ancestry A on P.node_id = A.parent_node_id
      join node N on N.node_id = A.node_id
    ) select * from node_and_geom;
  """
  cursor.execute(stmt, {"root_id": root_id})
  results = cursor.fetchall()
  out : Dict[str, TableId] = {}
  for r in results:
    g = wkb.loads(r.geom, hex=True)
    k = getKey(g)
    out[k] = r.node_id
  return out


def findRootByPoint(cursor : Any,
                    points : List[Point],
                    local_type_id : Optional[TableId],
                   ) -> Dict[TableId, List[Point]]:
  # TODO: Postgres probably has native support for Point so the unnesting is pointless
  # TODO: Yep, upgrade to psycopg3
  # TODO: Fix hard coded 4326
  stmt = """
    with fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s) as x, unnest(%(ys)s) as y
    ) select R.root_id, jsonb_agg(P.point) as points
      from root R, node N, fix_point_hack P, local_type T
      where T.local_type_id = %(local_type_id)s and
            R.local_type_id = T.local_type_id and
            R.node_id = N.node_id and
            ST_Contains(N.geom, P.point)
      group by R.root_id
  """
  xs, ys = zip(*((p.x, p.y) for p in points))
  cursor.execute(stmt, {"xs": list(xs), "ys": list(ys), "local_type_id": local_type_id})
  results = cursor.fetchall()
  out : Dict[TableId, List[Point]] = {}
  for r in results:
    out[r.root_id] = [Point(p["coordinates"]) for p in r.points]
  return out


def findRootByGeom(cursor : Any,
                   geom_ids : Set[TableId],
                  ) -> Dict[TableId, Set[TableId]]:
  stmt = """
    select R.root_id, array_agg(gid) as geom_ids
    from root R, node N, geom G, unnest(%(geom_ids)s) as gid
    where gid = G.geom_id and
          R.node_id = N.geom_id and
          ST_Contains(N.geom, G.geom)
    group by R.root_id
  """
  cursor.execute(stmt, {"geom_ids": geom_ids})
  results = cursor.fetchall()
  out : Dict[TableId, Set[TableId]] = {}
  for r in results:
    out[r.root_id] = r.geom_ids
  return out

# TODO: Make dataclasses out of these compound types
TreeItem = Tuple[Poly, List[Poly], List[Point]]

# TODO: Is there a problem with duplicating the root node geom in 'node'?
#       Is the 'root' node the buffered version of the 'geom' node?
#       Should probably make a note of this somewhere
def getTree(cursor : Any,
             root_id : TableId,
             points_of_interest : List[Point],
            ) -> Tuple[List[TreeItem], List[TreeItem]]:
  stmt = """
    with recursive fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s) as x, unnest(%(ys)s) as y

    ), rebuilt_ancestry as (
      select N.node_id,
             N.value,
             N.geom,
             '{}'::bigint[] as ancestry,
             P.point,
             (A.parent_node_id is null) as is_leaf,
             456 as hmm
      from root R
      join node N on R.node_id = N.node_id
      left join node_ancestry A on N.node_id = A.parent_node_id
      natural full outer join fix_point_hack P
      where R.root_id = %(root_id)s and
            ST_Contains(N.geom, P.point)
      UNION ALL
      select N.node_id,
             N.value,
             N.geom,
             array_append(RA.ancestry, RA.node_id) as ancestry,
             RA.point,
             (A2.node_id is not null) as is_leaf,
             123 as hmm

      from rebuilt_ancestry RA
      join node_ancestry A on RA.node_id = A.parent_node_id
      join node N on N.node_id = A.node_id
      left join node_ancestry A2 on N.node_id = A2.node_id
      where ST_Contains(N.geom, RA.point)

    ), agg_points as (
      select is_leaf, node_id, value, geom, ancestry, jsonb_agg(point) as points
      from rebuilt_ancestry
      group by is_leaf, node_id, value, geom, ancestry

    ) select is_leaf,
             jsonb_agg(
               jsonb_build_object('id', node_id,
                                  'queue_item',
                                    jsonb_build_array(geom, ancestry, points)
                                  )
             )
             from agg_points
             group by is_leaf
  """
  xs, ys = zip(*((p.x, p.y) for p in points_of_interest))
  args = {"root_id": root_id, "xs": list(xs), "ys": list(ys) }
  cursor.execute(stmt, args)
  results = cursor.fetchall()
  leaves = a = results[0]
  branches = b = results[1]
  if b["is_leaf"]:
    leaves = b
    branches = a
  return leaves, branches



