from typing import Dict, Any, Set, List, Tuple

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


def findRootsByPoint(cursor : Any,
                     points : List[Point],
                    ) -> Dict[TableId, List[Point]]:
  # TODO: Postgres probably has native support for Point so the unnesting is pointless
  # TODO: Yep, upgrade to psycopg3
  # TODO: Fix hard coded 4326
  stmt = """
    with fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s) as x, unnest(%(ys)s) as y
    ) select R.root_id, jsonb_agg(P.point) as points
      from root R, geom G, fix_point_hack P
      where R.geom_id = G.geom_id and
            ST_Contains(G.geom, P.point)
      group by R.root_id
  """
  xs, ys = zip(*((p.x, p.y) for p in points))
  cursor.execute(stmt, {"xs": list(xs), "ys": list(ys)})
  results = cursor.fetchall()
  out : Dict[TableId, List[Point]] = {}
  for r in results:
    out[r.root_id] = [Point(p["coordinates"]) for p in r.points]
  return out


def findRootsByGeom(cursor : Any,
                    geom_ids : Set[TableId],
                   ) -> Dict[TableId, Set[TableId]]:
  stmt = """
    select R.root_id, array_agg(gid) as geom_ids
    from root R, geom RG, geom G, unnest(%(geom_ids)s) as gid
    where R.geom_id = RG.geom_id and
          gid = G.geom_id and
          ST_Contains(RG.geom, G.geom)
    group by R.root_id
  """
  cursor.execute(stmt, {"geom_ids": geom_ids})
  results = cursor.fetchall()
  out : Dict[TableId, Set[TableId]] = {}
  for r in results:
    out[r.root_id] = r.geom_ids
  return out

# TODO: Is there a problem with duplicating the root node geom in 'node'?
#       Is the 'root' node the buffered version of the 'geom' node?
#       Should probably make a note of this somewhere
def getLeaves(cursor : Any,
              root_id : TableId,
              points_of_interest : List[Point],
             ) -> List[Tuple[Poly, List[Poly], List[Point]]]:
  stmt = """
    with fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s) as x, unnest(%(ys)s) as y
    ), recursive rebuilt_ancestry (
      select N.node_id, N.geom, '{}'::Polygon[] as ancestry, jsonb_agg(point) as points
      from root R, node N
      where R.root_id = %(root_id)s and
            R.node_id = N.node_id
      UNION ALL
      select N.node_id, N.geom, array_append(RA.ancestry, RA.geom) as ancestry, jsonb_agg(P.point) as points
      from rebuilt_ancestry RA
      join node_ancestry A on RA.node_id = A.parent_node_id
      join node N on N.node_id = A.node_id

    ) select RA.geom, RA.ancestry, RA.points
      from rebuilt_ancestry RA, A.parent_node_id is null as is_parent
      left join ancestry A on A.root_id = %(root_id)s and RA.node_id = A.parent_node_id
      where not is_parent
  """
  args = {"root_id": root_id}
  cursor.execute(stmt, args)
  results = cursor.fetchall()
  out : List[Tuple[Poly, List[Poly], List[Point]]] = []
  for r in results:
    out.append((r.geom, r.ancestry, r.points))

  return out











