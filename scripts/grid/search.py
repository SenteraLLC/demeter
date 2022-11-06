from typing import TypedDict

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from shapely import wkb  # type: ignore
from shapely.geometry import Point  # type: ignore
from shapely.geometry import Polygon as Poly

from demeter.db import TableId

from .spatial_utils import getNodeKey


def getNodeIdLookup(
    cursor: Any,
    root_id: TableId,
) -> Dict[str, TableId]:
    stmt = """
    with recursive node_and_geom as (
      select N.node_id, N.geom, 0 as level
      from root R, node N
      where R.root_id = %(root_id)s and
            R.root_node_id= N.node_id
      UNION ALL
      select N.node_id, N.geom, P.level + 1
      from node_and_geom P
      join node_ancestry A on P.node_id = A.parent_node_id
      join node N on N.node_id = A.node_id
    ) select * from node_and_geom;
  """
    cursor.execute(stmt, {"root_id": root_id})
    results = cursor.fetchall()
    out: Dict[str, TableId] = {}
    for r in results:
        g = wkb.loads(r.geom, hex=True)
        k = getNodeKey(g, r.level)
        out[k] = r.node_id
    return out


def findRootByPoint(
    cursor: Any,
    points: List[Point],
    observation_type_id: Optional[TableId],
) -> Dict[TableId, List[Point]]:
    # TODO: Postgres 3 has native support for Point so we will be able to remove this hack
    stmt = """
    with fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s, %(ys)s) as u(x, y)
    ) select R.root_id, jsonb_agg(P.point) as points
      from root R, node N, fix_point_hack P, observation_type T
      where T.observation_type_id = %(observation_type_id)s and
            R.observation_type_id = T.observation_type_id and
            R.root_node_id = N.node_id and
            ST_Contains(N.geom, P.point)
      group by R.root_id
  """
    xs, ys = zip(*((p.x, p.y) for p in points))
    cursor.execute(
        stmt, {"xs": list(xs), "ys": list(ys), "observation_type_id": observation_type_id}
    )
    results = cursor.fetchall()
    out: Dict[TableId, List[Point]] = {}
    # TODO: This aggregation can happen in postgres
    #       See 'getTree'
    for r in results:
        out[r.root_id] = [Point(p["coordinates"]) for p in r.points]
    return out


def findRootByGeom(
    cursor: Any,
    geom_ids: Set[TableId],
) -> Dict[TableId, Set[TableId]]:
    stmt = """
    select R.root_id, array_agg(gid) as geom_ids
    from root R, node N, geom G, unnest(%(geom_ids)s) as gid
    where gid = G.geom_id and
          R.root_node_id = N.geom_id and
          ST_Contains(N.geom, G.geom)
    group by R.root_id
  """
    cursor.execute(stmt, {"geom_ids": geom_ids})
    results = cursor.fetchall()
    out: Dict[TableId, Set[TableId]] = {}
    for r in results:
        out[r.root_id] = r.geom_ids
    return out




class NodeMeta(TypedDict):
    level: int
    bounds: Poly
    value: float
    ancestry: List[TableId]
    points: List[Point]
    is_bud: bool


from collections import ChainMap
from collections.abc import ItemsView
from dataclasses import dataclass
from typing import Iterator


@dataclass
class Tree:
    leaves: Dict[TableId, NodeMeta]
    branches: Dict[TableId, NodeMeta]

    # def __iter__(self) -> ItemsView[TableId, NodeMeta]:
    #  return ChainMap(self.leaves, self.branches).items()

    def __iter__(self) -> Iterator[Tuple[TableId, NodeMeta]]:
        return iter(ChainMap(self.leaves, self.branches).items())


def getTree(
    cursor: Any,
    root_id: TableId,
    points_of_interest: List[Point],
    start_time: datetime,
    time_delta: timedelta = timedelta(seconds=1),
) -> Tree:
    stmt = """
    with recursive fix_point_hack as (
      select ST_SetSRID(ST_MakePoint(x, y), 4326) as point
      from unnest(%(xs)s, %(ys)s) as u(x, y)

    ), rebuilt_ancestry as (
      select N.node_id,
             -12345::bigint as parent_node_id,
             N.value,
             N.geom,
             '{}'::bigint[] as ancestry,
             P.point,
             0 as level
      from test_mlops.root R
      join test_mlops.node N on R.root_node_id = N.node_id
      natural full outer join fix_point_hack P
      where R.root_id = %(root_id)s and
            ST_Contains(N.geom, P.point)

      UNION ALL

      select N.node_id,
             A.parent_node_id,
             N.value,
             N.geom,
             array_append(RA.ancestry, RA.node_id) as ancestry,
             RA.point,
             (RA.level + 1) as level
      from rebuilt_ancestry RA
      join test_mlops.node_ancestry A on RA.node_id = A.parent_node_id
      join test_mlops.node N on N.node_id = A.node_id
      where ST_Contains(N.geom, RA.point)

    ), node_id_to_meta as (
      select not exists (
               select * from rebuilt_ancestry A2
                      where A1.node_id = A2.parent_node_id
             ) as is_leaf,
             node_id,
             json_build_object(
               'level', level,
               'bounds', geom::text,
               'value', value,
               'ancestry', ancestry,
               'points', jsonb_agg(point::text),
               'is_bud', exists (
                           select * from node_ancestry A
                           where A1.node_id = A.parent_node_id
                         )
             ) as meta
             from rebuilt_ancestry A1
             group by is_leaf, node_id, level, geom, value, ancestry, point
    ) select is_leaf,
             jsonb_object_agg(node_id, meta) as node_meta
             from node_id_to_meta
             group by is_leaf;
  """
    xs, ys = zip(*((p.x, p.y) for p in points_of_interest))
    args = {"root_id": root_id, "xs": list(xs), "ys": list(ys)}
    cursor.execute(stmt, args)
    results = cursor.fetchall()
    r0 = results[0]
    l = r0.node_meta
    try:
        b = results[1].node_meta
    except IndexError:
        b = {}
    return Tree(l, b) if r0.is_leaf else Tree(b, l)


def debug() -> None:
    stmt = """
  select A.geom::Polygon,
                   B.geom::Polygon,
                   ST_Contains(A.geom, B.geom),
                   ST_Covers(A.geom, B.geom),
                   ST_Overlaps(A.geom, B.geom),
                   ST_Area(ST_Intersection(A.geom, B.geom)) / ST_Area(B.geom)
            from test_mlops.node A, test_mlops.node B
            where A.node_id=36395 and B.node_id=36397;
  """

    """
  select A.geom::Polygon as parent,
         B.geom::Polygon as child,

         A.geom::Polygon as parent_geom,
         B.geom::Polygon as child_geom,

  """

    """
  select B.node_id as child,
         B.value as child_value,
         A.node_id as parent,
         A.value as parent_value,
         ST_Contains(A.geom, B.geom),
         ST_Covers(A.geom, B.geom),
         ST_Overlaps(A.geom, B.geom),
         ST_Area(A.geom) as parent_area,
         ST_Area(B.geom) as child_area,
         ST_Area(ST_Intersection(A.geom, B.geom)) / ST_Area(B.geom) as child_percent,
         ST_Area(ST_Intersection(A.geom, B.geom)) / ST_Area(A.geom) as parent_percent
            from test_mlops.node A, test_mlops.node B, test_mlops.node_ancestry N
            where A.node_id=N.parent_node_id and B.node_id=N.node_id
            order by B.node_id asc, A.node_id asc;
  """

    return None
