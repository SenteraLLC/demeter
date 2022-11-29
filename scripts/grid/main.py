from typing import Any, List, Dict, Tuple

import asyncio

from datetime import datetime, timedelta

from shapely.geometry import Point  # type: ignore
from shapely.geometry import Polygon as Poly


from shapely import wkb  # type: ignore

from demeter.db import TableId
from demeter.grid import getNodePolygon, getRoot

from . import main_loop
from .example import (
    do_stop,
    getObservationType,
    getStartingGeoms,
    insertTree,
)
from .search import findRootByPoint, getTree
from .spatial_utils import getNodeKey
from .valuer import Valuer


async def main(
    cursor: Any,
    stat: str,
    date: datetime,
    points: List[Point],
    start_polygon: Poly,
    keep_unused: bool,
) -> None:
    s = stat
    local_type_id = getObservationType(cursor, s)
    existing_roots = findRootByPoint(cursor, points, local_type_id)

    d = date
    v = Valuer(d, s)
    running = asyncio.create_task(v.request_loop())

    node_id_lookup: Dict[str, TableId] = {}
    seed_polys: List[Tuple[Poly, List[Poly], List[Point]]] = []

    if not len(existing_roots):
        root_id, root_node_id = getStartingGeoms(
            cursor, points, start_polygon, keep_unused, d, s
        )
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
            print(
                "Some points did not fall in starting range: ",
                len(points) - len(root_points),
            )
        if not len(root_points):
            # TODO: Add this functionality
            # print("No valid points of interest were provided. Performing exhaustive query. This may take awhile.")
            raise Exception("No points of interest is not currently supported.")
        seed_polys.append((start_polygon, [], root_points))

    elif len(existing_roots) > 1:
        raise Exception("Too many")

    else:
        # TODO: Where to store time delta
        # import sys
        root_id, points = existing_roots.popitem()
        root = getRoot(cursor, root_id)
        root_node_id = root.root_node_id
        # poly = getNodePolygon(cursor, root_node_id)

        # TODO FIXME Fix time delta
        existing = getTree(cursor, root_id, points, d, timedelta(days=1, minutes=-1))

        # TODO: Handle key calc here or in 'toTree' sql?
        # TODO: psycopg3 should let us skip directly to shapely::Polygon

        # TODO: Could be smart about not loading every possible result
        # v.load_existing({ getKey(wkb.loads(m["bounds"], hex=True)) : Value(m["value"]) for node_id, m in existing })
        v.load_existing(m[1] for m in existing)

        for node_id, n in existing.leaves.items():
            # The leaf nodes may be looked up as parents in 'insertTree'
            k = getNodeKey(wkb.loads(n["bounds"], hex=True), n["level"])
            node_id_lookup[k] = node_id

            # We need to store metadata for the nodes in the Valuer
            # TODO: Do in SQL
            my_ancestry = [getNodePolygon(cursor, node_id) for node_id in n["ancestry"]]
            p = wkb.loads(n["bounds"], hex=True)
            points = [wkb.loads(z, hex=True) for z in n["points"]]
            if getNodeKey(p, n["level"]) not in v.existing_keys:
                seed_polys.append((p, my_ancestry, points))

    branches, leaves = await main_loop(
        start_polygon, do_stop, keep_unused, v, seed_polys
    )

    # TODO: Child geoms need to be clipped to parent, then we can add constraint
    insertTree(cursor, branches, leaves, node_id_lookup, root_id)
    running.cancel()
