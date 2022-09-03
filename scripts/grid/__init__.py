from typing import Any, List, Tuple, Dict, Callable, Set

import asyncio
from asyncio import Task
from collections import deque
from enum import IntEnum
from time import time
from datetime import datetime

from shapely.geometry import Polygon as Poly, Point # type: ignore

from .valuer import Valuer, Value
from .spatial_utils import split

class StopState(IntEnum):
  NO_POINTS = -1

async def main_loop(root : Poly,
                    points_of_interest : List[Point],
                    do_stop_fn : Callable[[Poly, Value, List[Poly], Valuer, List[Point]], float],
                    keep_unused : bool,
                    v : Valuer,
                    seed_polys : List[Tuple[Poly, List[Poly], List[Point]]],
                   ) -> Tuple[List[Tuple[float, Poly, Poly]], List[Tuple[float, Poly, Poly]]]:
  branches : List[Poly] = []
  leaves : List[Poly] = []

  tasks : Dict[Task[Tuple[Value, List[Poly], List[Point]]], Poly] = {}

  q : deque[Tuple[Poly, List[Poly], List[Point]]] = deque(seed_polys)

  counter = 0
  pending : Set[Task[Tuple[Value, List[Poly], List[Point]]]] = set()
  while len(q) or len(pending):
    start = int(time())
    while len(q) and int(time()) - start < 5:
      parent, parent_ancestry, parent_points = q.pop()
      (my_ancestry := parent_ancestry.copy()).append(parent)
      parts = split(parent)
      tasks.update({asyncio.create_task(v.get_value(p, my_ancestry, parent_points)) : p for p in parts})
    completed, pending = await asyncio.wait(tasks, timeout=1)

    for c in completed:
      p = tasks[c]
      value, ancestry, my_points = c.result()
      del tasks[c]
      stop = do_stop_fn(p, value, ancestry, v, my_points)
      try:
        parent = ancestry[-1]
      except IndexError:
        parent = None
      if stop:
        if keep_unused or stop is not StopState.NO_POINTS:
          leaves.append((value, p, parent))
      else:
        branches.append((value, p, parent))
        q.appendleft((p, ancestry, my_points))
    counter += 1

  return branches, leaves


