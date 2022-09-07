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
                    do_stop_fn : Callable[[Poly, Value, List[Poly], Valuer, List[Point]], float],
                    keep_unused : bool,
                    v : Valuer,
                    seed_polys : List[Tuple[Poly, List[Poly], List[Point]]],
                   ) -> Tuple[List[Tuple[float, int, Poly, Poly]], List[Tuple[float, int, Poly, Poly]]]:
  branches : List[Tuple[float, int, Poly, Poly]] = []
  leaves : List[Tuple[float, int, Poly, Poly]] = []

  tasks : Dict[Task[Tuple[Value, bool, List[Poly], List[Point]]], Poly] = {}

  q : deque[Tuple[Poly, List[Poly], List[Point]]] = deque(seed_polys)

  counter = 0
  pending : Set[Task[Tuple[Value, bool, List[Poly], List[Point]]]] = set()
  while len(q) or len(pending):
    start = int(time())
    while len(q) and int(time()) - start < 5:
      parent, parent_ancestry, parent_points = q.pop()
      (my_ancestry := parent_ancestry.copy()).append(parent)
      parts = list(split(parent))
      tasks.update({asyncio.create_task(v.get_value(p, my_ancestry, parent_points)) : p for p in parts})
    completed, pending = await asyncio.wait(tasks, timeout=1)

    for c in completed:
      p = tasks[c]
      value, is_new, ancestry, my_points = c.result()
      del tasks[c]
      stop = do_stop_fn(p, value, ancestry, v, my_points)

      parent = None
      try:
        parent = ancestry[-1]
      except IndexError:
        pass
      level = len(ancestry)
      if stop:
        do_save = keep_unused or stop is not StopState.NO_POINTS
        if do_save and is_new:
          leaves.append((value, level, p, parent))
      else:
        if is_new:
          branches.append((value, level, p, parent))
        q.appendleft((p, ancestry, my_points))

      counter += 1

  return branches, leaves


