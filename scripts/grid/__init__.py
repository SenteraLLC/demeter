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
                    datetime : datetime,
                    stat : str,
                   ) -> Tuple[List[Tuple[float, Poly, Poly]], List[Tuple[float, Poly, Poly]]]:
  v = Valuer(datetime, stat)
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


