from typing import List, Iterator, Tuple, NewType


from shapely.geometry import Polygon as Poly # type: ignore

def frange(a : float, b : float , s : float) -> Iterator[Tuple[float, float]]:
  out = a
  while ((a < b and out < b) or
         (a >= b and out >= b)):
    next_out = out + s
    yield out, (out := next_out)

def split(p : Poly) -> Iterator[Poly]:
  x1, y1, x2, y2 = p.bounds
  xdiff = (x2-x1)/3
  ydiff = (y2-y1)/3
  for a1, a2 in frange(x1, x2, xdiff):
    for b1, b2 in frange(y1, y2, ydiff):
      yield Poly(((a1, b1), (a2, b1), (a2, b2), (a1, b2)))

from collections import OrderedDict

def _yieldSplitBuffer(p : Poly) -> Iterator[List[Poly]]:
  children = list(split(p))
  if len(children):
    yield children

  for c in children:
    yield from _yieldSplitBuffer(c)


def yieldSplitBuffer(p : Poly,
                     buffer_size : int = 999
                    ) -> Iterator[List[Poly]]:
  out : List[Poly] = []
  for ps in _yieldSplitBuffer(p):
    remaining_slots = buffer_size - len(out)
    if remaining_slots <= 0:
      break
    out.extend(ps[:remaining_slots])
  yield out

from typing import Dict
import asyncio
from asyncio import Queue, Task

Value = NewType('Value', float)

MAX_LOCATIONS = 200
TIMEOUT = 2400
from time import time

from collections import OrderedDict

from .meteo import req

def getCentroid(p : Poly) -> Tuple[float, float]:
  x1, y1, x2, y2 = p.bounds
  cx, cy = centroid = ((x1+x2)/2, (y1+y2)/2)
  return cx, cy

def points_to_string(x : float, y : float) -> str:
  return "{:.5f},{:.5f}".format(x, y)

def key(p : Poly) -> str:
  cx, cy = getCentroid(p)
  return points_to_string(cx, cy)


from datetime import datetime
import json
from shapely.geometry import Point

class Valuer:
  def __init__(self) -> None:
    self.q : Queue[Poly] = Queue()
    self.results : Dict[str, Value] = {}

  async def _get_value(self, p_str : str) -> Value:
    start = int(time())
    while True:
      try:
        t = int(time()) - start
        if t > TIMEOUT:
          raise Exception(f"TIMEOUT for {p_str} Q SIZE IS: {self.q.qsize()}.")
        r = self.results[p_str]
        #del self.results[p_str]
        return r
      except KeyError:
        pass
      await asyncio.sleep(1)

  async def get_value(self,
                      p : Poly,
                      my_ancestry : List[Poly] = [],
                      parent_points : List[Point] = [],
                     ) -> Tuple[Value, List[Poly], List[Point]]:
    s = key(p)
    if s not in self.results:
      self.q.put_nowait(p)
    my_points, _others = getContainedBy(p, parent_points)
    return await self._get_value(s), my_ancestry, my_points

  def get_value_nowait(self, p : Poly) -> Value:
    s = key(p)
    try:
      return self.results[s]
    except KeyError:
      pass
    raise Exception(f"Value not found for {s} : {p}")

  async def run(self) -> None:
    req_count = 0
    while True:
      out : OrderedDict[str, Poly] = OrderedDict()
      while len(out) <= MAX_LOCATIONS:
        try:
          p = self.q.get_nowait()
          out[key(p)] = p
        except asyncio.QueueEmpty:
          break
      for k, o in out.copy().items():
        remaining_slots = MAX_LOCATIONS - len(out)
        if remaining_slots <= 0:
          break
        buffer = next(yieldSplitBuffer(o, remaining_slots))

        if len(buffer):
          out.update((key(p), p) for p in buffer)

      if len(out):
        dates = [datetime.now()]
        stats = ["t_2m:C"]
        points = [ getCentroid(x) for x in out.values() ]
        r = req(dates, stats, points)
        req_count += 1
        if req_count % 10 == 0:
          print("REQUEST COUNT: ",req_count)
        for d in r["data"]:
          cs = d["coordinates"]
          for c in cs:
            lat = c["lat"]
            long = c["lon"]
            k = points_to_string(lat, long)
            ds = c["dates"]
            self.results[k] = ds[0]["value"]

      await asyncio.sleep(1)


from typing import Optional
from collections import deque

def do_stop(p : Poly,
            v : Value,
            ancestry : List[Poly],
            valuer : Valuer,
            my_points : List[Point],
           ) -> bool:
  if len(my_points) <= 0:
    return True
  if len(ancestry) < 2:
    return False

  parent = ancestry[-1]
  grandparent = ancestry[-2]
  pv = valuer.get_value_nowait(parent)
  gpv = valuer.get_value_nowait(grandparent)
  total_diff = abs(pv - v) + abs(gpv - v) + abs(pv - gpv)
  return total_diff < 1


from typing import Set


def getContainedBy(p : Poly, points : List[Point]) -> Tuple[List[Point], List[Point]]:
  yes : List[Point] = []
  no : List[Point] = []
  for t in points:
    if p.contains(t):
      yes.append(t)
    else:
      no.append(t)
  return yes, no

async def run2(root : Poly, points_of_interest : List[Point]) -> None:
  v = Valuer()
  branches : List[Poly] = []
  leaves : List[Poly] = []

  running = asyncio.create_task(v.run())
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
      stop = do_stop(p, value, ancestry, v, my_points)
      if stop:
        leaves.append(p)
      else:
        branches.append(p)
        q.appendleft((p, ancestry, my_points))
    counter += 1

  print("\nBRANCH COUNT: ",len(branches))
  print("\nLEAVE COUNT: ",len(leaves))
  print("Done.")
  #result = await running
  #print("Result is: ",result)

  #for i, l in enumerate(leaves):
  #  print(l, "\n ",l.area)

if __name__ == '__main__':
  test_points = [
    Point(45.3325, -93.742),
    Point(45.3055, -93.7941),
    Point(46.7867, -92.1005),
  ]

  start = Poly(((50, -100), (40, -100), (40, -90), (50, -90)))
  asyncio.run(run2(start, test_points))



