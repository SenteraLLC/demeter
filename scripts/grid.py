from typing import List, Iterator, Tuple, Awaitable, NewType

from dataclasses import dataclass, field
from dataclasses import InitVar

from demeter.db import Table, TableId, Detailed
from demeter.data import MultiPolygon, Polygon, Geom


@dataclass(frozen=True)
class Node(Table):
  polygon : InitVar[Polygon]
  value   : float
  geom    : str = field(init=False)

  def __post_init__(self, polygon : Polygon) -> None:
    g = Geom(
      crs_name = "urn:ogc:def:crs:EPSG::4326",
      type = "MultiPolygon",
      coordinates = (polygon, ),
      container_geom_id = None,
    )
    geom = g.geom


@dataclass(frozen=True)
class NodeRaster(Table):
  node_id : TableId
  # TODO: What Python type for raster?
  raster : str


@dataclass(frozen=True)
class Root(Detailed):
  geom_id : TableId
  local_type_id : TableId


@dataclass(frozen=True)
class Ancestry(Table):
  root_id : TableId
  parent_node_id : TableId
  node_id : TableId



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
TIMEOUT = 1200
from time import time

from collections import OrderedDict

from functools import lru_cache

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

class Valuer:
  def __init__(self) -> None:
    self.q : Queue[Poly] = Queue()
    self.results : Dict[str, Value] = {}

  #@lru_cache(maxsize=2**10)
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

  async def get_value(self, p : Poly) -> Value:
    s = key(p)
    if s not in self.results:
      self.q.put_nowait(p)
    #else:
      #print("ALREADY IN RESULTS: ",s)
    return await self._get_value(s)

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
        print("Remaining slots: ",remaining_slots)
        buffer = next(yieldSplitBuffer(o, remaining_slots))
        print("  Add buffer: ",len(buffer))

        if len(buffer):
          out.update((key(p), p) for p in buffer)

      if len(out):
        dates = [datetime.now()]
        stats = ["t_2m:C"]
        points = [ getCentroid(x) for x in out.values() ]
        #print("POINTS: ",points)
        r = req(dates, stats, points)
        req_count += 1
        #if req_count > 100:
        #  import sys
        #  sys.exit(1)
        if req_count % 10 == 0:
          print("\nREQ COUNT: ",req_count)
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
from shapely.geometry import Point

_test_points = [
  Point(45.3325, -93.742),
  Point(45.3055, -93.7941),
  Point(46.7867, -92.1005),
]

def do_stop(p : Poly,
            v : Value,
            parent_value : Value,
            grandparent_value : Value,
           ) -> bool:
  try:
    pv = parent_value
    gpv = grandparent_value
    # TODO: Better heuristic
    #does_contain = False
    #for z in _test_points:
    #  if p.contains(z):
    #    does_contain = True
    #    break
    #if not does_contain:
    #  return True

    total_diff = abs(pv - v) + abs(gpv - v) + abs(pv - gpv)
    return total_diff < 10
    #return total_diff < 0.1
  except IndexError:
    pass
  return False


from typing import Set
import asyncio
import sys

async def run2(root : Poly) -> None:
  v = Valuer()
  leaves : List[Poly] = []

  running = asyncio.create_task(v.run())
  tasks : Dict[Task[Value], Poly] = {}

  q : deque[Tuple[Poly, Optional[Poly]]] = deque(((root, None), ))
  _initial_value = await v.get_value(root)
  counter = 0
  while len(q):

    start = int(time())
    while len(q) and int(time()) - start < 5:
      parent, grandparent = q.pop()
      parts = split(parent)
      tasks.update({asyncio.create_task(v.get_value(p)) : p for p in parts})
    completed, pending = await asyncio.wait(tasks, timeout=1)

    #print("\nLEAVE COUNT: ",len(leaves))
    #print("Completed: ",len(completed))
    #print("PENDING: ",len(pending))
    #print("Q SIZE: ",len(q))
    #sys.stdout.flush()

    for c in completed:
      p = tasks[c]
      value = c.result()
      del tasks[c]

      parent_value = parent and v.get_value_nowait(parent)
      grandparent_value = grandparent and v.get_value_nowait(grandparent)
      if parent_value is None or \
         grandparent_value is None or \
         not do_stop(p, value, parent_value, grandparent_value):
        q.appendleft((p, parent))
      else:
        #print("NEW LEAF: ",p)
        leaves.append(p)
    counter += 1


  print("Done.")
  #result = await running
  #print("Result is: ",result)

  #for i, l in enumerate(leaves):
  #  print(l, "\n ",l.area)


if __name__ == '__main__':
  start = Poly(((50, -100), (40, -100), (40, -90), (50, -90)))
  asyncio.run(run2(start))






