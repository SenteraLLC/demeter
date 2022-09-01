from typing import Dict, NewType, List, Tuple

import asyncio
from asyncio import Queue
from collections import OrderedDict
from datetime import datetime
from time import time

from shapely.geometry import Polygon as Poly, Point # type: ignore

from .spatial_utils import getKey, getContainedBy, yieldSplitBuffer, getCentroid, pointsToKey
from .meteo import req


TIMEOUT = 2400
MAX_LOCATIONS = 200

Value = NewType('Value', float)

class Valuer:
  def __init__(self, time : datetime, stat : str) -> None:
    self.q : Queue[Poly] = Queue()
    self.results : Dict[str, Value] = {}
    self.time = time
    self.stat = stat

  async def _get_value(self, key : str) -> Value:
    start = int(time())
    while True:
      try:
        t = int(time()) - start
        if t > TIMEOUT:
          raise Exception(f"TIMEOUT for {key} Q SIZE IS: {self.q.qsize()}.")
        r = self.results[key]
        return r
      except KeyError:
        pass
      await asyncio.sleep(1)


  async def get_value(self,
                      p : Poly,
                      my_ancestry : List[Poly] = [],
                      parent_points : List[Point] = [],
                     ) -> Tuple[Value, List[Poly], List[Point]]:
    s = getKey(p)
    if s not in self.results:
      self.q.put_nowait(p)
    my_points, _others = getContainedBy(p, parent_points)
    return await self._get_value(s), my_ancestry, my_points


  def get_value_nowait(self, p : Poly) -> Value:
    k = getKey(p)
    try:
      return self.results[k]
    except KeyError:
      pass
    raise Exception(f"Value not found for {p} : {k}")


  async def request_loop(self) -> None:
    req_count = 0
    while True:
      out : OrderedDict[str, Poly] = OrderedDict()
      while len(out) <= MAX_LOCATIONS:
        try:
          p = self.q.get_nowait()
          out[getKey(p)] = p
        except asyncio.QueueEmpty:
          break

      for k, o in out.copy().items():
        remaining_slots = MAX_LOCATIONS - len(out)
        if remaining_slots <= 0:
          break
        buffer = next(yieldSplitBuffer(o, remaining_slots))

        if len(buffer):
          out.update((getKey(p), p) for p in buffer)
      if len(out):
        times = [self.time]
        points = [ getCentroid(x) for x in out.values() ]
        r = req([self.time], {self.stat}, points)

        req_count += 1
        if req_count % 10 == 0:
          print("REQUEST COUNT: ",req_count)
        for d in r["data"]:
          cs = d["coordinates"]
          for c in cs:
            lat = c["lat"]
            long = c["lon"]
            key = pointsToKey(lat, long)
            ds = c["dates"]
            self.results[key] = ds[0]["value"]

      await asyncio.sleep(1)


