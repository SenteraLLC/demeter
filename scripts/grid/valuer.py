import asyncio
from asyncio import Queue
from collections import OrderedDict
from datetime import datetime
from time import time
from typing import Dict, Iterator, List, NewType, Set, Tuple

from shapely import wkb  # type: ignore
from shapely.geometry import Point  # type: ignore
from shapely.geometry import Polygon as Poly

from .meteo import req
from .search import NodeMeta
from .spatial_utils import (
    getCentroid,
    getContainedBy,
    getKey,
    getNodeKey,
    pointsToKey,
    yieldSplitBuffer,
)

TIMEOUT = 2400
MAX_LOCATIONS = 200

Value = NewType("Value", float)


class Valuer:
    def __init__(self, time: datetime, stat: str) -> None:
        self.q: Queue[Tuple[Poly, int]] = Queue()
        self.results: Dict[str, Value] = {}
        self.existing_keys: Set[str] = set()
        self.time = time
        self.stat = stat

    async def _get_value(self, key: str) -> Value:
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

    def load_existing(
        self,
        existing_node_metas: Iterator[NodeMeta],
    ) -> bool:
        for m in existing_node_metas:
            g = wkb.loads(m["bounds"], hex=True)
            k = getKey(g)
            self.results[k] = Value(m["value"])
            if not m["is_bud"]:
                self.existing_keys.add(getNodeKey(g, m["level"]))
        return True

    async def get_value(
        self,
        p: Poly,
        my_ancestry: List[Poly] = [],
        parent_points: List[Point] = [],
    ) -> Tuple[Value, bool, List[Poly], List[Point]]:
        s = getKey(p)

        is_new = True
        if s not in self.results:
            level = len(my_ancestry)
            self.q.put_nowait((p, level))
        elif s in self.existing_keys:
            is_new = False

        my_points, _others = getContainedBy(p, parent_points)
        return await self._get_value(s), is_new, my_ancestry, my_points

    def get_value_nowait(self, p: Poly) -> Value:
        k = getKey(p)
        try:
            return self.results[k]
        except KeyError:
            pass
        raise Exception(f"Value not found for {p} : {k}")

    async def request_loop(self) -> None:
        req_count = 0
        while True:
            out: OrderedDict[str, Poly] = OrderedDict()
            while len(out) <= MAX_LOCATIONS:
                try:
                    p, level = self.q.get_nowait()
                    out[getKey(p)] = p
                except asyncio.QueueEmpty:
                    break

            # TODO: Copy by item, not entire array
            for k, o in out.copy().items():
                child_level = level + 1
                remaining_slots = MAX_LOCATIONS - len(out)
                if remaining_slots <= 0:
                    break
                buffer = next(yieldSplitBuffer(o, remaining_slots))

                if len(buffer):
                    out.update((getKey(p), p) for p in buffer)
            if len(out):
                times = [self.time]
                points = [getCentroid(x) for x in out.values()]
                r = req([self.time], {self.stat}, points)

                req_count += 1
                if req_count % 10 == 0:
                    print("REQUEST COUNT: ", req_count)
                for d in r["data"]:
                    cs = d["coordinates"]
                    for c in cs:
                        lat = c["lat"]
                        long = c["lon"]
                        key = pointsToKey(lat, long)
                        ds = c["dates"]
                        self.results[key] = ds[0]["value"]

            await asyncio.sleep(1)
