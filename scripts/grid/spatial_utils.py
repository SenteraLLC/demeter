from typing import Any, Dict, Iterator, List, NewType, Tuple

from shapely.geometry import Point
from shapely.geometry import Polygon as Poly  # type: ignore


def frange(a: float, b: float, s: float) -> Iterator[Tuple[float, float]]:
    out = a
    while (a < b and out < b) or (a >= b and out >= b):
        next_out = out + s
        yield out, (out := next_out)


def split(p: Poly) -> Iterator[Poly]:
    x1, y1, x2, y2 = p.bounds

    xdiff = (x2 - x1) / 3
    ydiff = (y2 - y1) / 3
    for a1, a2 in frange(x1, x2, xdiff):
        for b1, b2 in frange(y1, y2, ydiff):
            yield Poly(((a1, b1), (a2, b1), (a2, b2), (a1, b2)))


def _yieldSplitBuffer(p: Poly) -> Iterator[List[Poly]]:
    children = list(split(p))

    # Breadth first
    if len(children):
        yield children

    for c in children:
        yield from _yieldSplitBuffer(c)


def yieldSplitBuffer(p: Poly, buffer_size: int = 999) -> Iterator[List[Poly]]:
    out: List[Poly] = []
    for ps in _yieldSplitBuffer(p):
        remaining_slots = buffer_size - len(out)
        if remaining_slots <= 0:
            break
        out.extend(ps[:remaining_slots])
    yield out


def getCentroid(p: Poly) -> Tuple[float, float]:
    x1, y1, x2, y2 = p.bounds
    cx, cy = centroid = ((x1 + x2) / 2, (y1 + y2) / 2)
    return cx, cy


def pointsToKey(x: float, y: float) -> str:
    return "{:.5f},{:.5f}".format(x, y)


def getKey(p: Poly) -> str:
    cx, cy = getCentroid(p)
    return pointsToKey(cx, cy)


def getNodeKey(p: Poly, level: int) -> str:
    cx, cy = getCentroid(p)
    return f"[{level}]" + pointsToKey(cx, cy)


# def getKeyFromGeoJson(g : Dict[str, Any]) -> str:
#  p = Poly(g["coordinates"][0])
#  return getKey(p)


def getContainedBy(p: Poly, points: List[Point]) -> Tuple[List[Point], List[Point]]:
    yes: List[Point] = []
    no: List[Point] = []
    for t in points:
        if p.contains(t):
            yes.append(t)
        else:
            no.append(t)
    return yes, no
