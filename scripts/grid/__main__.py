import argparse
import asyncio
import json
from copy import deepcopy
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Iterator

from dotenv import load_dotenv
from shapely.geometry import Point  # type: ignore
from shapely.geometry import Polygon as Poly

from demeter.db import getConnection

from .example import getPoints, pointsToBound
from .main import main

DATE = "%Y-%m-%d"
SHORT = DATE + "Z"
LONG = " ".join([DATE, "T%H-%M-%SZ"])
DATE_FORMATS = [SHORT, LONG]
DATE_FORMATS_STR = " || ".join([s.replace("%", "%%") for s in DATE_FORMATS])


def parseTime(s: str) -> datetime:
    for f in DATE_FORMATS:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            pass
    raise ValueError(
        f"Invalid date string '{s}' does not match format: {DATE_FORMATS_STR}"
    )


def parsePoly(s: str) -> Poly:
    coords = json.loads(s)
    return Poly(coords)


def yieldTimeRange(
    a: datetime,
    b: datetime,
    delta: timedelta,
) -> Iterator[datetime]:
    x = deepcopy(a)
    while x < b:
        yield x
        x += delta


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Store meteomatics data with dynamic spatial-resolution using nested polygons"
    )
    parser.add_argument(
        "--stats",
        action="extend",
        help="List of meteomatic stats on which to query",
        required=True,
        nargs="+",
    )

    parser.add_argument(
        "--start_time",
        type=parseTime,
        help=f"Time on which to start data collection.\n{DATE_FORMATS_STR}",
        default=datetime.now(timezone.utc),
    )
    parser.add_argument(
        "--end_time",
        type=parseTime,
        help=f"Time on which to end data collection.\n{DATE_FORMATS_STR}",
        default=datetime.now(timezone.utc),
    )
    parser.add_argument(
        "--step_delta",
        type=json.loads,
        help="Python datetime.timedelta on which to step",
        default=timedelta(days=1),
    )

    parser.add_argument(
        "--bounds",
        type=parsePoly,
        help="Polygon bounds on which to build a new root. By default, try to use an existing root.",
    )
    parser.add_argument(
        "--points",
        type=Point,
        nargs="+",
        help="A list of points on which to collect data. Temporarily, this parameter is not required. When left blank it will default to random points from the demeter DB",
    )
    # TODO: Allow a user to supply 'bounds' or 'points' alone
    #       Only bounds -> Do not consider points of interest, exhaustive
    #       Only points -> Auto-generate bounds from points if it does not exist

    parser.add_argument(
        "--keep_unused",
        action="store_true",
        help="Save leaf nodes even if they do not contain a point of interest",
        default=False,
    )

    args = parser.parse_args()
    start = args.start_time
    end = args.end_time
    delta = args.step_delta
    if start > end:
        raise Exception(f'End time "{end}" cannot occur before start date "{start}".')
    if delta.total_seconds() <= 0:
        raise Exception(f'Invalid time delta "{delta}". Must be greater than zero.')

    connection = getConnection(env_name="TEST_DEMETER")
    cursor = connection.cursor()

    # TODO: Remove the 'or getPoints' expr when there is a better means of testing
    points = args.points or getPoints(cursor)

    start_polygon = args.bounds or pointsToBound(points)

    for d in yieldTimeRange(start, end, delta):
        for s in args.stats:
            asyncio.run(main(cursor, s, d, points, start_polygon, args.keep_unused))

    connection.commit()
