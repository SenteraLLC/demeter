"""Generalized helper functions for weather extraction processes."""

from datetime import datetime, timezone

from pytz import UTC
from pytz import timezone as pytz_timezone
from shapely.geometry import Point
from timezonefinder import TimezoneFinder

tzf = TimezoneFinder()


def localize_utc_datetime_with_utc_offset(
    d: datetime, utc_offset: timezone
) -> datetime:
    return d.tz_localize(UTC).astimezone(utc_offset)


def localize_utc_datetime_with_timezonefinder(d: datetime, geom: Point) -> datetime:
    """Localizes `d` using land time zone at a certain `geom` location."""
    lat = geom.y
    lon = geom.x

    tz_str = tzf.timezone_at(lat=lat, lng=lon)
    tz = pytz_timezone(tz_str)

    d = d.replace(tzinfo=UTC)

    return d.astimezone(tz=tz)


# def get_date_range(date_first: Timestamp, date_last: Timestamp):

#     list_dates = (
#         date_range(start=date_first.date(), end=date_last.date(), freq="d")
#         .to_pydatetime()
#         .tolist()
#     )
#     return list_dates
