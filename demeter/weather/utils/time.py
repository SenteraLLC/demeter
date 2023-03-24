"""Public helper functions for handling time in weather grid processing."""

from datetime import (
    datetime,
    timedelta,
    timezone,
)

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


def get_min_current_date_for_world_utm() -> datetime:
    """Get the earliest curent date (according to the UTM offset approach) across all zones."""
    now = datetime.now(tz=UTC)
    yesterday_eod = datetime(now.year, now.month, now.day, tzinfo=UTC) + timedelta(
        hours=12
    )

    # we have passed the EOD for yesterday across all UTM zones, then return today
    if now > yesterday_eod:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day) - timedelta(days=1)
