from datetime import datetime, timezone

from pytz import UTC


def localize_utc_datetime_with_utc_offset(
    d: datetime, utc_offset: timezone
) -> datetime:
    return d.tz_localize(UTC).astimezone(utc_offset)


# def get_date_range(date_first: Timestamp, date_last: Timestamp):

#     list_dates = (
#         date_range(start=date_first.date(), end=date_last.date(), freq="d")
#         .to_pydatetime()
#         .tolist()
#     )
#     return list_dates
