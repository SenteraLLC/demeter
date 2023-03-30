from typing import Any

from pandas import DataFrame


def get_daily_weather_types(cursor: Any) -> DataFrame:
    stmt = """
    SELECT * FROM weather.weather_type
    WHERE temporal_extent = '1 day'
    ORDER BY weather_type_id
    """
    cursor.execute(stmt)
    return DataFrame(cursor.fetchall())
