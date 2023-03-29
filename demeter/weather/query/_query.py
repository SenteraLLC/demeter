from typing import Any

from pandas import DataFrame

# from demeter.weather.initialize.weather_types import DAILY_WEATHER_TYPES


# def get_all_weather_types(cursor: Any) -> DataFrame:
#     """Get all rows from "weather_type" table and return as DataFrame."""

#     stmt = """select * from weather_type"""
#     cursor.execute(stmt)
#     df_result = DataFrame(cursor.fetchall())

#     return df_result


def get_daily_weather_types(cursor: Any) -> DataFrame:
    stmt = """
    SELECT * FROM weather.weather_type
    WHERE temporal_extent = '1 day'
    ORDER BY weather_type_id
    """
    cursor.execute(stmt)
    return DataFrame(cursor.fetchall())
