"""Helper functions for running inventory on Demeter Weather in terms of what is available and what is needed.

Helps with "update" and "add" step in weather extraction process.
"""

import warnings
from datetime import datetime, timedelta
from typing import (
    Any,
    List,
    Union,
)

from pandas import DataFrame, to_datetime
from psycopg2.sql import Identifier
from shapely.errors import ShapelyDeprecationWarning

from demeter.db._postgres.tools import doPgFormat

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def get_first_unstable_date_at_request_time(
    local_date_last_requested: datetime,
) -> datetime:
    """At a given localized request time, determine the first "unstable" date that would need to be re-extracted.

    Following guidance from Meteomatics rep, the Meteomatics model predictions become "stable" after 24 hours.
    Therefore, to ensure we have the stable version of each daily weather parameter, we must ensure that all values
    are extracted at least 24 hours after the local end-of-day for the date.
    """
    first_unstable_datetime = local_date_last_requested - timedelta(days=1)
    return datetime(
        first_unstable_datetime.year,
        first_unstable_datetime.month,
        first_unstable_datetime.day,
    )


def get_populated_cell_ids(cursor: Any, table: str = "daily") -> List:
    """Get list of all cell IDs that have data in a given table in the weather schema.

    If no data exists in the database, `None` is returned.

    Args:
        cursor: Connection with access to the weather schema.

        table (str): Name of weather table to consider; defaults to "daily" which is currently
            the only weather data table in use.
    """

    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    template = "select distinct cell_id from {}"
    stmt = doPgFormat(template, Identifier(table))
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    if len(df_result) == 0:
        return None

    return df_result["cell_id"].to_list()


def get_first_available_data_year_for_cell_id(
    cursor_weather: Any,
    cell_id_list: Union[int, List[int]],
    table: str = "daily",
) -> Union[DataFrame, None]:
    """Gets the first year where data are available for `cell_id_list` in weather.`table`

    If no data is available, None is returned.

    Due to the data-greedy approach that we use to extract weather data for a given cell ID,
    we use this more coarse inventory function to determine the first year of available data.
    Based on our approach, we can assume that, if data exists for at least one day of a year
    for a cell ID, then that cell ID has data for the entire year.

    Args:
        cursor (Any): Connection to Demeter weather schema

        cell_id_list (int or list or int): Cell ID[s] for which to determine first date of
            available data in the database

        table (str): Name of data table to search; defaults to "daily".

    Returns dataframe with two columns: "cell_id" and "first_year"
    """
    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    template = """
    select cell_id, MIN(date) as first_date
    from {0}
    where cell_id in %(cell_id)s
    group by cell_id;
    """
    stmt = doPgFormat(template, Identifier(table))
    args = {"cell_id": tuple(cell_id_list)}
    cursor_weather.execute(stmt, args)
    df_result = DataFrame(cursor_weather.fetchall())

    if len(df_result) == 0:
        first_year_dtypes = {"cell_id": int, "first_year": int}
        return DataFrame([], columns=first_year_dtypes.keys()).astype(first_year_dtypes)
    else:
        df_result["first_year"] = to_datetime(df_result["first_date"]).dt.year
        return df_result[["cell_id", "first_year"]]


def get_date_last_requested_for_cell_id(
    cursor: Any,
    table: str = "daily",
) -> Union[DataFrame, None]:
    """Get last request datetime (in UTC) for all cell IDs in weather.`table`

    To ensure we are including all "unstable" data, we should determine the most
    recent `date_requested` for all parameters and then take the minimum across all
    parameters.

    If no data is available, None is returned. "world_utm_id" is included in this
    query because it makes it easier to trace `cell_id` to a specific UTM polygon.

    Args:
        cursor (Any): Connection to Demeter weather schema
        table (str): Name of data table to search; defaults to "daily".

    Returns dataframe with three columns: "world_utm_id", "cell_id" and "date_last_requested"
    """
    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    template = """
    select q.world_utm_id, q.cell_id, MIN(q.date_last_requested) as date_last_requested
    from (
        select world_utm_id, cell_id, weather_type_id, MAX(date_requested) as date_last_requested
        from {0}
        group by world_utm_id, cell_id, weather_type_id
    ) as q
    group by q.world_utm_id, q.cell_id
    """

    stmt = doPgFormat(template, Identifier(table))
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    if len(df_result) == 0:
        return None
    else:
        return df_result


def get_daily_weather_type_for_cell_id(
    cursor: Any, cell_id_list: Union[int, List[int]], weather_type_id: int
) -> DataFrame:
    """Get all rows from "daily" table for list of cell IDs and a weather type ID.

    Args:
        cursor: Connection to demeter weather schema
        cell_id_list (int or list of int): List of cell ID[s] for which to get weather data
        weather_type_id (int): ID of weather type for which to query data.

    Returns the resulting "daily" table rows as a dataframe
    """
    if not isinstance(list, cell_id_list):
        cell_id_list = [cell_id_list]

    tuple_cell_id_list = tuple(cell_id_list)

    stmt = """
    select * from daily
    where cell_id in %(cell_id)s
    and weather_type_id = %(weather_type_id)s
    """
    args = {"cell_id": tuple_cell_id_list, "weather_type_id": weather_type_id}
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    return df_result


def get_all_weather_types(cursor: Any) -> DataFrame:
    """Get all rows from "weather_type" table and return as DataFrame."""

    stmt = """select * from weather_type"""
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    return df_result
