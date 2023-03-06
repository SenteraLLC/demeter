import warnings
from datetime import datetime, timedelta
from typing import (
    Any,
    List,
    Union,
)

from pandas import DataFrame, to_datetime
from psycopg2.sql import Identifier
from pytz import UTC
from shapely.errors import ShapelyDeprecationWarning

from demeter.db._postgres.tools import doPgFormat

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def get_min_current_date_for_world_utm() -> datetime:
    """Get the earliest date (according to the UTM offset approach) that at least one
    UTM zone is currently in."""
    now = datetime.now(tz=UTC)
    yesterday_eod = datetime(now.year, now.month, now.day, tzinfo=UTC) + timedelta(
        hours=12
    )

    # we have passed the EOD for yesterday across all UTM zones, then return today
    if now > yesterday_eod:
        return datetime(now.year, now.month, now.day)
    else:
        return datetime(now.year, now.month, now.day) - timedelta(days=1)


def get_first_unstable_date(local_date_last_requested: datetime) -> datetime:
    first_unstable_datetime = local_date_last_requested - timedelta(days=1)
    return datetime(
        first_unstable_datetime.year,
        first_unstable_datetime.month,
        first_unstable_datetime.day,
    )


def get_cell_ids_in_weather_table(cursor: Any, table: str = "daily"):
    """Get list of all cell IDs that have data in a given data in the weather schema."""

    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    template = "select distinct cell_id from {}"
    stmt = doPgFormat(template, Identifier(table))
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    assert len(df_result) > 0, f"No data exists in table {table}"

    return df_result["cell_id"].to_list()


def get_first_data_year_for_cell_id(
    cursor_weather: Any,
    cell_id: Union[int, List[int]],
    table: str = "daily",
) -> Union[DataFrame, None]:
    """Get first date of data available for `cell_id` in weather.`table`

    If no data is available, None is returned.

    Args:
        cursor (Any): Connection to Demeter weather schema

        cell_id (int or list or int): Cell ID[s] for which to determine first date of
            available data in the database

        table (str): Name of data table to search; defaults to "daily".

    Returns dataframe with two columns: "cell_id" and "first_year"
    """
    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    cell_id = tuple(cell_id)

    template = """
    select cell_id, MIN(date) as first_date
    from {0}
    where cell_id in %(cell_id)s
    group by cell_id;
    """
    stmt = doPgFormat(template, Identifier(table))
    args = {"cell_id": cell_id}

    cursor_weather.execute(stmt, args)
    df_result = DataFrame(cursor_weather.fetchall())

    if len(df_result) == 0:
        return None
    else:
        df_result["first_year"] = to_datetime(df_result["first_date"]).dt.year
        return df_result[["cell_id", "first_year"]]


def get_date_last_requested_for_cell_id(
    cursor_weather: Any,
    table: str = "daily",
) -> Union[DataFrame, None]:
    """Get last request datetime (in UTC) for all cell IDs in weather.`table`

    If no data is available, None is returned.

    Args:
        cursor (Any): Connection to Demeter weather schema

        table (str): Name of data table to search; defaults to "daily".

    Returns dataframe with three columns: "world_utm_id", "cell_id" and "date_last_requested"
    """
    assert table in [
        "daily"
    ], "Only the following table names are currently implemented: 'daily'"

    template = """
    select world_utm_id, cell_id, MAX(date_requested) as date_last_requested
    from {0}
    group by world_utm_id, cell_id;
    """
    stmt = doPgFormat(template, Identifier(table))
    cursor_weather.execute(stmt)
    df_result = DataFrame(cursor_weather.fetchall())

    if len(df_result) == 0:
        return None
    else:
        return df_result
