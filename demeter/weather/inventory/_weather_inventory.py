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

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import merge as pd_merge
from pandas import to_datetime
from psycopg2.sql import Identifier
from shapely.errors import ShapelyDeprecationWarning

from demeter.db._postgres.tools import doPgFormat
from demeter.weather.utils.grid import get_centroid, get_info_for_world_utm
from demeter.weather.utils.time import get_min_current_date_for_world_utm

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


def get_populated_cell_ids(cursor: Any, table: str = "daily") -> DataFrame:
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

    template = "select distinct cell_id, world_utm_id from {}"
    stmt = doPgFormat(template, Identifier(table))
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    if len(df_result) == 0:
        return None

    return df_result


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
    cursor: Any,
    cell_id_list: Union[int, List[int]],
    weather_type_id: Union[int, List[int]],
    keep: str = "recent",
) -> DataFrame:
    """Get all rows from "daily" table for list of cell IDs and a weather type ID.

    Args:
        cursor: Connection to demeter weather schema
        cell_id_list (int or list of int): List of cell ID[s] for which to get weather data
        weather_type_id (int or list of int): ID[s] of weather type[s] for which to query data

        keep (str): Indicates how duplicates should be handled in query for cell ID x date x
            parameter combiantions; "all" keeps all rows, "recent" keeps just the most recently
            extracted row (default)

    Returns the resulting "daily" table rows as a dataframe
    """
    assert keep in ["all", "recent"], '`keep` must be "all" or "recent"'

    if not isinstance(cell_id_list, list):
        cell_id_list = [cell_id_list]
    tuple_cell_id_list = tuple(cell_id_list)

    if not isinstance(weather_type_id, list):
        weather_type_id = [weather_type_id]
    tuple_weather_type_id = tuple(weather_type_id)

    if keep == "all":
        stmt = """
        select cell_id, date, weather_type_id, value, date_requested from daily
        where cell_id in %(cell_id)s
        and weather_type_id in %(weather_type_id)s
        """
    else:
        stmt = """
        with q1 AS (
            SELECT d.cell_id, d.date, d.weather_type_id, d.value, d.date_requested
            FROM daily AS d
            WHERE cell_id in %(cell_id)s and
            weather_type_id in %(weather_type_id)s
        ), q2 AS (
            SELECT q1.cell_id, q1.date, q1.weather_type_id, q1.value, q1.date_requested,
                ROW_NUMBER() OVER(PARTITION BY q1.cell_id, q1.weather_type_id, q1.date ORDER BY q1.date_requested desc) as rn
            FROM q1
        ) SELECT q2.cell_id, q2.date, q2.weather_type_id, q2.value, q2.date_requested FROM q2
        WHERE q2.rn = 1
        """

    args = {
        "cell_id": tuple_cell_id_list,
        "weather_type_id": tuple_weather_type_id,
    }
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    return df_result


def get_weather_grid_info_for_populated_cell_ids(cursor: Any) -> GeoDataFrame:
    """Determine the spatiotemporal bounds of needed weather data for all populated cell IDs in `weather.daily`.

    This function assumes the first date of needed data is Jan 1 of the first year where data exists
    for a given cell ID.

    Args:
        cursor: Connection with access to the weather schemas.
    """
    # get all cell IDs with data in `daily` table
    df_cell_ids = get_populated_cell_ids(cursor, table="daily")

    # get first year of data available and set "date_first"
    gdf_available = get_first_available_data_year_for_cell_id(
        cursor_weather=cursor, cell_id_list=df_cell_ids["cell_id"].to_list()
    )

    cols = [
        "utm_zone",
        "utc_offset",
        "world_utm_id",
        "cell_id",
        "centroid",
        "date_first",
        "date_last",
    ]

    if len(gdf_available) > 0:
        gdf_available["date_first"] = gdf_available["first_year"].map(
            lambda yr: datetime(yr, 1, 1)
        )

        # set "date_last" as 7 days from the last full day across all UTM zones
        today = get_min_current_date_for_world_utm()
        gdf_available["date_last"] = today + timedelta(days=7)

        # add back world UTM polygon information for each cell ID and add utm info
        gdf_available = pd_merge(gdf_available, df_cell_ids, on="cell_id")

        world_utm_list = [
            int(world_utm_id) for world_utm_id in df_cell_ids["world_utm_id"].unique()
        ]
        df_utm = get_info_for_world_utm(cursor, world_utm_list).rename(
            columns={"zone": "utm_zone"}
        )
        gdf_available = pd_merge(gdf_available, df_utm, on="world_utm_id")

        # get centroid for each cell ID
        gdf_available["centroid"] = gdf_available.apply(
            lambda row: get_centroid(cursor, row["world_utm_id"], row["cell_id"]),
            axis=1,
        )

        return gdf_available[cols]
    else:
        df = DataFrame(data=[], columns=cols)
        return GeoDataFrame(df, geometry="centroid")
