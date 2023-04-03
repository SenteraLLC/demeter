from typing import (
    Any,
    List,
    Union,
)

from pandas import DataFrame


def get_daily_weather_types(cursor: Any) -> DataFrame:
    stmt = """
    SELECT * FROM weather.weather_type
    WHERE temporal_extent = '1 day'
    ORDER BY weather_type_id
    """
    cursor.execute(stmt)
    return DataFrame(cursor.fetchall())


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
        select daily_id, cell_id, date, weather_type_id, value, date_requested from daily
        where cell_id in %(cell_id)s
        and weather_type_id in %(weather_type_id)s
        """
    else:
        stmt = """
        with q1 AS (
            SELECT d.daily_id, d.cell_id, d.date, d.weather_type_id, d.value, d.date_requested
            FROM daily AS d
            WHERE cell_id in %(cell_id)s and
            weather_type_id in %(weather_type_id)s
        ), q2 AS (
            SELECT q1.daily_id, q1.cell_id, q1.date, q1.weather_type_id, q1.value, q1.date_requested,
                ROW_NUMBER() OVER(PARTITION BY q1.cell_id, q1.weather_type_id, q1.date ORDER BY q1.date_requested desc) as rn
            FROM q1
        ) SELECT q2.daily_id, q2.cell_id, q2.date, q2.weather_type_id, q2.value, q2.date_requested FROM q2
        WHERE q2.rn = 1
        """

    args = {
        "cell_id": tuple_cell_id_list,
        "weather_type_id": tuple_weather_type_id,
    }
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    return df_result


def get_weather_type_id_from_db(cursor: Any, parameters: Union[str, List[str]]) -> dict:
    """
    Creates dictionary mapping desired parameters to Demeter weather type IDs.

    Args:
        cursor (Any): Connection to demeter weather database
        parameters (str or list of str): Parameter name[s] to determine `weather_type_id` from demeter

    Returns:
        Dictionary mapping parameter names (as keys) to Demeter weather type IDs (as values).
    """
    if not isinstance(parameters, list):
        parameters = [parameters]
    tuple_parameters = tuple(parameters)

    stmt = """select weather_type_id, weather_type from weather_type
    where weather_type in %(parameters)s"""
    args = {"parameters": tuple_parameters}
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    param_missing = [
        param
        for param in parameters
        if param not in df_result["weather_type"].to_list()
    ]
    msg = f"The following parameters were not found in the db: {str(param_missing)}"
    assert len(param_missing) == 0, msg

    df_result.set_index("weather_type", inplace=True)

    return df_result.to_dict()["weather_type_id"]
