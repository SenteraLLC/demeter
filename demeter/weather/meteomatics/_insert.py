"""Helper functions for organizing and inserting data into the demeter weather schema."""

from datetime import timedelta, timezone
from typing import (
    Any,
    List,
    Union,
)

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import melt as pd_melt
from pandas import merge as pd_merge
from sqlalchemy.engine import Connection


def maybe_insert_weather_type_to_db(
    cursor: Any,
    weather_type: str,
    temporal_extent: timedelta,
    units: str,
    description: str,
) -> None:
    """Inserts a given `weather_type` if it doesn't already exist in the database."""

    stmt = """select * from weather_type where weather_type = %(weather_type)s"""
    args = {"weather_type": weather_type}
    cursor.execute(stmt, args)
    res = cursor.fetchall()

    if len(res) == 0:
        stmt = """insert into weather_type (weather_type, temporal_extent, units, description)
        VALUES (%(weather_type)s, %(temporal_extent)s, %(units)s, %(description)s)"""
        args = {
            "weather_type": weather_type,
            "temporal_extent": temporal_extent,
            "units": units,
            "description": description,
        }
        cursor.execute(stmt, args)


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


def log_meteomatics_request(conn: Connection, request: dict) -> None:
    """
    Takes `request` information with request metadata and logs it in `weather.request_log` in demeter.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter weather database
        request (dict): Request metadata
    """
    rq_keys = [
        "zone",
        "utm_request_id",
        "n_pts_requested",
        "startdate",
        "enddate",
        "parameters",
        "date_requested",
        "status",
        "request_seconds",
    ]
    assert all(
        [(key in request.keys()) for key in rq_keys]
    ), f"`request` must contain the following keys: {rq_keys}"

    request["str_parameters"] = str(request["parameters"])
    stmt = """insert into request_log (zone, utm_request_id, n_pts_requested, startdate, enddate, parameters, date_requested, status, request_seconds)
    values (%(zone)s, %(utm_request_id)s, %(n_pts_requested)s, %(startdate)s, %(enddate)s, %(str_parameters)s, %(date_requested)s, %(status)s, %(request_seconds)s);
    """
    conn.execute(stmt, request)


def clean_meteomatics_data(
    df_wx: DataFrame, gdf_cell_id: GeoDataFrame, utc_offset: timezone
) -> DataFrame:
    """
    Cleans dataframe returned by Meteomatics request by adding cell ID back, unnesting indices, re-localizing date again, and melting to long format.

    Args:
        df_wx (pandas.DataFrame): Dataframe containing returned Meteomatics data for desired spatiotemporal AOI and parameters
        gdf_cell_id (geopandas.GeoDataFrame): GeoDataFrame that maps cell ID to passed `lon` and `lat` values.
        utc_offset (datetime.timezone): UTC offset for needed UTM zone

    Returns:
        df_melt_clean (pandas.DataFrame): Cleaned Meteomatics data
    """

    df_wx_in = df_wx.copy()

    # add cell ID back to MM data
    df_wx_full = pd_merge(
        df_wx_in.reset_index(), gdf_cell_id, how="left", on=["lat", "lon"]
    )

    # adjust validdate column to aware of `utc_offset` before converting to date
    df_wx_full["date"] = df_wx_full["validdate"].dt.tz_convert(utc_offset).dt.date

    # finalize columns
    df_wx_clean = df_wx_full.drop(columns=["lat", "lon", "validdate", "centroid"])

    # melt wide dataframe to long format
    df_melt_clean = pd_melt(
        df_wx_clean, id_vars=["world_utm_id", "cell_id", "date", "date_requested"]
    )

    return df_melt_clean


def insert_daily_weather(conn: Connection, df_clean: DataFrame):
    """
    Takes cleaned Meteomatics data (i.e., `df_clean`) and inserts all rows into `weather.daily` in demeter.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter weather database
        df_clean (DataFrame): Cleaned MM weather (i.e., passed through `clean_meteomatics_data()`)
    """
    rq_cols = [
        "world_utm_id",
        "cell_id",
        "date",
        "weather_type_id",
        "value",
        "date_requested",
    ]
    assert set(rq_cols).issubset(
        df_clean.columns
    ), f"The following columns must be in `df_clean`: {rq_cols}"

    engine = conn.engine
    df_insert = df_clean[rq_cols]

    df_insert.to_sql("daily", con=engine, if_exists="append", index=False)


# def maybe_add_enum_value_to_weather_parameter_type(
#     conn: Connection, new_value: str
# ) -> None:
#     """If not already listed as ENUM value, update database to recognize `new_value` as valid `weather_parameter` type."""
#     stmt = (
#         """SELECT unnest(enum_range(NULL::weather.weather_parameter))::text AS values"""
#     )
#     conn.execute(stmt)
#     df_values = DataFrame(conn.fetchall())

#     if new_value in df_values["values"].to_list():
#         print("ENUM value already included.")
#     else:
#         stmt = """ALTER TYPE weather_parameter ADD VALUE %(value)s"""
#         args = {"value": new_value}
#         conn.execute(stmt, args)
#         print("New ENUM value added.")
