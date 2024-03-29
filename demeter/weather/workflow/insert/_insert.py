"""Helper functions for cleaning and inserting MM data into the demeter weather schema."""

from datetime import timedelta
from typing import Any

from pandas import DataFrame
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

    df_log = DataFrame([request])[rq_keys]
    engine = conn.engine
    df_log.to_sql("request_log", con=engine, if_exists="append", index=False)


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
