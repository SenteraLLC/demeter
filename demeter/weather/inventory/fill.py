from datetime import (
    date,
    datetime,
    timedelta,
)
from typing import Any, List

from pandas import DataFrame, Timestamp
from pandas import concat as pd_concat
from pandas import date_range
from pandas import merge as pd_merge
from sqlalchemy.engine import Connection

from demeter.weather._grid_utils import get_info_for_world_utm
from demeter.weather.inventory._demeter_inventory import (
    determine_needed_weather_for_demeter,
)
from demeter.weather.inventory._utils import localize_utc_datetime_with_utc_offset
from demeter.weather.inventory._weather_inventory import (
    get_first_unstable_date_at_request_time,
    get_min_current_date_for_world_utm,
)

GDF_COLS = [
    "utm_zone",
    "utc_offset",
    "world_utm_id",
    "cell_id",
    "centroid",
    "date_first",
    "date_last",
]


def get_daily_weather(
    cursor: Any, cell_id: List[int], weather_type_id: int
) -> DataFrame:
    tuple_cell_id = tuple(cell_id)

    stmt = """
    select * from daily
    where cell_id in %(cell_id)s
    and weather_type_id = %(weather_type_id)s
    """
    args = {"cell_id": tuple_cell_id, "weather_type_id": weather_type_id}
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    return df_result


def get_weather_parameters(cursor: Any) -> DataFrame:
    """Get all weather parameters from database and return as DataFrame."""

    stmt = """select * from weather_type"""
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    return df_result


def get_date_range(date_first: datetime, date_last: date) -> List[Timestamp]:
    list_timestamps = (
        date_range(start=date_first, end=date_last, freq="d").to_pydatetime().tolist()
    )

    list_dates = [d.date() for d in list_timestamps]

    return list_dates


def get_gdf_for_fill(conn: Connection):
    """Creates `gdf` for "fill" step in weather workflow.

    This function performs an inventory of Demeter and determines which cell IDs, parameters, and dates need
    weather data. Then, an exhuastive inventory of the weather database is performed to determine which data has
    already been extracted. The final `gdf` is composed of cell ID x date x parameter combinations which are missing
    from the database.

    Args:
        conn (Connection): Connection to demeter database

    Returns:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date x parameter informaton for MM requests needed to
            perform "fill" step
    """

    cursor = conn.connection.cursor()

    gdf_need = determine_needed_weather_for_demeter(cursor)

    # get unique world_utm_id
    world_utm_id = list(gdf_need["world_utm_id"].unique())

    # loop through parameters
    df_parameters = get_weather_parameters(cursor)
    df_fill = None

    # let's make sure all data up until minimum "day before yesterday" is "stable"
    # after that point, we just make sure that data is available up until 7 days from min current date
    min_current_date = get_min_current_date_for_world_utm()
    last_date_forecast = min_current_date + timedelta(days=7)

    # brute force inventory at this point to avoid querying too much data at once
    # at most, this will run n_params (12) x 1201 iterations => 14412 (not that bad)
    for world_utm in world_utm_id:
        gdf_world_utm = gdf_need.loc[gdf_need["world_utm_id"] == world_utm]

        utc_offset = (
            get_info_for_world_utm(cursor, int(world_utm)).at[0, "utc_offset"].tzinfo
        )

        # determine last date that should be stable after this fill step
        current_datetime_local = datetime.now(tz=utc_offset)
        first_date_unstable_local = get_first_unstable_date_at_request_time(
            current_datetime_local
        )
        last_date_stable_local = first_date_unstable_local + timedelta(days=-1)

        # determine date range bounds for each cell ID
        df = gdf_world_utm[["cell_id", "date_first", "date_last"]]
        df["date"] = df.apply(
            lambda row: get_date_range(row["date_first"], last_date_forecast), axis=1
        )
        df_long = df.explode(["date"])[["cell_id", "date"]]

        # this will allow us to check for parameter limits when that is implemented
        for _, row in df_parameters.iterrows():
            df_weather = get_daily_weather(
                cursor,
                cell_id=gdf_world_utm["cell_id"].to_list(),
                weather_type_id=row["weather_type_id"],
            )

            # localize date requested
            df_weather["local_date_requested"] = df_weather.apply(
                lambda row: localize_utc_datetime_with_utc_offset(
                    row["date_requested"], utc_offset
                ),
                axis=1,
            )

            # mark if data row is "stable"
            which_stable = df_weather.apply(
                lambda row: get_first_unstable_date_at_request_time(
                    row["local_date_requested"]
                ).date()
                > row["date"],
                axis=1,
            )

            # mark if data row is forecast or recent historical
            which_recent = df_weather["date"].map(
                lambda dt: dt > last_date_stable_local.date()
            )

            which_keep = list(which_stable | which_recent)
            df_keep = df_weather.loc[which_keep]

            # join to highlight gaps
            df_join = pd_merge(df_long, df_keep, how="left", on=["cell_id", "date"])
            df_gaps = df_join.loc[df_join["daily_id"].isna()][["cell_id", "date"]]
            df_gaps.insert(df_gaps.shape[1], "parameter", row["weather_type"])

            if df_fill is None:
                df_fill = df_gaps.copy()
            else:
                df_fill = pd_concat([df_fill, df_gaps], axis=0)

    # TODO: Do we want to consolidate this based on cell ID and date? Does it depend on the splitting logic?
    # Realistically, this shouldn't really require too many requests since data will be checked regularly
    # and the "update" and "add" steps are completed before this step.

    return df_fill
