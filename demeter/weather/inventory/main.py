from datetime import datetime, timedelta

from geopandas import GeoDataFrame
from pandas import merge as pd_merge
from pytz import UTC
from sqlalchemy.engine import Connection

from demeter.weather._grid_utils import get_centroid
from demeter.weather.inventory._utils import localize_utc_datetime_with_utc_offset
from demeter.weather.inventory._weather_inventory import (
    get_date_last_requested_for_cell_id,
    get_first_unstable_date,
    get_info_for_world_utm,
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


def update_weather(conn_weather: Connection) -> GeoDataFrame:
    """DOCSTRING."""

    cursor_weather = conn_weather.connection.cursor()

    # for each cell ID, determine most recent data extraction
    df_last_requested = get_date_last_requested_for_cell_id(
        cursor_weather, table="daily"
    )

    # change extraction time to local time for each cell ID
    world_utm_id = [int(val) for val in df_last_requested["world_utm_id"].unique()]
    df_utm = get_info_for_world_utm(cursor_weather, world_utm_id)

    df_full = pd_merge(df_last_requested, df_utm, on="world_utm_id")

    # we need to extract those dates which were "unstable" at the time of the last request
    df_full["local_date_last_requested"] = df_full.apply(
        lambda row: localize_utc_datetime_with_utc_offset(
            row["date_last_requested"], row["utc_offset"].tzinfo
        ),
        axis=1,
    )

    # perform this action at the cell ID level
    df_full["date_first"] = df_full["local_date_last_requested"].map(
        lambda d: get_first_unstable_date(d)
    )

    # last day is 7 days from today UTC
    utc_today = datetime.now(tz=UTC).date()
    df_full["date_last"] = utc_today + timedelta(days=7)

    # get centroid for each cell ID
    df_full["centroid"] = df_full.apply(
        lambda row: get_centroid(
            cursor_weather, row["utm_zone"], row["utm_row"], row["cell_id"]
        ),
        axis=1,
    )

    gdf_full = GeoDataFrame(df_full[GDF_COLS], geometry="centroid")

    return gdf_full


def add_weather():
    pass


def fill_weather():
    pass
