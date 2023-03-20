from datetime import datetime, timedelta

from geopandas import GeoDataFrame
from pandas import concat as pd_concat
from pandas import merge as pd_merge
from pyproj import CRS
from sqlalchemy.engine import Connection

from demeter.weather._grid_utils import get_centroid, get_info_for_world_utm
from demeter.weather.inventory._demeter_inventory import (
    determine_needed_weather_for_demeter,
)
from demeter.weather.inventory._utils import localize_utc_datetime_with_utc_offset
from demeter.weather.inventory._weather_inventory import (
    get_cell_ids_in_weather_table,
    get_date_last_requested_for_cell_id,
    get_first_available_data_year_for_cell_id,
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


def get_gdf_for_update(conn: Connection) -> GeoDataFrame:
    """Creates `gdf` for "update" step in weather workflow.

    Performs an inventory of available data in the weather database. For all cell IDs that have data, data
    is extracted to overwrite any unstable historical data at the previous request time up until 7 days after
    the last full day. See `get_first_unstable_date_at_request_time()` for more information on data
    stability and `get_min_current_date_for_world_utm()` for more information on the last full day.

    This steps assumes that all cell IDs have been completely populated (i.e., have data up until 7 days
    after the last request date) and will not check for nor fill gaps in the database.

    Args:
        conn (Connection): Connection to demeter database

    Returns:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests needed to
            perform "update" step
    """
    cursor = conn.connection.cursor()

    # for each cell ID, determine most recent data extraction
    df_last_requested = get_date_last_requested_for_cell_id(cursor, table="daily")
    if df_last_requested is None:
        return None

    # change extraction time to local time for each cell ID
    world_utm_id = [int(val) for val in df_last_requested["world_utm_id"].unique()]
    df_utm = get_info_for_world_utm(cursor, world_utm_id)

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
        lambda d: get_first_unstable_date_at_request_time(d)
    )

    # last day is 7 days from the last full day across all UTM zones
    today = get_min_current_date_for_world_utm()
    df_full["date_last"] = today + timedelta(days=7)

    # get centroid for each cell ID
    df_full["centroid"] = df_full.apply(
        lambda row: get_centroid(cursor, row["world_utm_id"], row["cell_id"]),
        axis=1,
    )

    gdf_full = GeoDataFrame(
        df_full[GDF_COLS], geometry="centroid", crs=CRS.from_epsg(4326)
    )

    return gdf_full


def get_gdf_for_add(conn: Connection):
    """Creates `gdf` for "add" step in weather workflow.

    This function performs an inventory of Demeter and determines which cell IDs and years need weather data.
    Then, an inventory of the weather database determines which cell IDs already have data and retrieves the
    first year of data for each cell ID. The final `gdf` is composed of cell ID x year combinations of the
    following two types:
    (1) a cell ID already exists in the database but needs earlier years of data than what is available
    (2) a cell ID does not exist in the database and needs to be populated

    This steps assumes that data is populated for each cell ID fully from the first data year up until the
    present.

    Args:
        conn (Connection): Connection to demeter database

    Returns:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests needed to
            perform "add" step
    """

    cursor = conn.connection.cursor()

    gdf_need = determine_needed_weather_for_demeter(cursor)

    # find cell IDs which need more historical years of data
    cell_id = get_cell_ids_in_weather_table(cursor, table="daily")

    if cell_id is not None:
        gdf_available = get_first_available_data_year_for_cell_id(
            cursor, gdf_need["cell_id"].to_list()
        )

        gdf_new_years = pd_merge(gdf_available, gdf_need, on="cell_id")
        keep = gdf_new_years.apply(
            lambda row: row["date_first"] < datetime(row["first_year"], 1, 1), axis=1
        )
        gdf_new_years = gdf_new_years.loc[keep]
        gdf_new_years["date_last"] = gdf_new_years["first_year"].map(
            lambda yr: datetime(yr - 1, 12, 31)
        )

        # find cell IDs which are not in the database at all
        gdf_new_cells = gdf_need.loc[~gdf_need["cell_id"].isin(cell_id)]

        gdf_add = pd_concat(
            [gdf_new_years[gdf_new_cells.columns.values], gdf_new_cells], axis=0
        )
    else:
        gdf_add = gdf_need.copy()

    return gdf_add


def get_gdf_for_fill():
    pass
