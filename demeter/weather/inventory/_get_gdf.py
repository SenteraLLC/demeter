"""High-level functions to perform inventory and generate `gdf` for different steps of weather process."""
from datetime import datetime, timedelta

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import merge as pd_merge
from pyproj import CRS
from pytz import UTC
from sqlalchemy.engine import Connection

from demeter.weather.inventory._demeter_inventory import (
    get_weather_grid_info_for_all_demeter_fields,
)
from demeter.weather.inventory._weather_inventory import (
    get_daily_weather_type_for_cell_id,
    get_date_last_requested_for_cell_id,
    get_first_available_data_year_for_cell_id,
    get_first_unstable_date_at_request_time,
    get_populated_cell_ids,
    get_weather_grid_info_for_populated_cell_ids,
)
from demeter.weather.query import get_daily_weather_types
from demeter.weather.utils.grid import get_centroid, get_info_for_world_utm
from demeter.weather.utils.time import (
    get_date_list_for_date_range,
    get_min_current_date_for_world_utm,
    localize_utc_datetime_with_utc_offset,
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

    # ensure that data hasn't already been extracted for a given cell ID
    utc_date_now = datetime.now(tz=UTC).date()
    keep = df_last_requested["date_last_requested"].map(
        lambda dt: dt.date() < utc_date_now
    )
    df_last_requested = df_last_requested.loc[keep]
    if len(df_last_requested) == 0:
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

    This function performs an inventory of Demeter and determines which cell IDs and years need weather data
    to fully represent `demeter.fields`. Then, an inventory of the weather database determines which cell IDs
    already have data and retrieves the first year of data for each cell ID. The final `gdf` is composed of
    cell ID x year combinations of the following two types:
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

    # Find cell IDs currently present in `weather.daily` that need more historical years of data
    cell_ids_weather = get_populated_cell_ids(cursor, table="daily")[
        "cell_id"
    ].to_list()

    # Find cell IDs represented by all demeter fields (their cell IDs may or may not be present in `weather.daily`)
    gdf_need = get_weather_grid_info_for_all_demeter_fields(cursor)
    gdf_available = get_first_available_data_year_for_cell_id(
        cursor_weather=cursor, cell_id_list=gdf_need["cell_id"].to_list()
    )

    # If any cell IDs from `gdf_need` exist already in the database, we need to see which cell IDs need weather data for
    # any "new years" (gdf_new_years), as well as identify cell_ids/fields that are not yet present in the daily table at all (gdf_new_cells).
    if len(gdf_available) > 0:
        gdf_new_years = pd_merge(gdf_need, gdf_available, on="cell_id")
        keep = gdf_new_years.apply(
            lambda row: row["date_first"] < datetime(row["first_year"], 1, 1), axis=1
        )
        gdf_new_years = gdf_new_years.loc[keep]
        gdf_new_years["date_last"] = gdf_new_years["first_year"].map(
            lambda yr: datetime(yr - 1, 12, 31)
        )

        # find cell IDs for the fields that are present, but don't have any weather data yet
        gdf_new_cells = gdf_need.loc[~gdf_need["cell_id"].isin(cell_ids_weather)]

        gdf_add = pd_concat(
            [gdf_new_years[gdf_new_cells.columns.values], gdf_new_cells], axis=0
        )
    else:  # Of the needed cell_ids/fields, nothing is available yet. This is the first weather data to be populated.
        gdf_add = gdf_need.copy()

    # represents the grid info for all cell_ids that need new weather added (could be new years, new cell_ids, or both)
    return gdf_add


def get_gdf_for_fill(conn: Connection) -> GeoDataFrame:
    """Creates `gdf` for "fill" step in weather workflow.

    This function first performs an inventory of Demeter and determines which cell IDs, parameters, and dates need
    weather data. Second, it determines all cell IDs which already have data in the `daily` table and identifies
    the first year of data available for each, setting Jan 1 of that year as the first date of data needed at the cell
    ID level. Finally, the two inventories are merged together.

    Next, an exhuastive inventory of the weather database is performed to determine which data has
    already been extracted and identifies data gaps. The final `gdf` is composed of cell ID x date x parameter
    combinations which are missing from the database. Realistically, this shouldn't really require too many
    requests since data will be checked regularly and the "update" and "add" steps are completed before this step.

    The exhuastive inventory is a somewhat brute force method. After determining the weather grid needs for
    `demeter`, the function loops through unique world utm polygon x parameter combinations, gets all of the
    extracted weather data that is relevant to each combination, and verifies the following two things:
    (1) at the current UTC datetime, all dates have stable data available where it is possible (i.e., extracted
    more than 24 hours after the local end of day)
    (2) at the current UTC datetime, there is at least one row of data available for each date up until 7 days
    past the current minimum UTM date (following same protocol in "update")

    This step will NOT check for gaps for cell IDs that are not currently relevant to `demeter.field`. However, the
    update step will continue to get the majority of the data for these cell IDs. Then, once a specific cell ID is
    again relevant in `demeter.field`, the next "fill" step would fll in these gaps.

    Args:
        conn (Connection): Connection to demeter database

    Returns:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date x parameter informaton for MM requests needed to
            perform "fill" step
    """

    cursor = conn.connection.cursor()

    # perform two inventories
    gdf_need_fields = get_weather_grid_info_for_all_demeter_fields(cursor)
    gdf_need_weather = get_weather_grid_info_for_populated_cell_ids(cursor)

    # merge inventories together; keeping the later `date_first` for duplicates
    gdf_need = (
        pd_concat([gdf_need_fields, gdf_need_weather], axis=0)
        .sort_values(["cell_id", "date_first"])
        .drop_duplicates(["cell_id"], keep="last")
    )

    # get unique world_utm_id
    world_utm_id = list(gdf_need["world_utm_id"].unique())

    # get all daily weather types in the database
    df_parameters = get_daily_weather_types(cursor)
    df_fill = None

    # like in "update" step, we ensure data is available until 7 days from min current date
    min_current_date = get_min_current_date_for_world_utm()
    last_date_forecast = min_current_date + timedelta(days=7)

    # the following is a brute force inventory at this point to avoid querying too much data at once
    # at most, this loop will have n_params (12) x 1201 iterations => 14412 (not that bad)
    # first, we loop through world utm polygons
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

        # determine date range bounds for each cell ID and explode to make one row per cell id x date
        df = gdf_world_utm[["cell_id", "date_first", "date_last"]]
        df["date"] = df.apply(
            lambda row: get_date_list_for_date_range(
                row["date_first"], last_date_forecast
            ),
            axis=1,
        )
        df_long = df.explode(["date"])[["cell_id", "date"]]

        # then, we loop through parameters
        for _, row in df_parameters.iterrows():
            # get all weather and, for each cell ID x date, keep most recent row
            df_weather = get_daily_weather_type_for_cell_id(
                cursor,
                cell_id_list=gdf_world_utm["cell_id"].to_list(),
                weather_type_id=row["weather_type_id"],
            )
            df_weather.sort_values(["date_requested"], inplace=True)
            df_weather.drop_duplicates(
                ["weather_type_id", "cell_id", "date"], keep="last", inplace=True
            )

            # localize date requested
            df_weather["local_date_requested"] = df_weather.apply(
                lambda row: localize_utc_datetime_with_utc_offset(
                    row["date_requested"], utc_offset
                ),
                axis=1,
            )

            # mark if data row is "stable" (i.e., less than first unstable date)
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

            # join "stable" data with `df_long` to highlight data gaps
            df_join = pd_merge(df_long, df_keep, how="left", on=["cell_id", "date"])
            df_gaps = df_join.loc[df_join["daily_id"].isna()][["cell_id", "date"]]
            df_gaps.insert(df_gaps.shape[1], "parameter", row["weather_type"])

            if df_fill is None:
                df_fill = df_gaps.copy()
            else:
                df_fill = pd_concat([df_fill, df_gaps], axis=0)

    cell_id_keys = ["utm_zone", "utc_offset", "world_utm_id", "cell_id", "centroid"]

    if len(df_fill) > 0:
        gdf_world_utm = gdf_need[cell_id_keys].drop_duplicates(["cell_id"])
        gdf_fill = pd_merge(gdf_world_utm, df_fill, on="cell_id")

        if not isinstance(gdf_fill, GeoDataFrame):
            gdf_fill = GeoDataFrame(gdf_fill, geometry="centroid")

        return gdf_fill

    else:
        df = DataFrame(data=[], columns=cell_id_keys + ["date", "parameter"])
        return GeoDataFrame(df, geometry="centroid")
