"""Helper functions for running inventory on Demeter in terms of spatiotemporal needs for inserted fields.

Focuses on "add" step in weather extraction process.
"""

import warnings
from datetime import datetime
from typing import (
    Any,
    List,
    Union,
)

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import merge as pd_merge
from pytz import UTC
from shapely.errors import ShapelyDeprecationWarning
from shapely.geometry import Point
from shapely.wkb import loads as wkb_loads

from demeter.weather import get_cell_id
from demeter.weather._grid_utils import get_centroid, get_world_utm_info_for_cell_id
from demeter.weather.inventory._utils import localize_utc_datetime_with_timezonefinder

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def get_all_field_ids(cursor: Any) -> List:
    """Gets all field IDs currently inserted into Demeter."""
    stmt = """
    select field_id from field;
    """
    cursor.execute(stmt)
    df_result = DataFrame(cursor.fetchall())

    if len(df_result) > 0:
        return df_result["field_id"].to_list()
    else:
        return None


def get_first_plant_date_for_field_id(
    cursor: Any, field_id: Union[int, List[int]]
) -> Union[DataFrame, None]:
    """Gets first planting date (UTC) for a field ID or list of field IDs that is stored in `demeter` as DataFrame.

    Returns None if no planting date exists in demeter for given field IDs.

    Args:
        cursor (Any): Connection to `demeter` schema
        field_id (int): Demeter field ID to get planting date for
    """
    field_id = tuple(field_id)

    stmt = """
    select field_id, date_performed from act where act_type = 'plant' and field_id in %(field_id)s
    """
    args = {"field_id": field_id}
    cursor.execute(stmt, args)
    df_result = DataFrame(cursor.fetchall())

    assert (
        len(df_result) > 0
    ), "No planting dates are available in demeter for `field_id`."

    cols = ["field_id", "date_performed"]
    df_first_result = df_result.sort_values(cols).drop_duplicates(keep="first")
    return df_first_result


def get_temporal_bounds_for_field_id(
    cursor: Any,
    field_id: list[int],
    field_centroid: List[Point],
    n_hist_years: int = 10,
) -> DataFrame:
    """Gets list of dates needed for a field ID (or list of field IDs) based on available planting dates.

    Planting dates for each field are localized using `TimezoneFinder()` to ensure appropriate coverage
    for weather in local times.

    Assumes data is desired from Jan 1 of the year `n_hist_years` years before the planting year.
    Then, it adds the last date with the current date so as to ensure that the last year is recognized
    as the current year.

    Args:
        cursor (Any): Connection to `demeter` schema
        field_id (int): Demeter field ID[s] for which to determine bounds
        field_centroid (Point): Centroid (or location on field) to use to determine time zone
        n_hist_years (int): The number of years before the first planting date to get data
    """
    if not isinstance(field_id, list):
        field_id = [field_id]

    if not isinstance(field_centroid, list):
        field_centroid = [field_centroid]

    msg = "There must be one field centroid for each field ID passed."
    assert len(field_id) == len(field_centroid), msg

    gdf = GeoDataFrame(
        data={"field_id": field_id, "field_centroid": field_centroid},
        geometry="field_centroid",
    )

    df_plant = get_first_plant_date_for_field_id(cursor, field_id).rename(
        columns={"date_performed": "date_planted"}
    )
    gdf = pd_merge(gdf, df_plant, on="field_id", how="left")

    msg = "Planting dates are missing for the given field ID[s]."
    assert len(gdf.loc[gdf["date_planted"].isna()]) == 0, msg

    # localize planting date with political time zones to ensure appropriate weather coverage
    gdf["date_planted_local"] = gdf.apply(
        lambda row: localize_utc_datetime_with_timezonefinder(
            d=row["date_planted"], geom=row["field_centroid"]
        ),
        axis=1,
    )

    gdf["year_planted"] = gdf["date_planted_local"].map(lambda d: d.year)
    gdf["date_first"] = gdf["year_planted"].map(
        lambda yr: datetime(yr - n_hist_years, 1, 1)
    )
    gdf["date_last"] = datetime.now(tz=UTC).date()

    return gdf[["field_id", "date_first", "date_last"]]


def get_field_centroid_for_field_id(
    cursor: Any,
    field_id: Union[int, List[int]],
) -> GeoDataFrame:
    """Get centroid of field ID's geom and returns as GeoDataFrame.

    Args:
        cursor: Connection to demeter database
        field_id (int or list of int): Field ID[s] for which to get field centroid[s]
    """
    field_id_tuple = tuple(field_id)

    stmt = """
    select f.field_id, ST_Centroid(geo.geom) as centroid
    from (
        select geom, geom_id from demeter.geom
    ) as geo
    cross join demeter.field as f
    where field_id in %(field_id)s and f.geom_id = geo.geom_id;
    """
    args = {"field_id": field_id_tuple}
    cursor.execute(stmt, args)

    df_result = DataFrame(cursor.fetchall())

    assert len(df_result) > 0, "No geom was found for `field_id` in demeter."

    df_result["centroid"] = df_result["centroid"].map(
        lambda hex: wkb_loads(hex, hex=True)
    )
    df_result.rename(columns={"centroid": "field_centroid"}, inplace=True)

    gdf_result = GeoDataFrame(df_result, geometry="field_centroid")
    return gdf_result


def determine_needed_weather_for_demeter(
    cursor_demeter: Any,
    cursor_weather: Any,
) -> DataFrame:
    """Determine the spatiotemporal bounds of needed weather data based on available fields in Demeter.

    Args:
        cursor_demeter: Connection to demeter schema
        cursor_weather: Connection to demeter weather schema
    """

    # TODO: Are we requiring that all fields have a planting date? If so, fix assert function.
    field_id = get_all_field_ids(cursor_demeter)

    assert len(field_id) > 0, "There are no fields in `demeter`."

    gdf_field_space = get_field_centroid_for_field_id(cursor_demeter, field_id)
    gdf_field_space["cell_id"] = gdf_field_space.apply(
        lambda row: get_cell_id(cursor_weather, row["field_centroid"]), axis=1
    )

    # get temporal bounds based on planting date, location, and `n_hist_years`
    df_field_time = get_temporal_bounds_for_field_id(
        cursor_demeter, field_id, gdf_field_space["field_centroid"].to_list()
    )

    gdf_full = pd_merge(gdf_field_space, df_field_time, on="field_id")

    gdf_unique = gdf_full.sort_values(["cell_id", "date_first"]).drop_duplicates(
        ["cell_id"], keep="first"
    )  # only need to worry about `date_first`

    # add world utm ID to make centroid queries faster and add UTM zone
    df_world_utm = gdf_unique.apply(
        lambda row: get_world_utm_info_for_cell_id(cursor_weather, row["cell_id"]).iloc[
            0
        ],
        axis=1,
    )

    gdf_world_utm = pd_concat([gdf_unique, df_world_utm], axis=1).rename(
        columns={"zone": "utm_zone"}
    )

    gdf_world_utm["centroid"] = gdf_world_utm.apply(
        lambda row: get_centroid(cursor_weather, row["world_utm_id"], row["cell_id"]),
        axis=1,
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
    gdf_clean = GeoDataFrame(
        gdf_world_utm[cols],
        geometry="centroid",
    )

    return gdf_clean
