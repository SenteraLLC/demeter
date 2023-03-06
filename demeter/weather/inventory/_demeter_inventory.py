"""Helper function for broadly reconciling differences between Demeter and Demeter Weather in terms of spatiotemporal needs.

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
from pandas import merge as pd_merge
from pytz import UTC
from shapely.errors import ShapelyDeprecationWarning
from shapely.wkb import loads as wkb_loads

from demeter.weather import get_cell_id

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
    """Gets first planting date for a field ID or list of field IDs that is stored in `demeter` as DataFrame.

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
    cursor: Any, field_id: list[int], n_hist_years: int = 10
) -> DataFrame:
    """Gets list of dates needed for a field ID (or list of field IDs) based on available planting dates.

    Assumes data is desired from Jan 1 of the year `n_hist_years` years before the planting year.
    Then, it adds the last date with the current date so as to ensure that the last year is recognized
    as the current year.

    Args:
        cursor (Any): Connection to `demeter` schema
        field_id (int): Demeter field ID[s] for which to determine bounds
        n_hist_years (int): The number of years before the first planting date to get data
    """
    if not isinstance(field_id, list):
        field_id = [field_id]

    df = get_first_plant_date_for_field_id(cursor, field_id).rename(
        columns={"date_performed": "date_planted"}
    )

    df["year_planted"] = df["date_planted"].dt.year
    df["date_first"] = df["year_planted"].map(
        lambda yr: datetime(yr - n_hist_years, 1, 1)
    )
    df["date_last"] = datetime.now(tz=UTC).date()

    return df[["field_id", "date_first", "date_last"]]


def get_centroid_for_field_id(
    cursor: Any, field_id: Union[int, List[int]]
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


def get_spatiotemporal_weather_database_needs(
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

    gdf_field_space = get_centroid_for_field_id(cursor_demeter, field_id)
    gdf_field_space[["cell_id", "centroid"]] = gdf_field_space.apply(
        lambda row: get_cell_id(cursor_weather, row["field_centroid"]), axis=1
    )

    df_field_time = get_temporal_bounds_for_field_id(cursor_demeter, field_id)

    gdf_full = pd_merge(gdf_field_space, df_field_time, on="field_id")

    gdf_unique = gdf_full.sort_values(["cell_id", "date_first"]).drop_duplicates(
        ["cell_id"], keep="first"
    )  # only need to worry about `date_first`

    gdf_clean = GeoDataFrame(
        gdf_unique[["cell_id", "date_first", "date_last", "centroid"]],
        geometry="centroid",
    )

    return gdf_clean
