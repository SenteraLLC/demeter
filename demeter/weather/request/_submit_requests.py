"""High-level functions to submit and store Meteomatics requests and any extracted data."""

from typing import List

from geopandas import GeoDataFrame
from sqlalchemy.engine import Connection

from demeter.weather.insert._data_processing import (
    clean_meteomatics_data,
    filter_meteomatics_data_by_gdf_bounds,
)
from demeter.weather.insert._insert import insert_daily_weather, log_meteomatics_request
from demeter.weather.request._utils import submit_single_meteomatics_request


def submit_and_maybe_insert_meteomatics_request(
    conn: Connection,
    request: dict,
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
) -> dict:
    """Submits MM request based on `request`, logs metadata to db, and, if request is successful, inserts data to db.

    Args:
        conn (Connection): Connection to demeter weather database
        request (dict): Information for outlining Meteomatics request

        gdf_request (GeoDataFrame): Dataframe matching lat/lon coordinates to cell IDs and containing
            cell-level temporal bounds for weather data needed

        params_to_weather_types (dict): Dictionary mapping weather parameters to db weather types IDs

    Returns `request` with updated information on request metadata (i.e., "status", "request_seconds")
    """

    rq_gdf_cols = ["world_utm_id", "cell_id", "date_first", "date_last", "lat", "lon"]
    assert set(rq_gdf_cols).issubset(
        gdf_request.columns
    ), f"The following columns must be in `gdf_request`: {rq_gdf_cols}"

    df_wx, request = submit_single_meteomatics_request(request=request)

    # insert into database
    log_meteomatics_request(conn, request)

    # if request was successful, add to database
    if request["status"] == "SUCCESS":
        utc_offset = request["utc_offset"]
        df_clean = clean_meteomatics_data(
            df_wx=df_wx,
            gdf_request=gdf_request,
            utc_offset=utc_offset,
        )

        df_filter = filter_meteomatics_data_by_gdf_bounds(
            df_clean=df_clean, gdf_request=gdf_request
        )

        df_filter["weather_type_id"] = df_filter["variable"].map(
            params_to_weather_types
        )
        insert_daily_weather(conn, df_filter)

    return request


def submit_requests(
    conn: Connection,
    request_list: List[dict],
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
    parallel: bool = False,
):
    """Loops through passed `request_list` and submits each request to Meteomatics and stores data to the database.

    Parallelizable if desired, but this is not currently implemented. Default behavior is running requests consecutively.

    Args:
        conn: Connection to Demeter weather schema
        request_list (list of dict): List of `request` dictionaries, which each outline the spatiotemporal info for a MM request

        gdf_request (GeoDataFrame): Dataframe containing spatiotemporal information for the given weather
            requests

        params_to_weather_types (dict): Dictionary mapping weather parameters to db weather types IDs
        parallel (bool): Indicates whether requests should be run in parallel (not currently implemented); defaults to False.
    """
    rq_gdf_cols = ["world_utm_id", "cell_id", "date_first", "date_last", "centroid"]
    assert set(rq_gdf_cols).issubset(
        gdf_request.columns
    ), f"The following columns must be in `gdf_request`: {rq_gdf_cols}"

    # add lat/lon if not already added
    if not set(["lon", "lat"]).issubset(gdf_request.columns):
        gdf_request.insert(0, "lon", gdf_request.geometry.x)
        gdf_request.insert(0, "lat", gdf_request.geometry.y)

    if parallel:
        pass
    else:
        for ind in range(len(request_list)):
            request = request_list[ind]
            request = submit_and_maybe_insert_meteomatics_request(
                conn=conn,
                request=request,
                gdf_request=gdf_request,
                params_to_weather_types=params_to_weather_types,
            )
            request_list[ind] = request

    return request_list
