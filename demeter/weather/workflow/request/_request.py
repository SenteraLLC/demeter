"""High-level functions to submit and store Meteomatics requests and any extracted data."""

import logging
from typing import List, Tuple

from geopandas import GeoDataFrame
from meteomatics.exceptions import TooManyRequests
from sqlalchemy.engine import Connection
from tqdm import tqdm

from ..insert._data_processing import (
    clean_meteomatics_data,
    filter_meteomatics_data_by_gdf_bounds,
)
from ..insert._insert import insert_daily_weather, log_meteomatics_request
from ._failed import ERROR_CODES, reformat_failed_requests
from ._request_utils import (
    cut_request_list_along_utm_zone,
    get_n_requests_remaining_for_demeter,
    submit_single_meteomatics_request,
)


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

    rq_gdf_cols_1 = ["world_utm_id", "cell_id", "date_first", "date_last", "centroid"]
    rq_gdf_cols_2 = ["world_utm_id", "cell_id", "date", "parameter", "centroid"]
    assert_stmt_1 = set(rq_gdf_cols_1).issubset(gdf_request.columns)
    assert_stmt_2 = set(rq_gdf_cols_2).issubset(gdf_request.columns)
    assert (
        assert_stmt_1 or assert_stmt_2
    ), f"The columns of `gdf_request` must contain {rq_gdf_cols_1} or {rq_gdf_cols_2}"

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


def submit_request_list(
    conn: Connection,
    request_list: List[dict],
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
    parallel: bool = False,
) -> Tuple[List[dict], bool]:
    """DOCSTRING."""
    if parallel:
        pass
    else:
        for ind in tqdm(range(len(request_list)), desc="Submitting requests:"):
            request = request_list[ind]
            request = submit_and_maybe_insert_meteomatics_request(
                conn=conn,
                request=request,
                gdf_request=gdf_request,
                params_to_weather_types=params_to_weather_types,
            )
            request_list[ind] = request

            # if we ran out of requests, return just those which were completed
            if request["request_seconds"] == ERROR_CODES[TooManyRequests]:
                logging.info(
                    "Unable to complete requests. No more requests available for today."
                )
                completed_request_list = [
                    r for r in request_list if "status" in r.keys()
                ]
                return completed_request_list, False

    return request_list, True


def run_request_step(
    conn: Connection,
    request_list: List[dict],
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
    parallel: bool = False,
    max_attempts: int = 3,
) -> List[dict]:
    """Loops through passed `request_list` and submits each request to Meteomatics and stores data to the database.

    Parallelizable if desired, but this is not currently implemented. Default behavior is running requests consecutively.

    One very important (and simple) way to ensure missing requests are filled in later is by cutting
    the request list along UTM zones, such that we are either running all or none of the requests
    for a given UTM zone. That way, parameter groups are run in completion for a given cell ID x date.
    This is the purpose of the `cut_request_list_along_utm_zone()` function used in this function.

    Args:
        conn: Connection to Demeter weather schema
        request_list (list of dict): List of `request` dictionaries, which each outline the spatiotemporal info for a MM request

        gdf_request (GeoDataFrame): Dataframe containing spatiotemporal information for the given weather
            requests

        params_to_weather_types (dict): Dictionary mapping weather parameters to db weather types IDs
        parallel (bool): Indicates whether requests should be run in parallel (not currently implemented); defaults to False.
    """
    rq_gdf_cols_1 = ["world_utm_id", "cell_id", "date_first", "date_last", "centroid"]
    rq_gdf_cols_2 = ["world_utm_id", "cell_id", "date", "parameter", "centroid"]
    assert_stmt_1 = set(rq_gdf_cols_1).issubset(gdf_request.columns)
    assert_stmt_2 = set(rq_gdf_cols_2).issubset(gdf_request.columns)
    assert (
        assert_stmt_1 or assert_stmt_2
    ), f"The columns of `gdf_request` must contain {rq_gdf_cols_1} or {rq_gdf_cols_2}"

    # add lat/lon if not already added
    if not set(["lon", "lat"]).issubset(gdf_request.columns):
        gdf_request.insert(0, "lon", gdf_request.geometry.x)
        gdf_request.insert(0, "lat", gdf_request.geometry.y)

    pending_requests = request_list.copy()
    completed_requests = []
    tries = 0

    while (len(pending_requests) > 0) and (tries < max_attempts):
        tries += 1

        # trim down request list based on available requests
        n_requests_available = get_n_requests_remaining_for_demeter(conn)
        cut_requests = cut_request_list_along_utm_zone(
            pending_requests, n_requests_available
        )

        if len(cut_requests) > 0:
            # run these requests and add to list of completed requests
            cut_requests, completed = submit_request_list(
                conn, cut_requests, gdf_request, params_to_weather_types, parallel
            )
            completed_requests += cut_requests

            # stop while loop if we ran out of requests during `submit_request_list` (completed = False)
            # otherwise, check for failed requests and maybe re-run
            if completed:
                pending_requests = reformat_failed_requests(cut_requests)
                logging.info("Re-requesting in %s requests", len(pending_requests))
            else:
                pending_requests = []
        else:
            pending_requests = []

    return completed_requests
