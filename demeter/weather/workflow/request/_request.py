"""High-level functions to submit and store Meteomatics requests and any extracted data."""

import logging
from typing import List, Tuple

from geopandas import GeoDataFrame
from joblib import Parallel, delayed
from meteomatics.exceptions import TooManyRequests
from sqlalchemy.engine import Connection
from tqdm import tqdm

from demeter.weather.workflow.insert._data_processing import (
    clean_meteomatics_data,
    filter_meteomatics_data_by_gdf_bounds,
)
from demeter.weather.workflow.insert._insert import (
    insert_daily_weather,
    log_meteomatics_request,
)
from demeter.weather.workflow.request._failed import (
    ERROR_CODES,
    reformat_failed_requests,
)
from demeter.weather.workflow.request._request_utils import (
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

    # log request in request_log
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
    if request["request_seconds"] == ERROR_CODES[TooManyRequests]:
        logging.warning(
            "     Unable to complete request because daily limit was reached."
        )
    return request


def _logging_request_summary(request_list) -> None:
    n_completed = len(request_list)

    failed_requests = [r for r in request_list if r["status"] == "FAIL"]
    n_failed = len(failed_requests)
    percent_failed = round(100 * (n_failed / n_completed))

    n_success = n_completed - n_failed
    percent_success = round(100 * (n_success / n_completed))

    logging.info("     SUCCESS: %s/%s (%s%%)", n_success, n_completed, percent_success)
    logging.info("     FAIL: %s/%s (%s%%)", n_failed, n_completed, percent_failed)


def submit_request_list(
    conn: Connection,
    request_list: List[dict],
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
    parallel: bool = False,
) -> Tuple[List[dict], bool]:
    """Passes requests in `request_list` to `submit_and_maybe_insert_meteomatics_request()`.

    If not all requests can be submitted due to Meteomatics user limits (i.e., TooManyRequests),
    just the completed requests are returned.

    Returns the list of completed requests and a boolean value indicating whether or not all of the
    requests were completed.
    """
    logging.info("   NUMBER OF REQUESTS: %s", len(request_list))

    if parallel:
        logging.warning(
            "`parallel = True` has not yet been implemented in this workflow. No requests will be made."
        )

        n_jobs = 4

        results = Parallel(n_jobs=n_jobs)(
            delayed(submit_and_maybe_insert_meteomatics_request)(
                conn, request, gdf_request, params_to_weather_types
            )
            for request in request_list
        )

        for ind in tqdm(range(len(results)), desc="Submitting requests:"):
            request = results[ind]
            # TODO: When run in parallel, this check is too late (should be part of submit_and_maybe_insert_meteomatics_request)
            # if we ran out of requests, return just those which were completed
            if request["request_seconds"] == ERROR_CODES[TooManyRequests]:
                logging.info(
                    "     Unable to complete requests. No more requests available for today."
                )
                completed_request_list = [r for r in results if "status" in r.keys()]
                _logging_request_summary(completed_request_list)
                return completed_request_list, False
    else:
        results = [request for request in request_list]
        for ind in tqdm(range(len(request_list)), desc="Submitting requests:"):
            request = request_list[ind]
            result = submit_and_maybe_insert_meteomatics_request(
                conn=conn,
                request=request,
                gdf_request=gdf_request,
                params_to_weather_types=params_to_weather_types,
            )
            results[ind] = result

            # if we ran out of requests, return just those which were completed
            if result["request_seconds"] == ERROR_CODES[TooManyRequests]:
                logging.info(
                    "     Unable to complete requests. No more requests available for today."
                )
                completed_request_list = [r for r in results if "status" in r.keys()]
                _logging_request_summary(completed_request_list)
                return completed_request_list, False

    _logging_request_summary(results)
    return results, True


def run_request_step(
    conn: Connection,
    request_list: List[dict],
    gdf_request: GeoDataFrame,
    params_to_weather_types: dict,
    n_jobs: int = 1,
    max_attempts: int = 3,
) -> List[dict]:
    """Submits `request_list` and attempts to re-format and re-submit failed requests where possible.

    This step follows the below logic:
    (1) Determines how many requests are available using `get_n_requests_remaining_for_demeter()`.
    (2) Crops `request_list` along UTM boundaries following the number of requests available.
    (3) Submits the requests in the cropped list.
    --- () If all requests were completed by `submit_request_list()`, all requests are added to `complete_requests`.
    --- Checks for failed requests that need to be reformatted. Reformatted requests are organized and
    --- passed back to Step 1 if the cycle has not already been completed `max_attempts` times.
    --- () Else, only completed requests are added to `complete_requests`.
    (4) Full `completed_requests` is returned.

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
        max_attempts (int): Maximum number of times to complete the "run" cycle before ending function; default is 3.

    Returns a list of all of the `request` dictionaries which were submitted/completed during the function process.
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

        if tries > 1:
            logging.info(
                "   ATTEMPTING RE-REQUEST WITH %s REQUESTS",
                len(pending_requests),
            )

        # trim down request list based on available requests
        n_requests_available = get_n_requests_remaining_for_demeter(conn)
        cut_requests = cut_request_list_along_utm_zone(
            pending_requests, n_requests_available
        )

        if len(cut_requests) > 0:
            logging.info("   (%s) REQUEST SUMMARY:", tries)
            # if parallel is True:
            logging.info(
                "   NUMBER OF REQUESTS (split across %s cores): %s",
                n_jobs,
                len(cut_requests),
            )
            results = Parallel(n_jobs=n_jobs, prefer="threads")(
                delayed(submit_and_maybe_insert_meteomatics_request)(
                    conn, request, gdf_request, params_to_weather_types
                )
                for request in tqdm(cut_requests)
            )
            cut_requests = [r for r in results if "status" in r.keys()]
            _logging_request_summary(cut_requests)
            completed = True if len(cut_requests) == len(cut_requests) else False

            completed_requests += cut_requests

            # stop while loop if we ran out of requests during `submit_request_list` (completed = False)
            # otherwise, check for failed requests and maybe re-run
            if completed:
                pending_requests = reformat_failed_requests(cut_requests)
            else:
                pending_requests = []
        else:
            pending_requests = []

    return completed_requests
