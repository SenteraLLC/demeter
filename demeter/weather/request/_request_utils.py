"""Helper functions for submitting and tracking Meteomatics requests."""

from datetime import datetime, timedelta
from os import getenv
from time import time
from typing import (
    List,
    Tuple,
    Union,
)

from meteomatics.api import query_time_series, query_user_limits
from meteomatics.exceptions import NotFound
from pandas import DataFrame, Series
from pytz import UTC
from requests import ReadTimeout


def cut_request_list_along_utm_zone(
    request_list: List[dict], n_requests: int
) -> List[dict]:
    """Cuts request list along UTM zone boundaries so we do not request partial data for a UTM zone in a given day.

    By cutting request lists along UTM zone boundaries, we ensure that cell IDs are not half populated in
    the "add" step. Any data which is "cut" from the `request_list` will still be picked up by the "add" step
    tomorrow.

    Args:
        request_list (list of dict): List of `request` dictionaries for Meteomatics requests
        n_requests (int): Maximum number of requests to return

    Returns `cut_request_list` (list of dict) which contains a maximum of `n_requests` complete UTM-zone
    requests from `request_list`.
    """

    if len(request_list) <= n_requests:
        return request_list

    df_zone = (
        DataFrame(
            data={"count": Series([rq["zone"] for rq in request_list]).value_counts()}
        )
        .reset_index(names="zone")
        .sort_values(["count"])
    )
    df_zone["n_requests"] = df_zone["count"].cumsum()

    df_cut = df_zone.loc[df_zone["n_requests"] < n_requests]
    cut_request_list = [
        rq for rq in request_list if rq["zone"] in df_cut["zone"].to_list()
    ]

    return cut_request_list


def get_n_requests_remaining() -> int:
    """Determines how many requests are remaining on Sentera's Meteomatics license for today based on hard limit.

    The soft limit for our account is not given through this api request function.
    """
    res = query_user_limits(
        "sentera",
        getenv("METEOMATICS_KEY"),
    )
    used, total = res["requests since last UTC midnight"]

    return total - used


def check_coordinate_rounding(coord: Tuple[float, float], n: int = 5) -> bool:
    """Helper function for checking coordinate rounding for cell ID centroids.

    Since Python is not maintaining significant digits here, we can only ensure that
    there are not MORE than `n` decimal places.
    """
    first = str(coord[0])[::-1].find(".") <= n
    second = str(coord[1])[::-1].find(".") <= n

    if first and second:
        return True
    else:
        return False


def submit_single_meteomatics_request(
    request: dict, interval: timedelta = timedelta(hours=24)
) -> Tuple[Union[DataFrame, None], dict]:
    """
    Makes one Meteomatics request based on information outlined in `request` and adds request information to `request`.

    When `on_invalid` argument is set to "fill_with_invalid", invalid data is replaced with NaN and the full
    time series is returned.

    See https://www.meteomatics.com/en/api/request/optional-parameters/ for information on other parameters
    and defaults.

    Handles:
        `ReadTimeOut`: occurs when the request takes too long (> 300 seconds)

        `NotFound`: occurs when a parameter is not available for the requested
            time frame x point combination

    Args:
        request (dict): Dictionary containing metadata necessary to create a needed Meteomatics request.

        interval (datetime.timedelta): Direct reference to `interval` argument for Meteomatics' `query_time_series()`;
            defaults to a value representing 24 hour intervals for daily weather data.

    Returns:
        df_wx (pandas.DataFrame): Dataframe containing returned Meteomatics data for desired spatiotemporal AOI and parameters
        request (dict): Updated `request` dict with request metadata (i.e., "status", "request_seconds", "date_requested")
    """
    rq_keys = ["coordinate_list", "startdate", "enddate", "parameters"]
    assert all(
        [(key in request.keys()) for key in rq_keys]
    ), f"`request` must contain the following keys: {rq_keys}"

    msg = "Passed coordinate pair has not been rounded to 5th decimal place. Please re-evaluate requested points."
    assert all(
        [check_coordinate_rounding(tup) for tup in request["coordinate_list"]]
    ), msg

    n_requests_remaining = get_n_requests_remaining()
    assert n_requests_remaining > 0, "Meteomatics request limit reached for today."

    date_requested = datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S%z")
    request["date_requested"] = date_requested
    time_0 = time()

    try:
        df_wx = query_time_series(
            coordinate_list=request["coordinate_list"],
            startdate=request["startdate"],
            enddate=request["enddate"],
            interval=interval,
            parameters=request["parameters"],
            username="sentera",
            password=getenv("METEOMATICS_KEY"),
            request_type="GET",
            on_invalid="fill_with_invalid",
        )
        df_wx.insert(df_wx.shape[1], "date_requested", date_requested)

        time_1 = time()
        request["status"] = "SUCCESS"
        request["request_seconds"] = round(time_1 - time_0, 2)

    except ReadTimeout:
        time_1 = time()
        request["status"] = "FAIL"
        request["request_seconds"] = round(time_1 - time_0, 2)
        df_wx = None

    except NotFound:
        time_1 = time()
        request["status"] = "FAIL"
        request["request_seconds"] = -100
        df_wx = None

    return df_wx, request
