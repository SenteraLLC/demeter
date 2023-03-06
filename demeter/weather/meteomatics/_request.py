"""Helper functions for creating and submitting Meteomatics requests based on given spatiotemporal information."""

from datetime import (
    datetime,
    timedelta,
    timezone,
)
from os import getenv
from time import time
from typing import (
    List,
    Tuple,
    Union,
)

from geopandas import GeoDataFrame
from meteomatics.api import query_time_series
from meteomatics.exceptions import NotFound
from pandas import DataFrame
from pytz import UTC
from requests import ReadTimeout


def get_utc_date_range_with_offset(
    utc_offset: timezone, first_day: datetime, last_day: datetime
) -> Tuple[datetime, datetime]:
    """
    Gets UTC start and end dates for a daily Meteomatics request based on given date range and a UTC offset.

    Since Meteomatics summarized weather parameters (i.e., "t_max_2m_24h:C") are right bounded,
    data will be extracted for each time point for the previous 24 hours. This means that we have
    to specify the end of day for both the start and end date in order to capture the appropriate data
    (https://www.meteomatics.com/en/api/request/required-parameters/date-time-description/).

    Args:
        utc_offset (datetime.timezone): UTC offset for needed UTM zone
        first_day (datetime.datetime): First local day for which weather data is needed
        last_day (datetime.datetime): Last local day for which weather data is needed

    Returns two datetime objects which represent the local EOD for the first and last day
    (localized by `utc_offset`) in UTC time.
    """

    # get eod day with offset as time zone and then convert to UTC
    eod_start = datetime(
        year=first_day.year,
        month=first_day.month,
        day=first_day.day,
        hour=23,
        minute=59,
        second=59,
        tzinfo=utc_offset,
    ).astimezone(UTC)

    eod_last = datetime(
        year=last_day.year,
        month=last_day.month,
        day=last_day.day,
        hour=23,
        minute=59,
        second=59,
        tzinfo=utc_offset,
    ).astimezone(UTC)
    startdate = eod_start
    enddate = eod_last
    return startdate, enddate


def get_n_pts_requested(gdf_request: GeoDataFrame, n_params: int):
    """
    Calculate the number of points to be requested for `gdf` and `n_params`.

    This function determines temporal coverage for spatial coordinates in `gdf_request` as
    the maximum `date_last` and minimum `date_first` across all rows in the dataframe.

    Args:
        gdf_request (GeoDataFrame): GeoDataFrame containing cell ID x date requests
        n_params (int): Number of parameters to be extracted for these cell and date requests
    """

    max_n_dates = (
        gdf_request["date_last"].max() - gdf_request["date_first"].min()
    ).days

    return n_params * max_n_dates * len(gdf_request)


def get_request_for_single_gdf(
    gdf_request: GeoDataFrame,
    utc_offset: timezone,
    utm_zone: int,
    parameters: List[str],
    utm_request_id: int = 0,
) -> dict:
    """Organizes spatiotemporal information for one Meteomatics request based on passed `gdf_request`.

    Requests will be created to extract all of the listed `parameters` and dates will be localized based on `utc_offset`.

    NOTE: This function assumes the information in `gdf_request` can be extracted in one request and does no splitting.

    Args:
        gdf_request (GeoDataFrame): GeoDataFrame containing spatiotemporal information for MM request.
        utm_zone (int): UTM zone in which included cell IDs are located.
        utc_offset (datetime.timezone): UTC offset for given UTM zone.
        parameters (list of str): List of parameters to be extracted within this request list.
        utm_request_id (int): Request index if this request is in a series of requests; defaults to 0.

    Returns:
        request (dict): Request metadata to drive a single Meteomatics request
    """

    rq_cols = ["centroid", "date_first", "date_last"]
    assert set(rq_cols).issubset(
        gdf_request.columns
    ), "Columns 'centroid', 'date_first', and 'date_last' must be in `gdf`"

    assert (
        len(parameters) <= 10
    ), "Meteomatics requests cannot contain more than 10 parameters."

    first_day = gdf_request["date_first"].min()
    last_day = gdf_request["date_last"].max()

    # Get space
    coordinate_list = []
    for _, row in gdf_request.iterrows():
        coordinate_list += [(round(row["centroid"].y, 5), round(row["centroid"].x, 5))]

    # Get time
    startdate, enddate = get_utc_date_range_with_offset(
        utc_offset=utc_offset, first_day=first_day.date(), last_day=last_day.date()
    )

    # Create dict
    request = {
        "zone": utm_zone,
        "utm_request_id": utm_request_id,
        "utc_offset": utc_offset,
        "n_pts_requested": get_n_pts_requested(gdf_request, len(parameters)),
        "coordinate_list": coordinate_list,
        "startdate": startdate,
        "enddate": enddate,
        "parameters": parameters,
    }

    return request


def get_request_list_from_gdfs_list(
    list_gdfs: List[GeoDataFrame],
    parameters: List[str],
) -> List[dict]:
    """
    Creates list of `request` dictionaries which can be passed to `make_meteomatics_request()` for each GDF in `list_gdfs`.

    Uses `get_request_for_single_gdf()` for each `gdf` in `list_gdfs`.

    Args:
        list_gdfs (list of GeoDataFrame): List containing each of the GeoDataFrame slices to be individually requested.
        parameters (list of str): List of parameters to be extracted within this request list.

    Returns:
        request_list (list of dict): List of dictionaries containing metadata to inform MM requests for
        each item in `list_gdfs`
    """
    msg = "Error: More than one `utc_offset` given in `gdf`. Should only be passing one per request."
    request_list = []

    for n in range(len(list_gdfs)):
        gdf_request = list_gdfs[n]

        utc_offset = list(gdf_request["utc_offset"].unique())
        utm_zone = list(gdf_request["utm_zone"].unique())

        assert len(utc_offset) == 1, msg

        rq = get_request_for_single_gdf(
            gdf_request, utc_offset[0].tzinfo, utm_zone[0], parameters, utm_request_id=n
        )
        request_list += [rq]

    return request_list


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


# def split_request_algorithm(
#     gdf: GeoDataFrame, data_pts_threshold: int, n_params: int
# ) -> Tuple[int, int, List[GeoDataFrame]]:
#     """
#     Splits up `gdf` into blocks of, at most, `max_n_spatial_pts`.

#     This algorithm orders `gdf` by "date_first" so that the rows are listed from most to least days needed.
#     Then, the algorithm determines the number of rows (i.e., cell IDs) to include in each request group
#     given that all requests must have less than `data_pts_threshold` points and all included cell IDs will
#     have data extracted for the same days as that needed by the first row in the group. The algorithm continues
#     to "slice" `gdf` until all of the rows are in a request group.

#     NOTE: This approach requires that all rows have the same "date_last". Other approaches might be better
#     suited for optimizing request number when "date_last" can vary.

#     Args:
#         gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests.
#         data_pts_threshold (int): Maximum number of pts to be included in a given request
#         n_params (int): Number of parameters to be extracted for each cell ID x date combination.

#     Returns:
#         n_pts_requested (int): Total number of points requested across all requests
#         n_splits (int): Number of splits needed.
#         list_gdf_splits (list of GeoDataFrame): List containing each of the GeoDataFrame slices
#     """

#     n_pts_requested = 0
#     n_pts_wasted = 0
#     n_splits = 0

#     list_gdf_splits = []
#     gdf_remaining = gdf.copy().sort_values("date_first").reset_index(drop=True)
#     gdf_remaining["n_dates"] = gdf_remaining.apply(
#         lambda row: (row["date_last"] - row["date_first"]).days + 1, axis=1
#     )

#     while len(gdf_remaining) > 0:
#         max_n_dates = gdf_remaining["n_dates"].max()
#         n_pts_per_cell_id = max_n_dates * n_params
#         n_rows = int(floor(data_pts_threshold / n_pts_per_cell_id))

#         n_pts_requested += n_pts_per_cell_id * n_rows
#         n_pts_wasted += sum(max_n_dates - gdf_remaining.iloc[:n_rows]["n_dates"])
#         n_splits += 1

#         list_gdf_splits += [gdf_remaining.iloc[:n_rows]]

#         gdf_remaining = gdf_remaining.iloc[n_rows:]

#     return n_pts_requested, n_splits, list_gdf_splits


# def split_gdf_for_meteomatics_request_list(
#     gdf: GeoDataFrame, data_pts_threshold: int, parameters: List[str]
# ) -> List[GeoDataFrame]:
#     """
#     Splits up `gdf` using `split_requests_algorithm()` in two ways and chooses optimal split to reduce request waste.

#     A two-step approach is used here to ensure that we are minimizing the average number of points requested
#     across all requests organized in this function. In the case that the last split has very few data points
#     after the first split, the second split attempts to better split up the data to reduce the chance of request
#     time out.

#     NOTE: Currently, this function is really only useful when "date_last" is consistent across all spatial coordinates,
#     which is the case when first populating the database for a cell ID or when performing daily updates. A different
#     approach for optimizing MM requests should be used when filling weather gaps. This could be specified with an
#     argument like `optimization_fx` or maybe just a completely different function?


#     """

#     # Accumulate cell ID rows until user-passed pt threshold is met for each split
#     n_pts_requested, n_splits, list_gdfs_1 = split_request_algorithm(
#         gdf, data_pts_threshold, len(parameters)
#     )

#     # Determine average number of pts requested per split
#     reduced_data_pts_threshold = int(ceil(n_pts_requested / n_splits))

#     # Then, re-complete optimization with smaller number of pts to see if overall request size can be reduced.
#     _, n_splits_2, list_gdfs_2 = split_request_algorithm(
#         gdf, reduced_data_pts_threshold, len(parameters)
#     )

#     # Determine the better split based on request number
#     if n_splits < n_splits_2:
#         print("Using the first split.")
#         list_gdfs = list_gdfs_1
#     else:
#         print("Using the second split.")
#         list_gdfs = list_gdfs_2

#     return list_gdfs
