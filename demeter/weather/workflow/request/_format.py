"""Helper functions for formatting Meteomatics `request` dictionaries based on `gdf` rows"""
from datetime import datetime, timezone
from typing import List, Tuple

from geopandas import GeoDataFrame
from pytz import UTC


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
        coordinate_list += [(row["centroid"].y, row["centroid"].x)]

    # Get time
    startdate, enddate = get_utc_date_range_with_offset(
        utc_offset=utc_offset, first_day=first_day.date(), last_day=last_day.date()
    )

    # Create dict
    request = {
        "zone": int(utm_zone),
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
