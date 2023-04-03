"""High-level functions to help organize Meteomatics requests for submission based on passed `gdf`."""

from typing import List

from geopandas import GeoDataFrame
from numpy import ceil

from ..request._format import get_request_list_from_gdfs_list
from ._split_utils import (
    split_by_n_cells,
    split_by_utm_zone,
    split_by_year,
    split_by_year_for_fill,
)


def split_gdf_for_update(
    gdf: GeoDataFrame,
    parameter_sets: List[List[str]],
    n_cells_per_set: List[int],
):
    """
    Creates `request_list` for extracting weather data needed to update the database with recent data as given in `gdf`.

    This splitting strategy should be used only for "update" data requests, because it assumes each cell ID requires a
    short time series (e.g. 1-3 weeks). As a consequence, no splitting is done along the temporal dimension.

    The splitting strategy at this step is as follows:
    1. Split by parameter sets as specified in `parameter_sets`. This is required because MM only allows 10 parameters
    to be requested at a time. This also allows us to more easily customize how big requests get depending on the
    parameter, which is important because request time depends on parameter.

    2. Split by UTM zone since we cannot differentiate start and end dates within one request and each zone has a
    different offset for end of day.

    3. Split by number of cell IDs. This is not always necessary since we can extract many more cell IDs per UTM request
    with fewer time points per cell ID.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests.
        parameter_sets (list of str): List of sets of parameters to be extracted for each cell ID x date combination.

        n_cells_per_set (list of int): Maximum number of cell IDs to include in a request when requesting a
            certain parameter set (corresponds to order of `parameter_sets`).

    Returns:
        request_list (list of dict): List containing a dictionary for each request required to get data for `gdf`
        under the specified split protocol.
    """

    gdf_split_utm = split_by_utm_zone(gdf)

    # iterate over parameter sets
    request_list = []
    for i in range(len(parameter_sets)):
        this_parameter_set = parameter_sets[i]
        this_n_cells_max = n_cells_per_set[i]

        gdf_split_cells = [
            split_by_n_cells(this_gdf, this_n_cells_max) for this_gdf in gdf_split_utm
        ]
        gdf_split_cells = [item for sublist in gdf_split_cells for item in sublist]

        request_list += get_request_list_from_gdfs_list(
            list_gdfs=gdf_split_cells,
            parameters=this_parameter_set,
        )
    return request_list


def split_gdf_for_add(
    gdf: GeoDataFrame,
    parameter_sets: List[List[str]],
    n_cells_per_set: List[int],
    n_forecast_days: int = 7,
):
    """Creates `request_list` that will fully populate Demeter with weather for new cell ID X year combinations specified in `gdf`.

    This splitting strategy should be used only to extract weather for new cell IDs since such requests are often
    extracting entire years of data.

    The splitting strategy at this step is as follows:
    1. Split by parameter sets as specified in `parameter_sets`. This is required because MM only allows one
    to request 10 parameters at a time. This also allows us to more easily customize how big requests get depending on
    the parameter, which is important because request time depends on parameter.

    2. Split by UTM zone since we cannot differentiate start and end dates within one request and each zone has a different
    offset for end of day.

    3. Split by year. This approach was decided to help make the request process simpler in the absence of sufficient data
    for understanding how request time changes across parameter x time x space.

    4. Split by number of cell IDS. Using previous data for yearly extractions for each parameter set, we can gain a better
    understanding of how many cell IDs can be included in each UTM x year request with a lower risk of time out. Depending on
    the number of cell IDs, this is not always necessary.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests.
        parameter_sets (list of str): List of sets of parameters to be extracted for each cell ID x date combination.

        n_cells_per_set (list of int): Maximum number of cell IDs to include in a request when requesting a
            certain parameter set (corresponds to order of `parameter_sets`).

        n_forecast_days (int): Number of days of forecast weather to collect for each cell ID; defaults to 7 days.

    Returns:
        request_list (list of dict): List containing a dictionary for each request required to get data for `gdf`
        under the specified split protocol.
    """
    rq_cols = ["utm_zone", "utc_offset", "cell_id", "date_first", "date_last"]
    assert set(rq_cols).issubset(
        gdf.columns
    ), f"The following columns must be in `gdf`: {rq_cols}"

    # split by UTM zone
    gdf_split_utm = split_by_utm_zone(gdf)

    # split by year and flatten
    gdf_split_year = [
        split_by_year(this_gdf, n_forecast_days) for this_gdf in gdf_split_utm
    ]
    gdf_split_year = [item for sublist in gdf_split_year for item in sublist]

    # iterate over parameter sets
    request_list = []
    for i in range(len(parameter_sets)):
        this_parameter_set = parameter_sets[i]
        this_n_cells_max = n_cells_per_set[i]

        gdf_split_cells = [
            split_by_n_cells(this_gdf, this_n_cells_max) for this_gdf in gdf_split_year
        ]
        gdf_split_cells = [item for sublist in gdf_split_cells for item in sublist]

        request_list += get_request_list_from_gdfs_list(
            list_gdfs=gdf_split_cells,
            parameters=this_parameter_set,
        )

    return request_list


def split_gdf_for_fill(
    gdf: GeoDataFrame,
    n_cells_max: int = 100,
):
    """
    Creates `request_list` for extracting weather data needed to fill in database gaps as given in `gdf`.

    This splitting strategy imposes the following constraints on requests to keep things simple:

    1. UTM zones must be requested in separate requests to control for the "utc offset" problem.
    2. No more than 10 parameters can be requested in a given request.
    3. A single request must be constrained to getting data for one calendar year.
    4. No more than `n_cells_max` can be included in a given request.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date x parameter informaton for MM requests.
        n_cells_max (int): Maximum number of cell IDs to include in any single request.

    Returns:
        request_list (list of dict): List containing a dictionary for each request required to get data for `gdf`
        under the specified split protocol.
    """

    request_list = []

    # we always have to split by UTM because of UTC offset problem
    gdf_split_utm = split_by_utm_zone(
        gdf.rename(columns={"date": "date_first", "parameter": "date_last"})
    )

    gdf_split_utm = [
        this_gdf.rename(columns={"date_first": "date", "date_last": "parameter"})
        for this_gdf in gdf_split_utm
    ]

    # start loops within UTM zone GDFs to keep parameter lists minimal
    for gdf_utm_zone in gdf_split_utm:
        parameters = list(gdf_utm_zone["parameter"].unique())
        if len(parameters) > 10:
            n = int(ceil(len(parameters) / 10))
            parameter_sets = [parameters[i::n] for i in range(n)]
        else:
            parameter_sets = [parameters]

        gdf_split_year = split_by_year_for_fill(gdf_utm_zone)
        gdf_split_year = [
            this_gdf.drop_duplicates(["cell_id"]) for this_gdf in gdf_split_year
        ]

        gdf_split_cells = [
            split_by_n_cells(this_gdf, n_cells_max) for this_gdf in gdf_split_year
        ]
        gdf_split_cells = [item for sublist in gdf_split_cells for item in sublist]

        for pars in parameter_sets:
            request_list += get_request_list_from_gdfs_list(
                list_gdfs=gdf_split_cells,
                parameters=pars,
            )

    return request_list
