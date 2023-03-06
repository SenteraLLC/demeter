"""Higher-level functions to help organize, submit, and store Meteomatics requests and any extracted data."""

from typing import List

from geopandas import GeoDataFrame
from sqlalchemy.engine import Connection

from demeter.weather.meteomatics._insert import (
    clean_meteomatics_data,
    insert_daily_weather,
    log_meteomatics_request,
)
from demeter.weather.meteomatics._request import (
    get_request_list_from_gdfs_list,
    submit_single_meteomatics_request,
)
from demeter.weather.meteomatics._split import (
    split_by_n_cells,
    split_by_utm_zone,
    split_by_year,
)


def attempt_and_maybe_insert_meteomatics_request(
    conn: Connection,
    request: dict,
    gdf_cell_id: GeoDataFrame,
    params_to_weather_types: dict,
) -> dict:
    """Submits MM request based on `request`, logs metadata to db, and, if successful, inserts data to db.

    Args:
        conn (Connection): Connection to demeter weather database
        request (dict): Information for outlining Meteomatics request
        gdf_cell_id (GeoDataFrame): Dataframe matching lat/lon coordinates to cell IDs
        params_to_weather_types (dict): Dictionary mapping weather parameters to db weather types IDs
    """

    df_wx, request = submit_single_meteomatics_request(request=request)

    # insert into database
    log_meteomatics_request(conn, request)

    # if request was successful, add to database
    if request["status"] == "SUCCESS":
        utc_offset = request["utc_offset"]
        df_clean = clean_meteomatics_data(
            df_wx=df_wx,
            gdf_cell_id=gdf_cell_id,
            utc_offset=utc_offset,
        )
        df_clean["weather_type_id"] = df_clean["variable"].map(params_to_weather_types)
        insert_daily_weather(conn, df_clean)

    return request


def split_gdf_for_add_step(
    gdf: GeoDataFrame,
    parameter_sets: List[List[str]],
    n_cells_per_set: List[int],
    n_forecast_days: int = 7,
):
    """Creates `request_list` that will fully populate Demeter with weather for new cell IDS specified in `gdf`.

    This splitting strategy should be used only to extract weather for new cell IDs since such requests are often
    have a much higher number of time points (i.e., ~11 years of data per cell ID).

    The strategy functions as follows:
    1. Split by parameter sets as specified in `parameter_sets`. This is required because MM only allows one
    to request 10 parameters at a time. This also allows us to more easily customize how big requests get depending on
    the parameter, which is important because request time depends on parameter.

    2. Split by UTM zone since we cannot differentiate start and end dates within one request and each zone has a different
    offset for end of day.

    3. Split by year. This approach was decided to help make the request process simpler in the absence of sufficient data
    for understanding how request time changes across parameter x time x space.

    4. Split by number of cell IDS. Using previous data for yearly extractions for each parameter set, we can gain a better
    understanding of how many cell IDs can be included in each request with a lower risk of time out. Depending on the number
    of cell IDs, this is not always necessary.

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame of cell ID x date range informaton for MM requests.
        parameter_sets (list of str): List of sets of parameters to be extracted for each cell ID x date combination.

        n_cells_per_set (list of int): Maximum number of cell IDs to include in a request when requesting a
            certain parameter set (corresponds to order of `parameter_sets`).

        n_forecast_days (int): Number of days of forecast weather to collect for each cell ID

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


# def organize_and_process_meteomatics_requests(
#     conn: Connection,
#     gdf: GeoDataFrame,
#     step: str,
#     parameters: List[str] = None,
#     parameter_sets: List[List[str]] = None,
#     n_cells_max: int = None,
#     n_cells_max_set: List[int] = None,
#     parallel: bool = False,
# ) -> List[dict]:
#     """Takes `gdf` and information on desired parameters and organizes and processes MM requests to extract data based on the `step`.

#     `step` affects which splitting function is used to create `request_list` and, thus, controls how the function organizes
#     and optimizes data needs. These steps include:
#     1. "add": Initializes a cell ID in the database; often has the largest number of data pts and creates the chunkiest requests.

#     **NOT IMPLEMENTED YET**
#     2. "update": Update all existing cell IDs in the database with most up-to-date recent historical (past 2 days) and forecast
#     data.
#     3. "fill": Fill in gaps in the weather data following a full inventory.

#     Args:
#         conn (Connection): Connection to demeter weather database

#         gdf (GeoDataFrame): GeoDataframe containing information on the spatiotemporal dimensions of the MM
#             data needed; must contain the following columns: "utm_zone", "utc_offset", "world_utm_id", "cell_id",
#             "centroid","date_first","date_last".

#         step (int): Indicates which step in the weather data extraction process is being completed; can be: "add",
#             "update", or "fill".

#         parameter_sets (list of list of str): List of parameter sets to extract.

#         n_cells_max_set (list of int): Corresponding with the order of `parameter_sets`, maximum number of
#             cell IDs that shoudl be extracted for a given request with a given parameter set. This is necessary because
#             different parameters can have different request times.

#         n_cells_max (int): Maximum number of cell IDs to be extracted for any given request; this value is not used
#             if `n_pts_max_set` is set.

#         parameters (list of str): Full list of parameter names to extract with length <= 10; this parameter
#             is not used if `parameter_sets` is set.

#         default_n_pts_max (int): If `n_pts_max` and `n_pts_max_set` are not set, this value will be used
#             as the maximum value of points requested for all parameter sets. Defaults to 50000.
#     """

#     # check some assumptions
#     if parameter_sets is not None:
#         msg = "All sublists in `parameter_sets` must have length <= 10."
#         assert any([len(sublist) <= 10] for sublist in parameter_sets), msg
#         parameters = [elem for sublist in parameter_sets for elem in sublist]
#     else:
#         assert parameters is not None, "Must set `parameters` or `parameter_sets`"
#         msg = "Cannot have more than 10 parameters in a request. Use `parameter_sets` argument to specify breaks in list."
#         assert len(parameters) <= 10, msg
#         parameter_sets = [parameters]

#     if n_cells_max is not None:
#         n_cells_max_set = [n_cells_max] * len(parameter_sets)
#     else:
#         assert (
#             n_cells_max_set is not None
#         ), "Must set `n_cells_max` or `n_cells_max_set`"
#         msg = "`n_pts_max_set` and `parameter_set` must be the same length"
#         assert len(n_cells_max_set) == len(parameter_sets), msg

#     # make sure all of the columns are present
#     rq_cols = [
#         "utm_zone",
#         "utc_offset",
#         "world_utm_id",
#         "cell_id",
#         "centroid",
#         "date_first",
#         "date_last",
#     ]
#     assert set(rq_cols).issubset(
#         gdf.columns
#     ), f"The following columns must be in `gdf`: {rq_cols}"

#     assert step in ["add", "update", "fill"], "Step must be 'add','update',or 'fill"

#     # TODO: Make into argument and implement `pool` function
#     parallel = False

#     # Get information on parameters from DB and checks that they exist there
#     cursor = conn.connection.cursor()
#     params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

#     # Split requests based on step
#     if step == "add":
#         request_list = split_gdf_for_add_step(gdf, parameter_sets, n_cells_max_set)
#     else:
#         pass

#     # organize cell ID information with lat and lon to connect MM info to weather network
#     gdf_cell_id = gdf[["world_utm_id", "cell_id", "centroid"]]
#     gdf_cell_id.insert(0, "lon", gdf_cell_id.geometry.x)
#     gdf_cell_id.insert(0, "lat", gdf_cell_id.geometry.y)

#     # perform requests
#     if parallel:
#         pass
#     else:
#         for ind in range(len(request_list)):
#             request = request_list[ind]
#             request = attempt_and_maybe_insert_meteomatics_request(
#                 conn=conn,
#                 request=request,
#                 gdf_cell_id=gdf_cell_id,
#                 params_to_weather_types=params_to_weather_types,
#             )
#             request_list[ind] = request

#     return request_list
