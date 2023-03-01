from typing import List

from geopandas import GeoDataFrame
from sqlalchemy.engine import Connection

from demeter.weather.meteomatics._insert import (
    clean_meteomatics_data,
    get_weather_type_id_from_db,
    insert_daily_weather,
    log_meteomatics_request,
)
from demeter.weather.meteomatics._requests import (
    get_meteomatics_request_list,
    split_gdf_for_meteomatics_request_list,
    submit_single_meteomatics_request,
)


def attempt_and_maybe_insert_meteomatics_request(
    conn: Connection,
    request: dict,
    gdf_cell_id: GeoDataFrame,
    params_to_weather_types: dict,
):
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
        df_clean["date_requested"] = request["date_requested"]
        insert_daily_weather(conn, df_clean)


# TODO: This should return failed requests?
# TODO: How should we handle duplicate data? Due to errors in making `gdf` or due to structure of the optimized request?
def organize_and_process_meteomatics_requests(
    conn: Connection,
    gdf: GeoDataFrame,
    parameters: List[str] = None,
    parameter_sets: List[List[str]] = None,
    n_pts_max: int = None,
    n_pts_max_set: List[int] = None,
    default_n_pts_max: int = 50000,
    parallel: bool = False,
):
    """Takes `gdf` and information on desired parameters and organizes and processes MM requests to extract data.

    Args:
        conn (Connection): Connection to demeter weather database

        gdf (GeoDataFrame): GeoDataframe containing information on the spatiotemporal dimensions of the MM
            data needed; must contain the following columns: "utm_zone", "utc_offset", "world_utm_id", "cell_id",
            "centroid","date_first","date_last".

        parameter_sets (list of list of str): List of parameter sets to extract.

        n_pts_max_set (list of int): Corresponding with the order of `parameter_sets`, maximum number of
            points to be extracted for any given request with a given parameter set. This is necessary because
            different parameters can have different request times.

        n_pts_max (int): Maximum number of points to be extracted for any given request; this value is not used
            if `n_pts_max_set` is set.

        parameters (list of str): Full list of parameter names to extract with length <= 10; this parameter
            is not used if `parameter_sets` is set.

        default_n_pts_max (int): If `n_pts_max` and `n_pts_max_set` are not set, this value will be used
            as the maximum value of points requested for all parameter sets. Defaults to 50000.
    """

    # check some assumptions
    if parameter_sets is not None:
        msg = "All sublists in `parameter_sets` must have length <= 10."
        assert any([len(sublist) <= 10] for sublist in parameter_sets), msg
        parameters = [elem for sublist in parameter_sets for elem in sublist]
    else:
        assert parameters is not None, "Must set `parameters` or `parameter_sets`"
        msg = "Cannot have more than 10 parameters in a request. Use `parameter_sets` argument to specify breaks in list."
        assert len(parameters) <= 10, msg
        parameter_sets = [parameters]

    if n_pts_max is not None:
        n_pts_max_set = [n_pts_max] * len(parameter_sets)
    elif n_pts_max_set is None:
        n_pts_max_set = [default_n_pts_max] * len(parameter_sets)
    else:
        msg = "`n_pts_max_set` and `parameter_set` must be the same length"
        assert len(n_pts_max_set) == len(parameter_sets), msg

    # make sure all of the columns are present
    rq_cols = [
        "utm_zone",
        "utc_offset",
        "world_utm_id",
        "cell_id",
        "centroid",
        "date_first",
        "date_last",
    ]
    assert set(rq_cols).issubset(
        gdf.columns
    ), f"The following columns must be in `gdf`: {rq_cols}"

    # TODO: Make into argument and implement `pool` function
    parallel = False

    # Get information on parameters from DB and checks that they exist there
    cursor = conn.connection.cursor()
    params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

    # SPLIT #1: UTM zone due to `utc_offset` problem
    df_zones = gdf[["utm_zone", "utc_offset"]].drop_duplicates()
    request_list = []
    for _, row in df_zones.iterrows():
        gdf_zone_subset = gdf.loc[gdf["utm_zone"] == row["utm_zone"]]
        utc_offset = row["utc_offset"].tzinfo

        # SPLIT #2: parameters due to MM request limiting to 10 parameters
        for i in range(len(parameter_sets)):
            this_parameter_set = parameter_sets[i]
            this_n_pts_max = n_pts_max_set[0]

            # SPLIT #3: split along time to maximize spatial dimension
            list_gdfs = split_gdf_for_meteomatics_request_list(
                gdf=gdf_zone_subset,
                data_pts_threshold=this_n_pts_max,
                parameters=this_parameter_set,
            )

            request_list += get_meteomatics_request_list(
                list_gdfs=list_gdfs,
                utm_zone=row["utm_zone"],
                utc_offset=utc_offset,
                parameters=this_parameter_set,
            )

    # organize cell ID information with lat and lon to connect MM info to weather network
    gdf_cell_id = gdf[["world_utm_id", "cell_id", "centroid"]]
    gdf_cell_id.insert(0, "lon", round(gdf_cell_id.geometry.x, 5))
    gdf_cell_id.insert(0, "lat", round(gdf_cell_id.geometry.y, 5))

    # perform requests
    if parallel:
        pass
    else:
        for ind in range(len(request_list)):
            request = request_list[ind]
            attempt_and_maybe_insert_meteomatics_request(
                conn=conn,
                request=request,
                gdf_cell_id=gdf_cell_id,
                params_to_weather_types=params_to_weather_types,
            )
