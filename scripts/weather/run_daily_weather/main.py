import logging
from typing import List

from sqlalchemy.engine import Connection

from demeter.weather.query import get_weather_type_id_from_db
from demeter.weather.workflow.inventory import (
    get_gdf_for_add,
    get_gdf_for_fill,
    get_gdf_for_update,
)
from demeter.weather.workflow.request import (
    get_n_requests_remaining_for_demeter,
    print_meteomatics_request_report,
    run_request_step,
)
from demeter.weather.workflow.split import (
    split_gdf_for_add,
    split_gdf_for_fill,
    split_gdf_for_update,
)

from .defaults import (
    N_CELLS_FILL,
    N_CELLS_PER_SET_ADD,
    N_CELLS_PER_SET_UPDATE,
    PARAMETER_SETS,
)


def run_daily_weather(
    conn: Connection,
    parameter_sets: List[List[str]] = PARAMETER_SETS,
    n_cells_per_set_add: List[int] = N_CELLS_PER_SET_ADD,
    n_cells_per_set_update: List[int] = N_CELLS_PER_SET_UPDATE,
    n_cells_fill: int = N_CELLS_FILL,
    fill: bool = False,
    parallel: bool = False,
):
    """
    Perform weather processing steps to maintain `demeter.weather`.

    Each step involves the same 3 steps:
    - "get": Collected spatiotemporal information on what data is needed to complete this step.
    - "split": Splits the information and translates into practical Meteomatics requests.
    - "run": Submits, tracks, and inserts requests.

    Steps:
    (1) UPDATE: This step updates the database, such that all populated cell IDs have up-to-date
    stable historical and forecast data. The data requested in this step is derived based on which
    cell IDs are populated in `daily` and the last `date_requested` for each cell ID.

    (2) ADD: This step adds data for any cell ID which is needed to represent a planted field in
    `demeter` but is not yet in the weather database OR a cell ID x year combination needed for a new
    planted field in demeter whose cell ID is already in the database. This information is derived
    based on `demeter` (field location and planting dates) and already available data in `weather`.

    (3) FILL: (optional) This step performs an exhaustive inventory to find data gaps in the weather
    database based on those cell IDs populated in the database as well as cell IDs x dates need for `demeter`.

    Args:
        conn (Connection): Connection to Demeter and weather database
        parameter_sets (list of list of str): List of parameter groupings to be requested together.

        n_cells_per_set_add (list of int): List of values to be passed to `split_by_n_cells()` as
            `max_n_cells` for each parameter grouping, respectively, in the ADD step

        n_cells_per_set_update (list of int): List of values to be passed to `split_by_n_cells()` as
            `max_n_cells` for each parameter grouping, respectively, in the UPDATE step

        n_cells_fill (int): Value to be passed to `split_by_n_cells()` as `max_n_cells` for the
            "fill" step.

        fill (bool): If "True", "fill" step is run after "update" and "add"; otherwise, it is not.

        parallel (bool): If "True", "run" step is run in parallel for all steps; otherwise, requests are
            run in series.
    """

    # get information on parameters from DB and check that they exist there (errors if they don't)
    cursor = conn.connection.cursor()
    parameters = [elem for sublist in parameter_sets for elem in sublist]
    params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

    # track all requests that are completed to create error report if needed
    all_completed_requests = []

    #### STEP 1: Run UPDATE requests ####
    logging.info("UPDATING")
    n_requests_available = get_n_requests_remaining_for_demeter(conn)
    if n_requests_available > 0:
        gdf_update = get_gdf_for_update(conn)

        if len(gdf_update) > 0:
            update_requests = split_gdf_for_update(
                gdf_update, parameter_sets, n_cells_per_set_update
            )
            update_requests = run_request_step(
                conn,
                request_list=update_requests,
                gdf_request=gdf_update,
                params_to_weather_types=params_to_weather_types,
                parallel=parallel,
            )
            all_completed_requests += update_requests
        else:
            logging.info("Nothing to UPDATE.")
            update_requests = []
    else:
        logging.info("No more requests remaining to run UPDATE step.")

    #### STEP 2: Run ADD requests ####
    logging.info("ADDING")
    n_requests_available = get_n_requests_remaining_for_demeter(conn)
    if n_requests_available > 0:
        gdf_add = get_gdf_for_add(conn)

        if len(gdf_add) > 0:
            add_requests = split_gdf_for_add(
                gdf_add, parameter_sets, n_cells_per_set_add
            )
            add_requests = run_request_step(
                conn,
                request_list=add_requests,
                gdf_request=gdf_add,
                params_to_weather_types=params_to_weather_types,
                parallel=parallel,
            )
            all_completed_requests += update_requests
        else:
            logging.info("Nothing to ADD.")
            add_requests = []
    else:
        logging.info("No more requests remaining to run ADD step.")

    #### STEP 3. Run FILL step if needed ####
    if fill:
        logging.info("FILLING")
        n_requests_available = get_n_requests_remaining_for_demeter(conn)
        if n_requests_available > 0:
            gdf_fill = get_gdf_for_fill(conn)

            if len(gdf_fill) > 0:
                fill_requests = split_gdf_for_fill(gdf_fill, n_cells_max=n_cells_fill)
                fill_requests = run_request_step(
                    conn,
                    request_list=fill_requests,
                    gdf_request=gdf_fill,
                    params_to_weather_types=params_to_weather_types,
                    parallel=parallel,
                )
                all_completed_requests += update_requests
            else:
                logging.info("Nothing to FILL.")
                fill_requests = []
        else:
            logging.info("No more requests remaining to run FILL step.")

    #### STEP 4: Generate and print report
    print_meteomatics_request_report(all_completed_requests)
