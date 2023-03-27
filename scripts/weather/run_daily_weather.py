"""Daily add/update of Demeter weather database based on Demeter field data.

This is the working draft of a script that would execute on daily weather extraction for `Demeter`.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Run "add" on those cell ID x year combinations which are new
3. Run "fill" to fill in any gaps in the database
"""
# %% imports
from dotenv import load_dotenv
from pandas import Series

from demeter.db import getConnection
from demeter.weather.initialize.weather_types import DAILY_WEATHER_TYPES
from demeter.weather.insert import get_weather_type_id_from_db
from demeter.weather.inventory import (
    get_gdf_for_add,
    get_gdf_for_fill,
    get_gdf_for_update,
)
from demeter.weather.request import (
    cut_request_list_along_utm_zone,
    get_n_requests_remaining_for_demeter,
    submit_requests,
)
from demeter.weather.request.split import (
    split_gdf_for_add,
    split_gdf_for_fill,
    split_gdf_for_update,
)

# %% Create connections to databases
c = load_dotenv()

conn = getConnection(env_name="DEMETER-DEV_LOCAL")
cursor = conn.connection.cursor()

parallel = False  # not implemented

# %% Organize parameter sets

# This section is one of the sections that will be most impacted by future stories on parameters.
# We are keeping this part very basic (i.e., letting the user define how parameters are split up
# and how many cells should be considered for each parameter group) mostly because we have such a
# limited undertanding of how the API functions across space, time, and parameters. Currently, the
# workflow is operating with static parameter groups. There is no programmatic splitting of parameters
# when it comes to creating requests.

# wind gusts has been removed for now to avoid problems
full_parameters = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
parameter_sets = [full_parameters[:6], full_parameters[6:]]
parameters = [elem for sublist in parameter_sets for elem in sublist]

# get information on parameters from DB and check that they exist there (errors if they don't)
params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

# %% 1. Prepare "update_requests" list which will get updated data for recent historical and forecast dates


# %%  2. Prepare "add_requests" list which gets new cell ID x year combinations for `demeter.fields` that do not yet exist in `weather`


# %% 3. Actually complete the requests

# We extract "update" first because it is bound to have far fewer requests.

# Another important consideration that came up in this story was the resilience of the
# two inventory functions (`get_gdf_for_add` and `get_gdf_for_update`) in the case that we
# have enough remaining requests to do, say, half of the needed requests in a given day. Will
# the system be smart enough to catch those missing requests the next day?

# TODO: Even so, we need to test to make sure that no requests are blatantly left behind and the
# inventory functions are resilient.

# One very important (and simple) way to ensure missing requests are filled in later is by cutting
# the request list along UTM zones, such that we are either running all or none of the requests
# for a given UTM zone. That way, parameter groups are run in completion for a given cell ID x date.
# This is the purpose of the `cut_request_list_along_utm_zone()` function.

# %% Run UPDATE requests
n_requests_available = get_n_requests_remaining_for_demeter(conn)

# `n_cells_per_set` is a user lever to control for parameter variability in request time
n_cells_per_set = [1000, 1000]

if n_requests_available > 0:
    # get the cell ID x date information for where we need data
    # this information is derived based on which cell IDs are populated in `daily` and the last `date_requested` for each cell ID
    gdf_update = get_gdf_for_update(conn)

    if gdf_update is not None:
        update_requests = split_gdf_for_update(
            gdf_update, parameter_sets, n_cells_per_set
        )
        update_requests = cut_request_list_along_utm_zone(
            update_requests, n_requests_available
        )
    else:
        print("Nothing to UPDATE.")
        update_requests = []

    # run UPDATE requests
    if len(update_requests) > 0:
        update_requests = submit_requests(
            conn,
            request_list=update_requests,
            gdf_request=gdf_update,
            params_to_weather_types=params_to_weather_types,
        )

        # TODO: Re-try failed requests if relevant; add to `n_requests_used`
        Series(data=[r["status"] for r in update_requests]).value_counts()
    else:
        "No requests running for UPDATE step."

else:
    print("No more requests remaining to run UPDATE step.")


# %% Run ADD requests
n_requests_available = get_n_requests_remaining_for_demeter(conn)

n_cells_per_set = [100, 100]

if n_requests_available > 0:
    # get the cell ID x date information for where we need data
    # this information is derived based on `demeter` (field location and planting dates) and already available data in `weather`
    gdf_add = get_gdf_for_add(conn)

    if gdf_add is not None:
        add_requests = split_gdf_for_add(gdf_add, parameter_sets, n_cells_per_set)
        add_requests = cut_request_list_along_utm_zone(
            add_requests, n_requests_available
        )
    else:
        print("Nothing to ADD.")
        add_requests = []

    if len(add_requests) > 0:
        add_requests = submit_requests(
            conn,
            request_list=add_requests,
            gdf_request=gdf_add,
            params_to_weather_types=params_to_weather_types,
        )

        # TODO: Re-try failed requests if relevant; add to `n_requests_used`
        Series(data=[r["status"] for r in add_requests]).value_counts()
    else:
        "No requests running for ADD step."

else:
    print("No more requests remaining to run ADD step.")


# %% 4. Run FILL step if needed
n_requests_available = get_n_requests_remaining_for_demeter(conn)

if n_requests_available > 0:
    # perform exhaustive inventory to find data gaps
    gdf_fill = get_gdf_for_fill(conn)

    if len(gdf_fill) > 0:
        fill_requests = split_gdf_for_fill(gdf_fill)
        fill_requests = cut_request_list_along_utm_zone(
            fill_requests, n_requests_available
        )
    else:
        print("Nothing to FILL.")
        fill_requests = []

    if len(fill_requests) > 0:
        fill_requests = submit_requests(
            conn,
            request_list=fill_requests,
            gdf_request=gdf_fill,
            params_to_weather_types=params_to_weather_types,
        )

        # TODO: Re-try failed requests if relevant; add to `n_requests_used`
        Series(data=[r["status"] for r in fill_requests]).value_counts()
    else:
        "No requests running for FILL step."
else:
    print("No more requests remaining to run FILL step.")

# %%
