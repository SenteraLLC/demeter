"""Daily add/update of Demeter weather database based on Demeter field data.

This is the working draft of a script that would execute on daily weather extraction for `Demeter`.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Run "add" on those cell ID x year combinations which are new
"""
# %% imports
from dotenv import load_dotenv
from pandas import Series

from demeter.db import getConnection
from demeter.weather.inventory import get_gdf_for_add, get_gdf_for_update
from demeter.weather.meteomatics import (
    cut_request_list_along_utm_zone,
    get_n_requests_remaining,
    get_weather_type_id_from_db,
    run_requests,
    split_gdf_for_add,
    split_gdf_for_update,
)
from demeter.weather.schema.weather_types import DAILY_WEATHER_TYPES

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

# `n_cells_per_set` is a user lever to control for parameter variability in request time
n_cells_per_set = [
    1000,
    1000,
]

# get the cell ID x date information for where we need data
# this information is derived based on which cell IDs are populated in `daily` and the last `date_requested` for each cell ID
gdf_update = get_gdf_for_update(conn)

# TODO: Should `get_gdf_for_update` take the most recent request date into account when deciding to
# update a cell ID? If I run the "update" step and then immediately re-run it, it will try to update again.

if gdf_update is not None:
    update_requests = split_gdf_for_update(gdf_update, parameter_sets, n_cells_per_set)
else:
    update_requests = []

# %%  2. Prepare "add_requests" list which gets new cell ID x year combinations for `demeter.fields` that do not yet exist in `weather`
n_cells_per_set = [100, 100]

# get the cell ID x date information for where we need data
# this information is derived based on `demeter` (field location and planting dates) and already available data in `weather`
gdf_add = get_gdf_for_add(conn)

if gdf_add is not None:
    add_requests = split_gdf_for_add(gdf_add, parameter_sets, n_cells_per_set)
else:
    add_requests = []

# %% 3. Actually complete the requests: start with "update" and then do "add"

# I spent way too long thinking about the order in which we should be completing requests
# across these two steps (i.e., "add" and "update"). Realistically, I'm not sure we will
# have to deal with this problem very often. However, I think it's worth thinking about.
# The central question is: On a given day, if we have to choose between the "add" and the
# "update", which one is more important to do?

# ensure we are tracking the number of requests we are using and that are remaining
n_requests_demeter = (
    2500  # our team's self-imposed maximum daily usage out of 5000 hard limit
)
n_requests_limit = get_n_requests_remaining()

# `n_requests` represents the total number of requests that we will allow ourselves to run today
n_requests = min([n_requests_limit, n_requests_demeter])

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

update_requests = cut_request_list_along_utm_zone(update_requests, n_requests)
n_requests_used = len(update_requests)

# run UPDATE requests
if len(update_requests) > 0:
    update_requests = run_requests(
        conn,
        request_list=update_requests,
        gdf_request=gdf_update,
        params_to_weather_types=params_to_weather_types,
    )

    # TODO: Re-try failed requests if relevant; add to `n_requests_used`
    Series(data=[r["status"] for r in update_requests]).value_counts()

# run ADD requests
n_requests_remaining = n_requests - n_requests_used
if n_requests_remaining > 0 and len(add_requests) > 0:
    add_requests = cut_request_list_along_utm_zone(add_requests, n_requests_remaining)

    n_requests_used += len(add_requests)

    add_requests = run_requests(
        conn,
        request_list=add_requests,
        gdf_request=gdf_add,
        params_to_weather_types=params_to_weather_types,
    )

    # TODO: Re-try failed requests if relevant; add to `n_requests_used`
    Series(data=[r["status"] for r in add_requests]).value_counts()
