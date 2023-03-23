"""Daily add/update of Demeter weather database based on Demeter field data.

This is the working draft of a script that would execute on daily weather extraction for `Demeter`.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Run "add" on those cell ID x year combinations which are new
"""
# %% imports
from dotenv import load_dotenv

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

# %% Organize parameter sets

# wind gusts has been removed for now to avoid problems
# we use `n_cells_per_set` to control for parameter variability in request time
full_parameters = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
parameter_sets = [full_parameters[:6], full_parameters[6:]]
parameters = [elem for sublist in parameter_sets for elem in sublist]

parallel = False

# get information on parameters from DB and checks that they exist there
params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

# %% 1. Prepare "update" GDF which is based on those cell IDs already in demeter
n_cells_per_set = [1000, 1000]
gdf_update = get_gdf_for_update(conn)

if gdf_update is not None:
    update_requests = split_gdf_for_update(gdf_update, parameter_sets, n_cells_per_set)
else:
    update_requests = []

# %%  2. Prepare "add" GDF which is based on new cell ID x year combinations
n_cells_per_set = [100, 100]
gdf_add = get_gdf_for_add(conn)
add_requests = split_gdf_for_add(gdf_add, parameter_sets, n_cells_per_set)

# %% 3. Actually complete the requests: start with "update" and then do "add"

n_requests_demeter = (
    2500  # our team's self-imposed maximum daily usage of 5000 hard limit
)
n_requests_limit = get_n_requests_remaining()

n_requests = min([n_requests_limit, n_requests_demeter])

# run UPDATE requests
if len(update_requests) > n_requests:
    update_requests = cut_request_list_along_utm_zone(update_requests, n_requests)

n_requests_used = len(update_requests)

if len(update_requests) > 0:
    update_requests = run_requests(
        conn,
        request_list=update_requests,
        gdf_request=gdf_update,
        params_to_weather_types=params_to_weather_types,
    )

# TODO: Re-try failed requests if relevant; add to `n_requests_used`

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
