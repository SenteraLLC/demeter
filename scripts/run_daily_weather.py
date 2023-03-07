"""Daily add/update of Demeter weather database based on Demeter field data.

This is the working draft of a script that would execute on daily weather extraction for `Demeter`.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Run "add" on those cell ID x year combinations which are new
"""
# TODO: Make `demeter_user` able to search `weather` schema.
# %% imports
from dotenv import load_dotenv

from demeter.db import getConnection
from demeter.weather.inventory.main import get_gdf_for_add, get_gdf_for_update
from demeter.weather.meteomatics._insert import get_weather_type_id_from_db
from demeter.weather.meteomatics.main import (
    run_requests,
    split_gdf_for_add,
    split_gdf_for_update,
)
from demeter.weather.schema.weather_types import DAILY_WEATHER_TYPES

# %% Create connections to databases
c = load_dotenv()
conn_weather = getConnection(env_name="DEMETER-DEV_LOCAL_WEATHER")
cursor_weather = conn_weather.connection.cursor()

conn_demeter = getConnection(env_name="DEMETER-DEV_LOCAL")
cursor_demeter = conn_demeter.connection.cursor()

# %% Organize parameter sets

# wind gusts has been removed for now to avoid problems
# we use `n_cells_per_set` to control for parameter variability in request time
full_parameters = [weather_type[0] for weather_type in DAILY_WEATHER_TYPES]
parameter_sets = [full_parameters[:6], full_parameters[6:]]
parameters = [elem for sublist in parameter_sets for elem in sublist]

# TODO: Make into argument and implement `pool` function
parallel = False

# get information on parameters from DB and checks that they exist there
params_to_weather_types = get_weather_type_id_from_db(cursor_weather, parameters)

# %% 1. Prepare "update" GDF which is based on those cell IDs already in demeter
n_cells_per_set = [1000, 1000]
gdf_update = get_gdf_for_update(conn_weather)
update_requests = split_gdf_for_update(gdf_update, parameter_sets, n_cells_per_set)

# %%  2. Prepare "add" GDF which is based on new cell ID x year combinations
n_cells_per_set = [100, 100]
gdf_add = get_gdf_for_add(conn_demeter, conn_weather)
add_requests = split_gdf_for_add(gdf_add, parameter_sets, n_cells_per_set)

# TODO: This should inventory by year in case we have some weird split along cell ID x year
# combinations due to limited request number.

# %% 3. Actually complete the requests: start with "add" and then do "update"?

# TODO: Clean up dataframe before inserting so it is not inserting any potentially duplicate data

# If the number of requests for "update" is greater than our share, we want to prioritize
# the "add" step first, given that "update" overwrites already existing data in the database.
n_requests_demeter = 2500
if len(add_requests) > n_requests_demeter:
    add_requests = add_requests[:n_requests_demeter]

n_requests_used = len(add_requests)

# run ADD requests
gdf_cell_id = gdf_add[["world_utm_id", "cell_id", "centroid"]]
gdf_cell_id.insert(0, "lon", gdf_cell_id.geometry.x)
gdf_cell_id.insert(0, "lat", gdf_cell_id.geometry.y)
run_requests(conn_weather, request_list=add_requests, gdf_cell_id=gdf_cell_id)

# run UPDATE requests
n_requests_remaining = n_requests_demeter - n_requests_used
if n_requests_remaining > 0:
    update_requests = update_requests[:n_requests_remaining]

    n_requests_used += len(update_requests)

    gdf_cell_id = gdf_update[["world_utm_id", "cell_id", "centroid"]]
    gdf_cell_id.insert(0, "lon", gdf_cell_id.geometry.x)
    gdf_cell_id.insert(0, "lat", gdf_cell_id.geometry.y)
    run_requests(conn_weather, request_list=update_requests, gdf_cell_id=gdf_cell_id)
