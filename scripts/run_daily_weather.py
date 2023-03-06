"""Daily add/update of Demeter weather database based on Demeter field data.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Determine all cell IDs and the first year of data available for each in `weather`
3. Get all cell IDs and first planting dates from `demeter` (** This step is not currently fully using Demeter's creation timestamps.)
4. Run "add" on those cell ID x year combinations which are new
"""
# TODO: Make `demeter_user` able to search `weather` schema.

from dotenv import load_dotenv

# %% imports
from demeter.db import getConnection
from demeter.weather.inventory._demeter_inventory import (
    get_spatiotemporal_weather_database_needs,
)
from demeter.weather.inventory._weather_inventory import (
    get_cell_ids_in_weather_table,
    get_first_data_year_for_cell_id,
)
from demeter.weather.inventory.main import update_weather
from demeter.weather.meteomatics.main import organize_and_process_meteomatics_requests

# %% Create connections to databases
c = load_dotenv()
conn_weather = getConnection(env_name="DEMETER-DEV_LOCAL_WEATHER")
cursor_weather = conn_weather.connection.cursor()

conn_demeter = getConnection(env_name="DEMETER-DEV_LOCAL")
cursor_demeter = conn_demeter.connection.cursor()

# WITHIN EACH STEP:
# - figure out cell ID, date_first, date_last
# - add centroid
# - split requests to be "optimal" for step
# - run requests

# %% 1. Run "update" on those cell IDs already in demeter
gdf_update = update_weather(conn_weather)
request_list = organize_and_process_meteomatics_requests(
    conn_weather, gdf_update, step="update", parameter_sets=None, n_cells_max_set=None
)

# %%  2. Get all cell IDs and first planting dates from `demeter`
gdf_need = get_spatiotemporal_weather_database_needs(cursor_demeter, cursor_weather)

# %% 3. Determine all cell IDs and the first year of data available for each in `weather`

# get_new_cell_id_by_year_combinations

# Find cell IDs which are not in the database or need more historical years of data.
cell_id = get_cell_ids_in_weather_table(cursor_weather, table="daily")
gdf_available = get_first_data_year_for_cell_id(
    cursor_weather, gdf_need["cell_id"].to_list()
)

# %% 4. Run "add" on those cell ID x year combinations which are new
gdf_new = gdf_need.loc[~gdf_need["cell_id"].isin(cell_id)]
