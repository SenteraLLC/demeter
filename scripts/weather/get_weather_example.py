"""
Sample Python script that exemplifies a modular workflow for extracting weather data for new cell IDS.
"""

# %% imports

import random
from datetime import datetime
from random import sample

from dotenv import load_dotenv
from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import concat as pd_concat

from demeter.db import getConnection
from demeter.weather.initialize.weather_types import DAILY_WEATHER_TYPES
from demeter.weather.query import get_weather_type_id_from_db
from demeter.weather.query._grid import get_centroid, get_world_utm_info_for_cell_id
from demeter.weather.workflow.request import submit_and_maybe_insert_meteomatics_request
from demeter.weather.workflow.split import split_gdf_for_add

c = load_dotenv()
conn = getConnection(env_name="DEMETER-DEV_LOCAL_WEATHER")
cursor = conn.connection.cursor()

# %% STEP 1: Gather information on cell ID, date_first, date_last

# In this case, we create sample data.
# Generate test data (n = n_cells) from row T and zones 11-15 (world utm ID 913-917)
random.seed(999999)
n_cells = 100
cell_ids = sample(range(17132197, 17218911), n_cells)

cols = [
    "world_utm_id",
    "utm_zone",
    "utc_offset",
    "cell_id",
    "date_first",
    "date_last",
    "centroid",
]
df = DataFrame(columns=cols)

for id_ind in range(n_cells):
    cell_id = int(cell_ids[id_ind])

    df_world_utm = get_world_utm_info_for_cell_id(cursor, cell_id)

    centroid = get_centroid(cursor, df_world_utm["world_utm_id"].item(), cell_id)

    year = sample(range(2015, 2024), 1)[0]
    date_first = datetime(year, 1, 1)
    date_last = datetime.now().date()
    data = {
        "world_utm_id": [df_world_utm["world_utm_id"].item()],
        "utm_zone": [df_world_utm["zone"].item()],
        "utc_offset": [df_world_utm["utc_offset"].item()],
        "cell_id": [cell_id],
        "date_first": [date_first],
        "date_last": [date_last],
        "centroid": centroid,
    }
    df = pd_concat([df, DataFrame(data)])

df.reset_index(drop=True, inplace=True)
gdf = GeoDataFrame(df, geometry="centroid")


# %% STEP 2: Organize parameter sets

# wind gusts has been removed for now to avoid problems
# we use `n_cells_max_set` to control for parameter variability in request time
full_parameters = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
parameter_sets = [full_parameters[:6], full_parameters[6:]]
n_cells_per_set = [100, 100]

parameters = [elem for sublist in parameter_sets for elem in sublist]

# TODO: Make into argument and implement `pool` function
parallel = False

# get information on parameters from DB and checks that they exist there
params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)

# %% STEP 3: Split up the requests using some sort of logic
# This step will be specific to the different weather extraction steps
request_list = split_gdf_for_add(gdf, parameter_sets, n_cells_per_set)

# %% STEP 4: Perform requests
# organize cell ID information with lat and lon to connect MM info to weather network
gdf_cell_id = gdf[["world_utm_id", "cell_id", "centroid"]]
gdf_cell_id.insert(0, "lon", gdf_cell_id.geometry.x)
gdf_cell_id.insert(0, "lat", gdf_cell_id.geometry.y)


if parallel:
    pass
else:
    for ind in range(len(request_list)):
        print(ind)
        request = request_list[ind]
        request = submit_and_maybe_insert_meteomatics_request(
            conn=conn,
            request=request,
            gdf_cell_id=gdf_cell_id,
            params_to_weather_types=params_to_weather_types,
        )
        request_list[ind] = request

# %% Re-run failed weather attempts?
