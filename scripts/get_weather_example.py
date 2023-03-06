"""
Sample Python script to utilize some of the weather data helper functions to extract weather data for new cell IDS.
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
from demeter.weather._grid_utils import get_centroid, get_world_utm_info_for_cell_id
from demeter.weather.meteomatics._insert import get_weather_type_id_from_db
from demeter.weather.meteomatics.main import (
    attempt_and_maybe_insert_meteomatics_request,
    split_gdf_for_new_cell_ids,
)
from demeter.weather.schema.weather_types import DAILY_WEATHER_TYPES

c = load_dotenv()
conn = getConnection(env_name="DEMETER-DEV_LOCAL_WEATHER")
cursor = conn.connection.cursor()

# %% generate test data (n = n_cells) from row T and zones 11-15 (world utm ID 913-917)
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


# %%
# # wind gusts has been removed for now to avoid problems
full_parameters = [weather_type[0] for weather_type in DAILY_WEATHER_TYPES]
parameter_sets = [full_parameters[:6], full_parameters[6:]]
n_cells_max_set = [100, 100]

if parameter_sets is not None:
    msg = "All sublists in `parameter_sets` must have length <= 10."
    assert any([len(sublist) <= 10] for sublist in parameter_sets), msg
    parameters = [elem for sublist in parameter_sets for elem in sublist]

# TODO: Make into argument and implement `pool` function
parallel = False

# Get information on parameters from DB and checks that they exist there
params_to_weather_types = get_weather_type_id_from_db(cursor, parameters)
request_list = split_gdf_for_new_cell_ids(gdf, parameter_sets, n_cells_max_set)

# %%
# request_split_utm = split_by_utm_zone(gdf)

# request_split_year = [split_by_year(this_gdf) for this_gdf in request_split_utm]
# request_split_year = [item for sublist in request_split_year for item in sublist]

# request_split_cells = [
#     split_by_n_cells(this_gdf, 100) for this_gdf in request_split_year
# ]
# request_split_cells = [item for sublist in request_split_cells for item in sublist]

# request_list += get_request_list_from_gdfs_list(
#     list_gdfs=list_gdfs,
#     utm_zone=row["utm_zone"],
#     utc_offset=utc_offset,
#     parameters=this_parameter_set,
# )


# %%
# organize cell ID information with lat and lon to connect MM info to weather network
gdf_cell_id = gdf[["world_utm_id", "cell_id", "centroid"]]
gdf_cell_id.insert(0, "lon", round(gdf_cell_id.geometry.x, 5))
gdf_cell_id.insert(0, "lat", round(gdf_cell_id.geometry.y, 5))

# perform requests
if parallel:
    pass
else:
    for ind in range(len(request_list)):
        print(ind)
        request = request_list[ind]
        request = attempt_and_maybe_insert_meteomatics_request(
            conn=conn,
            request=request,
            gdf_cell_id=gdf_cell_id,
            params_to_weather_types=params_to_weather_types,
        )
        request_list[ind] = request

# %% Re-run failed weather attempts?
