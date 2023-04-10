from demeter.weather.weather_types import DAILY_WEATHER_TYPES

# TODO: This section is one of the sections that will be most impacted by future stories on parameters.
# We are keeping this part very basic (i.e., letting the user define how parameters are split up
# and how many cells should be considered for each parameter group) mostly because we have such a
# limited undertanding of how the API functions across space, time, and parameters. Currently, the
# workflow is operating with static parameter groups. There is no programmatic splitting of parameters
# when it comes to creating requests.

PARAMETERS = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
PARAMETER_SETS = [PARAMETERS[:6], PARAMETERS[6:]]

# TODO: What if we could design these request limits programmatically with the `request_log`?
# `n_cells_per_set` is a user lever to control for parameter variability in request time
N_CELLS_PER_SET_UPDATE = [1000, 1000]
N_CELLS_PER_SET_ADD = [100, 100]
N_CELLS_FILL = 100
