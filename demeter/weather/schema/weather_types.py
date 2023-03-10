# TODO: Fix wind gust description
DAILY_WEATHER_TYPES = [
    {
        "weather_type": "t_min_2m_24h:C",
        "units": "degrees Celsius",
        "description": "Minimum daily air temperature at two meters above ground",
    },
    {
        "weather_type": "t_max_2m_24h:C",
        "units": "degrees Celsius",
        "description": "Maximum daily air temperature at two meters above ground",
    },
    {
        "weather_type": "t_mean_2m_24h:C",
        "units": "degrees Celsius",
        "description": "Mean daily air temperature at two meters above ground",
    },
    {
        "weather_type": "precip_24h:mm",
        "units": "millimeters",
        "description": "Cumulative daily precipitation, including liquid (rain, freezing rain), mix (sleet), and solid (hail, graupel, snow) precipitation.",
    },
    {
        "weather_type": "wind_speed_mean_2m_24h:ms",
        "units": "meters per second",
        "description": "Mean daily wind speed at two meters above ground",
    },
    # {
    #     "weather_type":"wind_gusts_2m_24h:ms",
    #     "units":"meters per second",
    #     "description":"Wind gusts at two meters above ground",
    # ],
    {
        "weather_type": "relative_humidity_mean_2m_24h:p",
        "units": "percent",
        "description": "Mean daily relative humidity at two meters above ground",
    },
    {
        "weather_type": "global_rad_24h:J",
        "units": "Joules",
        "description": "Cumulative daily global radiation (diffuse + direct radiation)",
    },
    {
        "weather_type": "evapotranspiration_24h:mm",
        "units": "millimeters",
        "description": "Cumulative daily evapotranspiration",
    },
    {
        "weather_type": "volumetric_soil_water_-50cm:m3m3",
        "units": "cubic meters/cubic meters",
        "description": "Volumetric soil water between soil depth of 28-100cm",
    },
    {
        "weather_type": "soil_moisture_deficit:mm",
        "units": "millimeters",
        "description": "Difference between the actual water content of the soil (mm) and the soil's field capacity (mm)",
    },
    {
        "weather_type": "leaf_wetness:idx",
        "units": "binary (0/1)",
        "description": "Amount of dew left on surfaces to be used for detection of fog and dew conditions; value of 1 indicates wetness",
    },
]
