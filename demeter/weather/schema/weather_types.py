# TODO: Fix wind gust description
DAILY_WEATHER_TYPES = [
    [
        "t_min_2m_24h:C",
        "degrees Celsius",
        "Minimum daily air temperature at two meters above ground",
    ],
    [
        "t_max_2m_24h:C",
        "degrees Celsius",
        "Maximum daily air temperature at two meters above ground",
    ],
    [
        "t_mean_2m_24h:C",
        "degrees Celsius",
        "Mean daily air temperature at two meters above ground",
    ],
    [
        "precip_24h:mm",
        "millimeters",
        "Cumulative daily precipitation, including liquid (rain, freezing rain), mix (sleet), and solid (hail, graupel, snow) precipitation.",
    ],
    [
        "wind_speed_mean_2m_24h:ms",
        "meters per second",
        "Mean daily wind speed at two meters above ground",
    ],
    # [
    #     "wind_gusts_2m_24h:ms",
    #     "meters per second",
    #     "Wind gusts at two meters above ground",
    # ],
    [
        "relative_humidity_mean_2m_24h:p",
        "percent",
        "Mean daily relative humidity at two meters above ground",
    ],
    [
        "global_rad_24h:J",
        "Joules",
        "Cumulative daily global radiation (diffuse + direct radiation)",
    ],
    ["evapotranspiration_24h:mm", "Millimeters", "Cumulative daily evapotranspiration"],
    [
        "volumetric_soil_water_-50cm:m3m3",
        "cubic meters/cubic meters",
        "Volumetric soil water between soil depth of 28-100cm",
    ],
    [
        "soil_moisture_deficit:mm",
        "miliimeters",
        "Difference between the actual water content of the soil (mm) and the soil's field capacity (mm)",
    ],
    [
        "leaf_wetness:idx",
        "binary (0/1)",
        "Amount of dew left on surfaces to be used for detection of fog and dew conditions; value of 1 indicates wetness",
    ],
]
