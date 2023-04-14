from demeter._version import __version__
from demeter.weather.weather_types import DAILY_WEATHER_TYPES


def format_demeter_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `demeter` schema SQL file with new `schema_name` and version."""

    if schema_name != "test_demeter":
        schema_sql = schema_sql.replace("test_demeter", schema_name)

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    return schema_sql


# schema_name must be a part of this formatting function
def format_raster_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `raster` schema SQL file with new version."""

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    return schema_sql


def _get_weather_types_as_string():
    """Extracts weather type names from `weather_types.py` and formats to be placed in schema creation SQL statement."""
    list_types = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
    string_list_types = "'" + "','".join(list_types) + "'"
    return string_list_types


def format_weather_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `weather` schema SQL file with new weather types ENUM and version."""
    weather_types = _get_weather_types_as_string()

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    # Add list of weather types
    schema_sql = schema_sql.replace("PARAMETER_LIST", weather_types)

    return schema_sql
