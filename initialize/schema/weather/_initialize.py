"""High-level functions for initializing the weather schema within an existing Postgres database."""

from datetime import (
    time,
    timedelta,
    timezone,
)
from os.path import (
    dirname,
    join,
    realpath,
)

from geo_utils.general import get_utc_offset
from geopandas import read_file as gpd_read_file
from sqlalchemy.engine import Connection

from demeter._version import __version__
from demeter.weather._grid_utils import (
    add_rast_metadata,
    add_raster,
    create_raster_for_utm_polygon,
    insert_utm_polygon,
)
from demeter.weather.weather_types import DAILY_WEATHER_TYPES
from demeter.weather.workflow.insert import maybe_insert_weather_type_to_db

from ..._utils import initialize_schema

DAILY_TEMPORAL_EXTENT = timedelta(days=1)


def populate_daily_weather_types(conn: Connection):
    """Populate the weather types table with daily MM parameters listed and detailed in `weather_types.py`.

    NOTE: This function assumes that all parameters listed have a daily temporal extent.
    """
    with conn.connection.cursor() as cursor:
        for weather_type_dict in DAILY_WEATHER_TYPES:
            weather_type = weather_type_dict["weather_type"]
            units = weather_type_dict["units"]
            description = weather_type_dict["description"]
            maybe_insert_weather_type_to_db(
                cursor, weather_type, DAILY_TEMPORAL_EXTENT, units, description
            )
        conn.connection.commit()


def populate_weather_grid(conn: Connection):
    """Populate the weather with 5km weather grid.

    See Confluence doc: https://sentera.atlassian.net/wiki/spaces/GML/pages/3260710936/Creating+the+5km+weather+grid
    """
    file_dir = realpath(join(dirname(__file__)))

    # load grid
    gdf_utm = (
        gpd_read_file(join(file_dir, "utm_grid.geojson"))
        .sort_values(["row", "zone"])
        .reset_index(drop=True)
    )

    # connect to database
    with conn.connection.cursor() as cursor:
        # loop through polygons, create rasters, and insert
        cell_id_min, cell_id_max = 0, 0
        raster_5km_id = 1
        for _, utm_poly in gdf_utm.iterrows():
            row, zone = utm_poly.row, int(utm_poly.zone)

            (
                array_cell_id,
                profile,
                cell_id_min,
                cell_id_max,
            ) = create_raster_for_utm_polygon(utm_poly, cell_id_min, cell_id_max)

            # get utc offset (with handling poles)
            if zone == 0:
                if row in ["A", "Y"]:
                    utc_offset_tz = timezone(timedelta(hours=-6))
                else:
                    utc_offset_tz = timezone(timedelta(hours=6))
            else:
                utc_offset_tz, _ = get_utc_offset(zone)
            utc_offset = time(0, 0, tzinfo=utc_offset_tz)

            raster_epsg = profile["crs"].to_epsg()

            # add UTM polygon to `world_utm` table
            insert_utm_polygon(
                cursor, zone, row, utm_poly.geometry, utc_offset, raster_epsg
            )

            # add raster
            add_raster(conn, array_cell_id, profile)

            # add raster metadata
            add_rast_metadata(cursor, raster_5km_id, profile)

            raster_5km_id += 1

            conn.connection.commit()


def _get_weather_types_as_string():
    """Extracts weather type names from `weather_types.py` and formats to be placed in schema creation SQL statement."""
    list_types = [weather_type["weather_type"] for weather_type in DAILY_WEATHER_TYPES]
    string_list_types = "'" + "','".join(list_types) + "'"
    return string_list_types


def _format_weather_schema_sql(schema_name: str, schema_sql: str) -> str:
    """Formats `weather` schema SQL file with new weather types ENUM and version."""
    weather_types = _get_weather_types_as_string()

    schema_sql = schema_sql.replace("v0.0.0", "v" + __version__)

    # Add list of weather types
    schema_sql = schema_sql.replace("PARAMETER_LIST", weather_types)

    return schema_sql


def initialize_weather_schema(conn: Connection, drop_existing: bool = False) -> bool:
    """Initializes weather schema using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        drop_existing (bool): Indicates whether or not an existing schema called `schema_name` should be dropped if
        exists.
    """

    file_dir = realpath(join(dirname(__file__), ".."))
    sql_fname = join(file_dir, "sql", "schema_weather.sql")

    return initialize_schema(
        conn,
        schema_name="weather",
        sql_fname=sql_fname,
        sql_function=_format_weather_schema_sql,
        drop_existing=drop_existing,
    )
