import logging
import subprocess
from datetime import time
from os import getenv
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile

from geo_utils.general import get_utc_offset
from geopandas import read_file as gpd_read_file
from sqlalchemy.engine import Connection

from demeter.db import getConnection
from demeter.weather._grid_utils import (
    add_rast_metadata,
    add_raster,
    create_raster_for_utm_polygon,
    insert_utm_polygon,
)


def populate_weather_grid():
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
    conn = getConnection(env_name="DEMETER-DEV_LOCAL_WEATHER_SUPER")
    cursor = conn.connection.cursor()
    trans = conn.begin()

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

        # get utc offset
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

    trans.commit()
    trans.close()
    conn.close()


def initialize_weather_schema(conn: Connection, drop_existing: bool = False) -> bool:
    """Initializes weather schema using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        drop_existing (bool): Indicates whether or not an existing schema called `schema_name` should be dropped if
        exists.
    """

    # check if the given schema name already exists
    stmt = """
        select * from information_schema.schemata
        where schema_name = %(schema_name)s
    """
    cursor = conn.connection.cursor()
    cursor.execute(stmt, {"schema_name": "weather"})
    results = cursor.fetchall()

    # if the schema exists already, drop existing if `drop_existing` is True, else do nothing
    if len(results) > 0:
        logging.info("A schema of name 'weather' already exists in this database.")
        if not drop_existing:
            logging.info(
                "No further action will be taken as no `--drop_existing` flag was passed."
            )
            logging.info(
                "Add `--drop_existing` flag to command call if you would like to re-initialize the schema."
            )
            return False
        else:
            logging.info(
                "`drop_existing` is True. The existing schema will be dropped and overwritten."
            )
            stmt = """DROP SCHEMA IF EXISTS weather CASCADE;"""
            conn.execute(stmt)

    # make temporary SQL file and overwrite schema name lines
    file_dir = realpath(join(dirname(__file__)))

    with NamedTemporaryFile() as tmp:
        with open(join(file_dir, "schema_weather.sql"), "r") as schema_f:
            schema_sql = schema_f.read()

            # Add user passwords
            schema_sql = schema_sql.replace(
                "weather_user_password", getenv("weather_user_password")
            )
            schema_sql = schema_sql.replace(
                "weather_ro_user_password", getenv("weather_ro_user_password")
            )
        tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
        tmp.flush()
        host = conn.engine.url.host
        username = conn.engine.url.username
        password = conn.engine.url.password
        database = conn.engine.url.database
        port = conn.engine.url.port
        psql = f'PGPASSWORD={password} psql -h {host} -p {port} -U {username} -f "{tmp.name}" {database}'

        subprocess.call(psql, shell=True)

    return True
