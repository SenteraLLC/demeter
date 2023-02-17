import logging
import subprocess
from os import getenv
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile

from sqlalchemy.engine import Connection


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
        print(schema_sql)
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
