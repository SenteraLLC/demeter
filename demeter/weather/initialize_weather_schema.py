"""Initializes (or re-initializes) a Demeter schema instance for a given host and database environment.

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that the demeter users are
already set up on your database using `scripts.initialize_demeter`.

It can be run from the command line as: python3 -m demeter.weather.initialize_weather_schema --database_env='DEV' --database_host='LOCAL'

"""
import argparse
import subprocess
from os.path import (
    dirname,
    join,
    realpath,
)

import click
from dotenv import load_dotenv  # type: ignore
from sqlalchemy.engine import Connection

from demeter.db import getConnection


def initialize_weather_schema(conn: Connection, drop_existing: bool = False) -> bool:
    """Initializes weather schema using database connection.

    Returns True if schema was successfully initialized.

    Args:
        conn (sqlalchemy.engine.Connection): Connection to demeter database where schema should be created.
        drop_existing (bool): Indicates whether or not an existing schema called `schema_name` should be dropped if exists.
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
        print("A schema of name 'weather' already exists in this database.")
        if not drop_existing:
            print("Change `drop_existing` to True if you'd like to drop it.")
            return False
        else:
            print(
                "`drop_existing` is True. The existing schema will be dropped and overwritten."
            )
            stmt = """DROP SCHEMA IF EXISTS weather CASCADE;"""
            conn.execute(stmt)

    # make temporary SQL file and overwrite schema name lines
    file_dir = realpath(join(dirname(__file__)))
    schema_file = join(file_dir, "schema_weather.sql")

    # collect credentials
    host = conn.engine.url.host
    username = conn.engine.url.username
    password = conn.engine.url.password
    database = conn.engine.url.database
    port = conn.engine.url.port
    psql = f'PGPASSWORD={password} psql -h {host} -p {port} -U {username} -f "{schema_file}" {database}'
    print(schema_file)
    subprocess.call(psql, shell=True)

    return True


if __name__ == "__main__":

    c = load_dotenv()

    parser = argparse.ArgumentParser(description="Initialize weather schema instance.")

    parser.add_argument(
        "--database_host",
        type=str,
        help="Host of database to query/change; can be 'AWS' or 'LOCAL'.",
        default="LOCAL",
    )

    parser.add_argument(
        "--database_env",
        type=str,
        help="Database instance to query/change; can be 'DEV' or 'PROD'.",
        default="DEV",
    )

    parser.add_argument(
        "--drop_existing",
        type=bool,
        help="Should the schema be re-created if it exists?",
        default=False,
    )

    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert database_env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    database_env_name = f"DEMETER-{database_env}_{database_host}_SUPER"

    print(f"Connecting to database: {database_env_name}")

    conn = getConnection(env_name=database_env_name)

    if drop_existing:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
        else:
            print("Continuing command with `drop_existing` set to False.")
            drop_existing = False

    _ = initialize_weather_schema(conn, drop_existing)
