import logging
import subprocess
from os import getenv
from os.path import (
    dirname,
    join,
    realpath,
)
from tempfile import NamedTemporaryFile
from typing import Iterable

from pandas import DataFrame
from psycopg2.sql import Identifier
from sqlalchemy.engine import Connection

from demeter.cli import check_and_format_db_connection_args
from demeter.db import getConnection
from demeter.db._postgres.tools import doPgFormat

USER_LIST = (
    "demeter_user",
    "demeter_ro_user",
    "weather_user",
    "weather_ro_user",
    "raster_user",
    "raster_ro_user",
)


def _run_create_users_sql(conn: Connection):
    """Runs `create_users.sql` which attempts to create all users in `USER_LIST` in connected database.

    If a user already exists, a SQL error will occur, but will not break script.

    Requires that all user passwords are set using environment variables in the .env file.
    Raises AssertionError if they are not all set.
    """
    # check to make sure all environment variables are present
    list_missing_passwords = []
    for user in USER_LIST:
        check_password = getenv(f"{user}_password")
        if check_password is None:
            list_missing_passwords += [f"{user}_password"]
    msg = (
        f"The following passwords are not set in .env file: {list_missing_passwords}\n"
        "Please set them and try again."
    )
    assert len(list_missing_passwords) == 0, msg

    # make temporary SQL file and overwrite schema name lines
    file_dir = realpath(dirname(__file__))
    with NamedTemporaryFile() as tmp:
        with open(join(file_dir, "create_users.sql"), "r") as schema_f:
            schema_sql = schema_f.read()

            # add user passwords
            for user in USER_LIST:
                env_varname = f"{user}_password"
                schema_sql = schema_sql.replace(env_varname, getenv(env_varname))

        tmp.write(schema_sql.encode())  # Writes SQL script to a temp file
        tmp.flush()  # Push file contents so they are accessible
        host = conn.engine.url.host
        username = conn.engine.url.username
        password = conn.engine.url.password
        database = conn.engine.url.database
        port = conn.engine.url.port
        psql = f'PGPASSWORD={password} psql -h {host} -p {port} -U {username} -f "{tmp.name}" {database}'
        subprocess.call(psql, shell=True)


def _drop_existing_demeter_users(conn: Connection, user_list: Iterable[str]):
    """Drops all users from `user_list` that already exist in database."""

    if not isinstance(user_list, tuple):
        user_list = tuple(user_list)

    with conn.connection.cursor() as cursor:
        # check which users in `user_list` already exist
        stmt = """
            select rolname from pg_roles
            where rolname in %(user_list)s
        """
        cursor = conn.connection.cursor()
        cursor.execute(stmt, {"user_list": user_list})
        df_results = DataFrame(cursor.fetchall())

        # drop all existing users from `user_list`
        if len(df_results) > 0:
            drop_roles = df_results["rolname"].to_list()

            logging.info(
                "The following users already exist in the database and will be dropped: %s.",
                drop_roles,
            )

            for role in drop_roles:
                template = "DROP OWNED by {0};"
                stmt = doPgFormat(template, Identifier(role))
                cursor.execute(stmt)

                template = "DROP USER {0};"
                stmt = doPgFormat(template, Identifier(role))
                cursor.execute(stmt)

            conn.connection.commit()

        else:
            logging.info(
                "No demeter users have been created yet, so no users will be dropped."
            )


def create_db_users(
    database_host: str,
    database_env: str,
    drop_existing: bool,
    user_list: Iterable[str] = USER_LIST,
):
    """Create demeter users in `user_list` on specified DB host and environment.

    Args:
        database_host (str): Host of database to query/change; can be 'AWS' or 'LOCAL'.
        database_env (str): Database instance to query/change; can be 'DEV' or 'PROD'.
        drop_existing (bool): Should the schema be re-created if it exists?
        user_list (list of str): Optional list of users to drop and re-create. Defaults to all demeter users.
    """
    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env, superuser=True
    )

    # set up database connectionx
    logging.info("Connecting to database: %s", database_env_name)
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # main program
    if drop_existing:
        _drop_existing_demeter_users(conn, user_list=user_list)

    logging.info("Creating demeter users")
    _run_create_users_sql(conn)
    conn.close()

    logging.info(
        "All users have been created. User access to schemas is provided upon schema initialization."
    )
