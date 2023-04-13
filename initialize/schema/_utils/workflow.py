import logging
from typing import Iterable

from pandas import DataFrame
from sqlalchemy.engine import Connection

from demeter.cli import check_and_format_db_connection_args
from demeter.db import getConnection

from .initialize import initialize_schema_type

RQ_USERS = {
    "DEMETER": ("demeter_user", "demeter_ro_user"),
    "RASTER": ("demeter_user", "demeter_ro_user", "raster_user", "raster_ro_user"),
    "WEATHER": ("demeter_user", "demeter_ro_user", "weather_user", "weather_ro_user"),
}


def _check_schema_users(conn: Connection, user_list: Iterable[str]) -> Iterable[str]:
    """Checks the database and ensures that all users in `user_list` exist.

    This function will raise an AssertionError if not all users in `user_list` exist
    in the database, but, otherwise, does nothing.
    """
    if not isinstance(user_list, tuple):
        user_list = tuple(user_list)

    with conn.connection.cursor() as cursor:
        # check if the given schema name already exists
        stmt = """
            select rolname from pg_roles
            where rolname in %(user_list)s
        """
        cursor = conn.connection.cursor()
        cursor.execute(stmt, {"user_list": user_list})
        df_results = DataFrame(cursor.fetchall())

        if len(df_results) > 0:
            existing_users = df_results["rolname"].to_list()
            missing_users = []
            for user in user_list:
                if user not in existing_users:
                    missing_users += [user]
        else:
            missing_users = user_list

    msg = (
        f"The following users have not yet been created: {missing_users}.\n"
        "Please run `python3 -m initialize.users`."
    )
    assert len(missing_users) == 0, msg


def run_schema_initialization(
    database_host: str,
    database_env: str,
    schema_name: str,
    schema_type: str,
    drop_existing: bool,
) -> bool:
    """Initialize  `schema_type` schema on specified DB host and environment.

    Currently implemented schemas: demeter, weather, raster

    Args:
        database_host (str): Host of database to query/change; can be 'AWS' or 'LOCAL'.
        database_env (str): Database instance to query/change; can be 'DEV' or 'PROD'.
        schema_name (str): Schema name to use for new schema.
        schema_type (str): Type of schema for which to run initialization.
        drop_existing (bool): Should the schema be re-created if it exists?

    Returns True if the schema was initialized. Returns False if the schema already exists and was not
        reinitialized.
    """
    assert (
        schema_type in RQ_USERS.keys()
    ), f"`schema_type` = {schema_type} not implemented."

    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env, superuser=True
    )

    # set up database connection
    logging.info("Connecting to database: %s", database_env_name)
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # check that users have been created
    _check_schema_users(conn=conn, user_list=RQ_USERS[schema_type])

    logging.info(
        "Initializing %s schema instance with name: %s", schema_type, schema_name
    )
    initialized = initialize_schema_type(
        conn=conn,
        schema_name=schema_name,
        schema_type=schema_type,
        drop_existing=drop_existing,
    )
    conn.close()

    return initialized
