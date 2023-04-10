import logging

from demeter.db import getConnection

from ..._utils import check_and_format_db_connection_args, check_schema_users
from ._initialize import initialize_raster_schema

RQ_USERS = ("demeter_user", "demeter_ro_user", "raster_user", "raster_ro_user")


def main(database_host: str, database_env: str, drop_existing: bool):
    """Initialize  `raster` schema on specified DB host and environment.

    Args:
        database_host (str): Host of database to query/change; can be 'AWS' or 'LOCAL'.
        database_env (str): Database instance to query/change; can be 'DEV' or 'PROD'.
        drop_existing (bool): Should the schema be re-created if it exists?
    """
    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env, superuser=True
    )

    # set up database connection
    logging.info("Connecting to database: %s", database_env_name)
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # check that users have been created
    check_schema_users(conn=conn, user_list=RQ_USERS)

    logging.info("Initializing raster schema instance")
    _ = initialize_raster_schema(conn, drop_existing)
    conn.close()
