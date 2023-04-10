import logging
from typing import Iterable

from demeter.db import getConnection

from .._utils import check_and_format_db_connection_args
from ._users import (
    USER_LIST,
    create_demeter_users,
    drop_existing_demeter_users,
)


def main(
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
        drop_existing_demeter_users(conn, user_list=user_list)

    logging.info("Creating demeter users")
    create_demeter_users(conn)
    conn.close()

    logging.info(
        "All users have been created. User access to schemas is provided upon schema initialization."
    )
