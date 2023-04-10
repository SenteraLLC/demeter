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
