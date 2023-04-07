"""Create all database users for Demeter database.

Initialize users for the first time on local `demeter-dev`:
python3 -m initialize.users

Drop all users and re-create them on local `demeter-dev`:
python3 -m initialize.users --drop_existing

Drop just the weather users and re-create them on local `demeter-dev`:
python3 -m initialize.users --drop_existing --user_list weather_user weather_ro_user

For help: python3 -m initialize.users --help

This script requires that you have the appropriate superuser credentials for the database in your .env file, as well as
the necessary passwords for all database users. See "Setting up Demeter" in Confluence.
"""
import argparse
import logging
from typing import Iterable

from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from demeter.db import getConnection

from .._utils import (
    check_and_format_db_connection_args,
    confirm_user_choice,
    get_flag_as_bool,
)
from ._users import (
    USER_LIST,
    create_demeter_users,
    drop_existing_demeter_users,
)

if __name__ == "__main__":
    c = load_dotenv()
    logging_init()

    parser = argparse.ArgumentParser(description="Create Demeter users.")

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
        action="store_true",
        help="Should existing roles be dropped and recreated?",
        default=False,
    )

    parser.add_argument(
        "--user_list",
        help="Optional list of users to drop and re-create. Defaults to all demeter users.",
        nargs="+",
        default=USER_LIST,
    )

    # parse args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env
    user_list = args.user_list

    if not isinstance(user_list, Iterable):
        user_list = [user_list]

    drop_existing = get_flag_as_bool(args.drop_existing)

    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env
    )

    # confirm user choice
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing roles?",
        no_response="Continuing command with `drop_existing` set to False.",
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
