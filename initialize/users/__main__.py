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
from typing import Iterable

from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from .._utils import confirm_user_choice
from ._create_users import USER_LIST, create_db_users

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
    drop_existing = args.drop_existing
    user_list = args.user_list

    if not isinstance(user_list, Iterable):
        user_list = [user_list]

    # confirm user choice
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing roles?",
        no_response="Continuing command with `drop_existing` set to False.",
    )

    create_db_users(
        database_host=database_host,
        database_env=database_env,
        drop_existing=drop_existing,
        user_list=user_list,
    )
