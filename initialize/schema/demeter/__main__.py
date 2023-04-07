"""Initializes (or re-initializes) a Demeter schema instance for a given host and database environment.

To initialize `demeter`:
python3 -m intialize.schema.demeter --schema_name demeter

To re-initialize `demeter` from scratch on local `demeter-dev`:
python3 -m intialize.schema.demeter --schema_name demeter --drop_existing

For help: python3 -m intialize.schema.demeter --help

This script requires that you have the appropriate superuser credentials for the database in your .env file
and have set up `demeter_user` and `demeter_ro_user` as users on the database.
"""
import argparse
import logging

from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from demeter.db import getConnection

from ..._utils import (
    check_and_format_db_connection_args,
    check_schema_users,
    confirm_user_choice,
    get_flag_as_bool,
)
from ._initialize import initialize_demeter_instance

RQ_USERS = ("demeter_user", "demeter_ro_user")

if __name__ == "__main__":
    c = load_dotenv()
    logging_init()  # enables tqdm progress bar to work with logging

    parser = argparse.ArgumentParser(description="Initialize Demeter instance.")

    parser.add_argument(
        "--schema_name",
        type=str,
        help="Schema name to use for new Demeter instance.",
        required=True,
    )

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
        help="Should the schema be re-created if it exists?",
        default=False,
    )

    # set up args
    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    database_env = args.database_env

    drop_existing = get_flag_as_bool(args.drop_existing)

    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env
    )

    # confirm user choice
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing schema?",
        no_response="Continuing command with `drop_existing` set to False.",
    )

    # set up database connection
    logging.info("Connecting to database: %s", database_env_name)
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # check that users have been created
    check_schema_users(conn=conn, user_list=RQ_USERS)

    logging.info("Initializing demeter schema instance with name: %s", schema_name)
    _ = initialize_demeter_instance(conn, schema_name, drop_existing)
    conn.close()
