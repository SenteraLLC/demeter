"""Initializes (or re-initializes) a Demeter raster schema instance for a given host and database environment.

To initialize raster schema on local "demeter-dev":
python3 -m initialize.schema.raster

To re-initialize raster schema on local "demeter-dev":
python3 -m initialize.schema.raster --drop_existing


For help: python3 -m initialize.schema.raster --help

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that you have set up
the following users: demeter_user, demeter_ro_user, raster_user, and raster_ro_user.
"""
import argparse
import logging

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter.db import getConnection

from ..._utils import (
    check_and_format_db_connection_args,
    check_schema_users,
    confirm_user_choice,
    get_flag_as_bool,
)
from ._initialize import initialize_raster_schema

RQ_USERS = ("demeter_user", "demeter_ro_user", "raster_user", "raster_ro_user")

if __name__ == "__main__":
    c = load_dotenv()
    logging_init()

    parser = argparse.ArgumentParser(description="Initialize raster schema instance.")

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
    database_host = args.database_host
    database_env = args.database_env

    drop_existing = get_flag_as_bool(args.drop_existing)

    # ensure appropriate set-up
    database_env_name, ssh_env_name = check_and_format_db_connection_args(
        host=database_host, env=database_env, superuser=True
    )

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

    logging.info("Initializing raster schema instance")
    _ = initialize_raster_schema(conn, drop_existing)
    conn.close()
