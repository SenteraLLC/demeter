"""Initializes (or re-initializes) a Demeter raster schema instance for a given host and database environment.

To initialize raster schema on local "demeter-dev":
python3 -m demeter_initialize.schema.raster

To re-initialize raster schema on local "demeter-dev":
python3 -m demeter_initialize.schema.raster --drop_existing


For help: python3 -m demeter_initialize.schema.raster --help

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that you have set up
the following users: demeter_user, demeter_ro_user, raster_user, and raster_ro_user.
"""
import argparse

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter.cli import confirm_user_choice

from .._utils.initialize import SQL_FNAMES
from .._utils.workflow import run_schema_initialization

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

    parser.add_argument(
        "--schema_name",
        type=str,
        help="Schema name to use for new `raster` instance.",
        default="raster",
    )

    # set up args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing
    schema_name = args.schema_name

    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing schema?",
        no_response="Continuing command with `drop_existing` set to False.",
    )

    _ = run_schema_initialization(
        database_host=database_host,
        database_env=database_env,
        schema_name=schema_name,
        schema_type="RASTER",
        drop_existing=drop_existing,
        sql_fnames=SQL_FNAMES,
    )
