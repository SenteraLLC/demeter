"""Initializes (or re-initializes) a Demeter weather schema instance for a given host and database environment and populates the weather grid and weather types.

To initialize and populate weather schema on local "demeter-dev":
python3 -m demeter_initialize.schema.weather

To re-initialize and populate weather schema on local "demeter-dev":
python3 -m demeter_initialize.schema.weather --drop_existing


For help: python3 -m demeter_initialize.schema.weather --help

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that you have set up
the following users: demeter_user, demeter_ro_user, weather_user, and weather_ro_user.
"""
import argparse

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter.cli import confirm_user_choice

from .._utils.initialize import SQL_FNAMES
from .._utils.workflow import run_schema_initialization
from ._populate_weather import populate_weather

if __name__ == "__main__":
    c = load_dotenv()
    logging_init()

    parser = argparse.ArgumentParser(
        description="Initialize weather schema instance and populate with weather grid and weather types."
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

    parser.add_argument(
        "--schema_name",
        type=str,
        help="Schema name to use for new `weather` instance.",
        default="weather",
    )

    # set up args
    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing

    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing schema?",
        no_response="Continuing command with `drop_existing` set to False.",
    )

    initialized = run_schema_initialization(
        database_host=database_host,
        database_env=database_env,
        schema_name=schema_name,
        schema_type="WEATHER",
        drop_existing=drop_existing,
        sql_fnames=SQL_FNAMES,
    )

    if initialized:
        populate_weather(database_host=database_host, database_env=database_env)
