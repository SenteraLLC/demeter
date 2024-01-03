"""Initializes `demeter` users and all `demeter` schemas (i.e., demeter, weather, and raster).

To initialize users and all schemas on local `demeter-dev`:
python3 -m demeter_initialize.all --database_host LOCAL --database_env DEV --drop_existing

To re-initialize users and re-initialize all three schemas from scratch on local `demeter-dev`:
python3 -m demeter_initialize.all --drop_users --drop_schemas

For help: python3 -m demeter_initialize.all --help

This script requires that you have the appropriate superuser credentials for the database in your .env file for all demeter users.
"""
import argparse
import logging

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter.cli import confirm_user_choice

from ..schema._utils.workflow import run_schema_initialization
from ..schema.weather._populate_weather import populate_weather
from ..users._create_users import USER_LIST, create_db_users

SCHEMA_NAMES = {
    "DEMETER": "demeter",
    "RASTER": "raster",
    "WEATHER": "weather",
}


if __name__ == "__main__":
    logging_init()  # enables tqdm progress bar to work with logging
    c = load_dotenv()

    parser = argparse.ArgumentParser(description="Initialize Demeter database.")

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
        help="Should any existing users and schemas be dropped and re-created?",
        default=False,
    )

    # set up args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing

    # confirm user choices
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing users and schemas?",
        no_response="Continuing command with `drop_schemas` set to False.",
    )

    # create users
    logging.info("CREATING USERS")
    create_db_users(
        database_host=database_host,
        database_env=database_env,
        drop_existing=drop_existing,
        user_list=USER_LIST,
    )

    # create demeter
    for schema_type in SCHEMA_NAMES.keys():
        logging.info("CREATING %s", schema_type)
        initialized = run_schema_initialization(
            database_host=database_host,
            database_env=database_env,
            schema_name=SCHEMA_NAMES[schema_type],
            schema_type=schema_type,
            drop_existing=drop_existing,
        )
        if schema_type == "WEATHER" and initialized:
            populate_weather(database_host=database_host, database_env=database_env)
