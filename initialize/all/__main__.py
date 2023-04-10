"""Initializes `demeter` users and all `demeter` schemas (i.e., demeter, weather, and raster).

To initialize users and all schemas on local `demeter-dev`:
python3 -m intialize.all

To re-initialize users and re-initialize all three schemas from scratch on local `demeter-dev`:
python3 -m intialize.all --drop_users --drop_schemas

For help: python3 -m intialize.all --help

This script requires that you have the appropriate superuser credentials for the database in your .env file for all demeter users.
"""
import argparse
import logging

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from .._utils import confirm_user_choice, get_flag_as_bool
from ..schema.demeter.main import main as run_initialize_demeter
from ..schema.raster.main import main as run_initialize_raster
from ..schema.weather.main import main as run_initialize_weather
from ..users._users import USER_LIST
from ..users.main import main as run_initialize_users

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

    parser.add_argument(
        "--demeter_schema_name",
        type=str,
        help="Schema name to use for new Demeter instance.",
        default="demeter",
    )

    # set up args
    args = parser.parse_args()
    demeter_schema_name = args.demeter_schema_name
    database_host = args.database_host
    database_env = args.database_env

    drop_existing = get_flag_as_bool(args.drop_existing)

    # confirm user choices
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing users and schemas?",
        no_response="Continuing command with `drop_schemas` set to False.",
    )

    # create users
    logging.info("CREATING USERS")
    run_initialize_users(
        database_host=database_host,
        database_env=database_env,
        drop_existing=drop_existing,
        user_list=USER_LIST,
    )

    # create demeter
    logging.info("CREATING DEMETER")
    run_initialize_demeter(
        database_host=database_host,
        database_env=database_env,
        schema_name=demeter_schema_name,
        drop_existing=drop_existing,
    )

    # create weather
    logging.info("CREATING WEATHER")
    run_initialize_weather(
        database_host=database_host,
        database_env=database_env,
        drop_existing=drop_existing,
    )

    # create raster
    logging.info("CREATING RASTER")
    run_initialize_raster(
        database_host=database_host,
        database_env=database_env,
        drop_existing=drop_existing,
    )
