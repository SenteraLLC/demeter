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

from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from ..._utils import confirm_user_choice
from .._utils.workflow import run_schema_initialization

if __name__ == "__main__":
    c = load_dotenv()
    logging_init()  # enables tqdm progress bar to work with logging

    parser = argparse.ArgumentParser(description="Initialize Demeter instance.")

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
        help="Schema name to use for new Demeter instance.",
        default="demeter",
    )

    # set up args
    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing

    # confirm user choice
    drop_existing = confirm_user_choice(
        drop_existing,
        question="Are you sure you want to drop existing schema?",
        no_response="Continuing command with `drop_existing` set to False.",
    )

    _ = run_schema_initialization(
        database_host=database_host,
        database_env=database_env,
        schema_name=schema_name,
        schema_type="DEMETER",
        drop_existing=drop_existing,
    )
