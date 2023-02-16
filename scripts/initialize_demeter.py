"""Initializes (or re-initializes) a Demeter schema instance for a given host and database environment.

This script requires that you have the appropriate superuser credentials for the database in your .env file, as well as the necessary passwords for the
`demeter_user` and `demeter_ro_user`.
"""
import argparse
import logging

import click
from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from demeter.db import getConnection, initializeDemeterInstance

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
        type=bool,
        help="Should the schema be re-created if it exists?",
        default=False,
    )

    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    database_env = args.database_env
    drop_existing = args.drop_existing

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert database_env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    database_env_name = f"DEMETER-{database_env}_{database_host}_SUPER"

    logging.info("Connecting to database: %s", database_env_name)

    conn = getConnection(env_name=database_env_name)

    if drop_existing:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
        else:
            logging.info("Continuing command with `drop_existing` set to False.")
            drop_existing = False

    _ = initializeDemeterInstance(conn, schema_name, drop_existing)
