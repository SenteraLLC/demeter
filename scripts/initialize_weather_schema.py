"""Initializes (or re-initializes) a Demeter weather schema instance for a given host and database environment.

For help: python3 -m scripts.initialize_weather_schema --help

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that you have passwords
for the two weather schema users--weather_user and weather_ro_user-- in your .env file.

Once you have those things ready to go, set up the schema by running: python3 -m scripts.initialize_weather_schema --database_env='DEV' --database_host='LOCAL'
"""
import argparse
import logging
import sys

import click
from dotenv import load_dotenv  # type: ignore
from utils.logging.tqdm import logging_init

from demeter.db import getConnection
from demeter.weather._initialize import (
    initialize_weather_schema,
    populate_daily_weather_types,
    populate_weather_grid,
)

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

    # set up args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env

    if args.drop_existing:
        drop_existing = True
    else:
        drop_existing = False

    # ensure appropriate set-up
    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert database_env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    if database_host == "AWS":
        if click.confirm(
            "Are you sure you want to tunnel to AWS database?", default=False
        ):
            logging.info("Connecting to AWS database instance.")
        else:
            sys.exit()

    if drop_existing:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
    else:
        logging.info("Continuing command with `drop_existing` set to False.")
        drop_existing = False

    ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    database_env_name = f"DEMETER-{database_env}_{database_host}_SUPER"

    # set up database connection
    logging.info("Connecting to database: %s", database_env_name)
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    logging.info("Initializing weather schema instance")
    _ = initialize_weather_schema(conn, drop_existing)
    conn.close()

    populate_weather_grid()
    populate_daily_weather_types()
