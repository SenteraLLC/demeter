"""Initializes (or re-initializes) a Demeter schema instance for a given host and database environment.

This script requires that you have the appropriate superuser credentials for the database in your .env file. It also requires that you have passwords
for the two weather schema users--weather_user and weather_ro_user-- in your .env file.

Once you have those things ready to go, set up the schema by running: python3 -m demeter.weather.initialize_weather_schema --database_env='DEV' --database_host='LOCAL'

"""
import argparse

import click
from dotenv import load_dotenv  # type: ignore

from demeter.db import getConnection

from .._initialize import initialize_weather_schema

if __name__ == "__main__":

    c = load_dotenv()

    parser = argparse.ArgumentParser(description="Initialize weather schema instance.")

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

    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env

    if args.drop_existing:
        drop_existing = True
    else:
        drop_existing = False

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert database_env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    database_env_name = f"DEMETER-{database_env}_{database_host}_SUPER"

    print(f"Connecting to database: {database_env_name}")

    conn = getConnection(env_name=database_env_name)

    if drop_existing is True:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
        else:
            print("Continuing command with `drop_existing` set to False.")
            drop_existing = False

    _ = initialize_weather_schema(conn, drop_existing)
