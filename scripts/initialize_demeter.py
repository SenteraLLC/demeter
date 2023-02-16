"""Initializes (or re-initializes) a Demeter schema instance for a given host and database environment.

This script requires that you have the appropriate superuser credentials for the database in your .env file, as well as
the necessary passwords for the `demeter_user` and `demeter_ro_user`.
"""
import argparse
import sys

import click
from dotenv import load_dotenv  # type: ignore

from demeter.db import getConnection, initializeDemeterInstance

if __name__ == "__main__":
    c = load_dotenv()

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
            print("Connecting to AWS database instance.")
        else:
            sys.exit()

    if drop_existing:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
        else:
            print("Continuing command with `drop_existing` set to False.")
            drop_existing = False

    ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    database_env_name = f"DEMETER-{database_env}_{database_host}_SUPER"

    # set up database connection
    print(f"Connecting to database: {database_env_name}")
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    _ = initializeDemeterInstance(conn, schema_name, drop_existing)
