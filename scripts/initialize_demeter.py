# %%
import argparse

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
        "--drop_existing",
        type=bool,
        help="Should the schema be re-created if it exists?",
        default=False,
    )

    args = parser.parse_args()
    schema_name = args.schema_name
    database_host = args.database_host
    drop_existing = args.drop_existing

    if database_host == "AWS":
        conn = getConnection(env_name="DEMETER_AWS")
    else:
        conn = getConnection(env_name="DEMETER")

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"

    if drop_existing:
        if click.confirm(
            "Are you sure you want to drop existing schema?", default=False
        ):
            pass
        else:
            print("Continuing command with `drop_existing` set to False.")
            drop_existing = False

    _ = initializeDemeterInstance(conn, schema_name, drop_existing)
