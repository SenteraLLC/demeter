"""Daily add/update of Demeter weather database based on Demeter field data.

This is the working draft of a script that would execute on daily weather extraction for `Demeter`.

STEPS:
1. Run "update" on those cell IDs which already exist in the database
2. Run "add" on those cell ID x year combinations which are new
3. Run "fill" to fill in any gaps in the database

A few notes:
- We extract "update" first because it is bound to have far fewer requests.

Example usage:
python3 -m scripts.weather.run_daily_weather

"""
import argparse
import logging

from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter.db import getConnection

from .defaults import (
    N_CELLS_FILL,
    N_CELLS_PER_SET_ADD,
    N_CELLS_PER_SET_UPDATE,
    PARAMETER_SETS,
)
from .main import run_daily_weather

if __name__ == "__main__":
    """Run daily weather processing."""
    c = load_dotenv()
    logging_init()

    parser = argparse.ArgumentParser(
        description="Run daily weather processing to update/add/fill weather data in Demeter."
    )

    parser.add_argument(
        "--database_host",
        type=str,
        help="Host of demeter database; can be 'AWS' or 'LOCAL'.",
        default="LOCAL",
    )

    parser.add_argument(
        "--database_env",
        type=str,
        help="Database instance; can be 'DEV' or 'PROD'.",
        default="DEV",
    )

    parser.add_argument(
        "--fill",
        action="store_true",
        help="Should the 'fill' step be run?",
        default=False,
    )

    parser.add_argument(
        "--in_parallel",
        action="store_true",
        help="Should requests be run in parallel?",
        default=False,
    )

    # set up args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env

    if args.fill:
        fill = True
    else:
        fill = False

    if args.in_parallel:
        parallel = False
        logging.info(
            "Parallelization has not yet been implemented. Setting `parallel` to False."
        )
    else:
        parallel = False

    assert database_host in ["AWS", "LOCAL"], "`database_host` can be 'AWS' or 'LOCAL'"
    assert database_env in ["DEV", "PROD"], "`database_env` can be 'DEV' or 'PROD'"

    ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    database_env_name = f"DEMETER-{database_env}_{database_host}"

    logging.info("Getting connection")
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # TODO: Enable this CLI to take a configuration file name to customize `run_daily_weather()`?
    logging.info("Starting weather process")
    run_daily_weather(
        conn=conn,
        parameter_sets=PARAMETER_SETS,
        n_cells_per_set_add=N_CELLS_PER_SET_ADD,
        n_cells_per_set_update=N_CELLS_PER_SET_UPDATE,
        n_cells_fill=N_CELLS_FILL,
        fill=fill,
        parallel=parallel,
    )
