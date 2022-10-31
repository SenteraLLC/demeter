import argparse
import os
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from dotenv import load_dotenv

from demeter.db import getConnection

from .data_option import DataOption
from .log import setupLogger
from .main import main


def to_str(d: DataOption) -> str:
    return d.name.lower()


DATA_OPTIONS = {to_str(d) for d in DataOption}


def parseDataOption(s: str) -> DataOption:
    try:
        return DataOption[s.upper()]
    except KeyError:
        raise argparse.ArgumentTypeError(f"Bad data option: {s} not in {DATA_OPTIONS}")


def check_dir(d: str) -> str:
    if not os.path.isdir(d):
        raise argparse.ArgumentTypeError(f"Path is not a directory: {d}")
    return d


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Use interactive menus to get Demeter data"
    )

    parser.add_argument(
        "--output_directory",
        type=str,
        help="The output directory in which to store files",
        required=True,
    )
    parser.add_argument(
        "--targets",
        type=str,
        nargs="+",
        choices=DATA_OPTIONS,
        help="Data to be used as filters for the target type",
        default=[],
    )
    parser.add_argument(
        "--log_directory",
        type=str,
        help="The directory in which to store logs",
        default="/tmp/",
    )
    args = parser.parse_args()
    targets = [parseDataOption(f) for f in args.targets]

    output_directory = check_dir(args.output_directory)
    log_directory = check_dir(args.log_directory)
    setupLogger(log_directory)

    print("ARGS: ", args)

    connection = getConnection()
    cursor = connection.cursor()
    main(cursor, targets, output_directory)
