import os.path
from io import TextIOWrapper
from typing import Callable

this_dir = os.path.dirname(__file__)
open_sql: Callable[[str], TextIOWrapper] = lambda filename: open(
    os.path.join(this_dir, filename)
)

stmt = open_sql("getLatestFunctionSignature.sql")
