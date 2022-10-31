from typing import List, TypeAlias

from psycopg2.extensions import connection

from . import _generic_types as generic_types
from ._base_types import (Detailed, SelfKey, SomeKey, Table, TableId, TableKey,
                          TypeTable)
from ._json_type import *
# It is unlikely that anything besides Postgres will ever be supported
from ._postgres import getConnection as getConnection

Connection: TypeAlias = connection


__all__ = [
    "getConnection",
    "Connection",
    "JSON",
    "EMPTY_JSON",
    # Base Types
    "Table",
    "Detailed",
    "TypeTable",
    "TableKey",
    "TableId",
    "SelfKey",
    "SomeKey",
]

from ._register import register_sql_adapters

register_sql_adapters()
