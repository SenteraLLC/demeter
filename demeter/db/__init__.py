from typing import TypeAlias

from psycopg2.extensions import connection

from ._base_types import Detailed, SelfKey, SomeKey, Table, TableId, TableKey, TypeTable
from ._json_type import JSON, EMPTY_JSON

# It is unlikely that anything besides Postgres will ever be supported
from ._postgres.connection import getConnection, getEnv
from ._postgres.generator import SQLGenerator
from ._postgres.insert import generateInsertMany
from ._postgres.tools import doPgFormat, doPgJoin

from ._register import register_sql_adapters

Connection: TypeAlias = connection


__all__ = [
    "getConnection",
    "getEnv",
    "SQLGenerator",
    "generateInsertMany",
    "doPgFormat",
    "doPgJoin",
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


register_sql_adapters()
