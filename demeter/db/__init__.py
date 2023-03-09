from typing import TypeAlias

from psycopg2.extensions import connection

from ._base_types import (
    Detailed,
    SelfKey,
    SomeKey,
    Table,
    TableId,
    TableKey,
    TypeTable,
)
from ._initialize import initialize_demeter_instance
from ._json_type import EMPTY_JSON, JSON
from ._postgres.connection import (  # unlikely that anything besides Postgres will ever be supported
    getConnection,
    getEngine,
    getEnv,
    getExistingSearchPath,
    getSession,
)
from ._postgres.generator import SQLGenerator
from ._postgres.insert import generateInsertMany
from ._postgres.tools import doPgFormat, doPgJoin
from ._register import register_sql_adapters

Connection: TypeAlias = connection


__all__ = [
    "initialize_demeter_instance",
    "getConnection",
    "getEngine",
    "getExistingSearchPath",
    "getSession",
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
