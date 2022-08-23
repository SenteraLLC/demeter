from typing import List

from ._base_types import Table, Detailed, TypeTable, TableKey, TableId, SelfKey, SomeKey
from ._json_type import *
from . import _generic_types as generic_types

# It is unlikely that anything besides Postgres will ever be supported
from ._postgres import getConnection as getConnection
from psycopg2.extensions import connection

from typing import TypeAlias
Connection : TypeAlias = connection


__all__ = [
  'getConnection',
  'Connection',

  # Base Types
  'Table',
  'Detailed',
  'TypeTable',
  'TableKey',
  'TableId',
  'SelfKey',
  'SomeKey',

  'JSON',
  'EMPTY_JSON',
]

from ._register import register_sql_adapters
register_sql_adapters()

