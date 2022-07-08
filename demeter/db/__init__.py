from typing import List

#from .base_types import Table, Detailed, TypeTable, TableKey, TableId, SelfKey, SomeKey
from .base_types import *
#from .generic_types import GetId, ReturnId, ReturnKey, ReturnSameKey, GetTable
#from .union_types import AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable, AnyTable
from . import union_types
from . import lookup_types
from . import generic_types

#from .type_to_sql import generateInsertMany as generateInsertMany, \
#                         getInsertReturnIdFunction as getInsertReturnIdFunction, \
#                         getInsertReturnSameKeyFunction as getInsertReturnSameKeyFunction, \
#                         getInsertReturnKeyFunction as getInsertReturnKeyFunction, \
#                         getMaybeId as getMaybeId, \
#                         getMaybeIdFunction as getMaybeIdFunction, \
#                         getMaybeTableById as getMaybeTableById, \
#                         getTableFunction as getTableFunction, \
#                         getTableById as getTableById, \
#                         getTableByKey as getTableByKey, \
#                         getTableKeyFunction as getTableKeyFunction, \
#                         partialInsertOrGetId as partialInsertOrGetId

from .json_type import JSON as JSON, \
                       EMPTY_JSON as EMPTY_JSON

# It is unlikely that anything besides Postgres will ever be supported
from .postgres import SQLGenerator as Generator
from .postgres.tools import PGJoin as PGJoin, \
                            PGFormat as PGFormat

__all__ = [
  'JSON',
  'EMPTY_JSON',

  'Generator',
  'PGJoin',
  'PGFormat',

  # Base Types
  'Table',
  'Detailed',
  'TypeTable',
  'TableKey',
  'TableId',
  'SelfKey',
  'SomeKey',

#  # Generic Types
#  'GetId',
#  'GetTable',
#  'ReturnId',
#  'ReturnKey',
#  'ReturnSameKey',

#  # Type to SQL-functions
#  'generateInsertMany',
#  'getInsertReturnIdFunction',
#  'getInsertReturnSameKeyFunction',
#  'getInsertReturnKeyFunction',
#  'getMaybeId',
#  'getMaybeIdFunction',
#  'getMaybeTableById',
#  'getTableFunction',
#  'getTableById',
#  'getTableByKey',
#  'getTableKeyFunction',
#  'partialInsertOrGetId',

  # Special Developer Tools
  'union_types',
  'lookup_types',
  'generic_types',
  'postgres',
]

from .register import register_sql_adapters
register_sql_adapters()

