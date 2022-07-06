from typing import List

from .base_types import Table, Detailed, TypeTable, TableKey, TableId, SelfKey, SomeKey
from .generic_types import T, I, S, SK
from .generic_types import GetId, ReturnId, ReturnKey, ReturnSameKey
from .union_types import AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable, AnyTable

from .json import JSON as JSON, EMPTY_JSON as EMPTY_JSON

__all__ = [
  'JSON',
  'EMPTY_JSON',

  # base types
  'Table',
  'Detailed',
  'TypeTable',
  'TableKey',
  'TableId',
  'SelfKey',
  'SomeKey',

  # generic types
  'T',
  'AnyTypeTable',
  'AnyDataTable',
  'AnyIdTable',
  'AnyKeyTable',
  'AnyTable',

  'I',
  'GetId',
  'GetTable',
  'ReturnId',

  'S',
  'SK',
  'ReturnKey',
  'ReturnSameKey',

]


