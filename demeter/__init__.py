from typing import Union

from .connections import getPgConnection

from . import data
from . import task
from . import work

__all__ = [
  'data',
  'task',
  'work',
  'getPgConnection',
]

from .type_lookups import type_table_lookup, data_table_lookup, key_table_lookup

assert(set(AnyTypeTable.__args__) == set(type_lookups.type_table_lookup.keys())) # type: ignore

assert(set(AnyDataTable.__args__) == set(type_lookups.data_table_lookup.keys())) # type: ignore

assert(set(AnyKeyTable.__args__) == set(type_lookups.key_table_lookup.keys())) # type: ignore

