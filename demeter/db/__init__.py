from typing import List

from . import union_types
from . import generic_types

from .base_types import *
from .json_type import *

# It is unlikely that anything besides Postgres will ever be supported
from .postgres import SQLGenerator as Generator
from .postgres.tools import *

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

  # Special Developer Tools
  'union_types',
  'generic_types',
  'postgres',
]

from .register import register_sql_adapters
register_sql_adapters()


