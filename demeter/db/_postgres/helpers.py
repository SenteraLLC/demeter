from typing import Union
from typing import get_origin, get_args

from .._union_types import AnyTable

def is_none(table : AnyTable, key : str) -> bool:
  return getattr(table, key) is None

def is_optional(table : AnyTable, key : str) -> bool:
  f = table.__dataclass_fields__[key]
  t = f.type
  return (get_origin(t) is Union and \
           type(None) in get_args(t)
         ) or f.hash is False


import datetime

def is_date_or_time(table : AnyTable, key : str) -> bool:
  f = table.__dataclass_fields__[key]
  t = f.type
  return t in [datetime.time, datetime.date]

