from typing import Union
from typing import get_origin, get_args

from ..union_types import AnyTable

def is_none(table : AnyTable, key : str) -> bool:
  return getattr(table, key) is None

def is_optional(table : AnyTable, key : str) -> bool:
  field = table.__dataclass_fields__[key].type
  return get_origin(field) is Union and \
           type(None) in get_args(field)

