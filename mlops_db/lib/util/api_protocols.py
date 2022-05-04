from typing import Optional, Callable, TypeVar, Any

from .type_lookups import AnyIdTable, AnyIdTable, AnyKeyTable
from .types_protocols import TableKey


U = TypeVar('U', bound=AnyIdTable)
GetId = Callable[[Any, U], Optional[int]]

V = TypeVar('V', bound=AnyIdTable)
GetTable = Callable[[Any, int], V]

T = TypeVar('T', bound=AnyIdTable)
ReturnId = Callable[[Any, T], int]

# TODO: Fix typing issues here
S = TypeVar('S', bound=AnyKeyTable)
SK = TypeVar('SK', bound=TableKey)
ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]
