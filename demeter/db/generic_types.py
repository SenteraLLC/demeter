from typing import Optional, Callable, TypeVar, Any, Union, Mapping, Type

from .union_types import AnyIdTable, AnyKeyTable
from .base_types import Table, TableKey, TableId

T = TypeVar('T', bound=Table)

R = TypeVar('R')
InsertFn = Callable[[Any, T], R]

I = TypeVar('I', bound=AnyIdTable)
GetId = Callable[[Any, I], Optional[TableId]]
GetTable = Callable[[Any, TableId], I]
ReturnId = Callable[[Any, I], TableId]

IdFunction = Union[GetId[I], GetTable[I], ReturnId[I]]

S = TypeVar('S', bound=AnyKeyTable)
SK = TypeVar('SK', bound=TableKey)

ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]

KeyFunction = Union[ReturnKey[S, SK], ReturnSameKey[S]]
AnyFunction = Union[IdFunction[I], KeyFunction[S, SK]]
TypeToFunction = Mapping[Type[T], AnyFunction[I, S, SK]]

