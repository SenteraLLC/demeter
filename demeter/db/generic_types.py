from typing import Optional, Callable, TypeVar, Any, Union, Mapping, Type

from . import TableId, Table, TableKey
from . import union_types

T = TypeVar('T', bound=Table)

R = TypeVar('R')
InsertFn = Callable[[Any, T], R]

I = TypeVar('I', bound=union_types.AnyIdTable)
GetId = Callable[[Any, I], Optional[TableId]]
GetTable = Callable[[Any, TableId], I]
ReturnId = Callable[[Any, I], TableId]

IdFunction = Union[GetId[I], GetTable[I], ReturnId[I]]


S = TypeVar('S', bound=union_types.AnyKeyTable)
SK = TypeVar('SK', bound=TableKey)

ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]

GetTableByKey = Callable[[Any, SK], Optional[S]]

KeyFunction = Union[ReturnKey[S, SK], ReturnSameKey[S]]
AnyFunction = Union[IdFunction[I], KeyFunction[S, SK]]
TypeToFunction = Mapping[Type[T], AnyFunction[I, S, SK]]

