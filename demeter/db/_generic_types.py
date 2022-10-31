from typing import Any, Callable, Mapping, Optional, Type, TypeVar, Union

from . import _base_types as base_types
from . import _union_types as union_types

T = TypeVar("T", bound=base_types.Table)

from typing import Generic, Tuple


# TODO: MyPy can't resolve some generics at runtime
#       Find a way to delete this ASAP
class RTTI(Generic[T]):
    def __init__(self, typ: Type[T]) -> None:
        self.typ = typ


R = TypeVar("R")
InsertFn = Callable[[Any, T], R]

I = TypeVar("I", bound=union_types.AnyIdTable)
GetId = Callable[[Any, I], Optional[base_types.TableId]]
GetTable = Callable[[Any, base_types.TableId], I]
ReturnId = Callable[[Any, I], base_types.TableId]

IdFunction = Union[GetId[I], GetTable[I], ReturnId[I]]


S = TypeVar("S", bound=union_types.AnyKeyTable)
SK = TypeVar("SK", bound=base_types.TableKey)

ReturnKey = Callable[[Any, S], SK]
ReturnSameKey = Callable[[Any, S], S]

GetTableByKey = Callable[[Any, SK], Optional[S]]

KeyFunction = Union[ReturnKey[S, SK], ReturnSameKey[S]]
AnyFunction = Union[IdFunction[I], KeyFunction[S, SK]]
TypeToFunction = Mapping[Type[T], AnyFunction[I, S, SK]]
