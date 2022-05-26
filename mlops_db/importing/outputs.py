from typing import Union, Iterable, Awaitable

from ..lib.util.types_protocols import T

LoneArg = Union[T, Awaitable[T]]
IterableArg = Union[Iterable[T], Iterable[Awaitable[T]]]
Arg = Union[LoneArg[T], IterableArg[T]]

