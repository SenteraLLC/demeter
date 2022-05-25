from typing import Union, Iterable, Awaitable

from .future import T

LoneArg = Union[T, Awaitable[T]]
IterableArg = Union[Iterable[T], Iterable[Awaitable[T]]]
Arg = Union[LoneArg[T], IterableArg[T]]

