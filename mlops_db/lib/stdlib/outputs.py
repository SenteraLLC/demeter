from typing import Union, Generic, Iterable, Any

from .future import Deferred, T, Future

# TODO: Remove
from typing import TypeVar

LoneArg = Union[T, Deferred[T]]
IterableArg = Union[Iterable[T], Iterable[Deferred[T]]]
Arg = Union[LoneArg[T], IterableArg[T], Future[T,Any]]

# TODO: TypeVar
A = TypeVar('A', bound=Arg)

from dataclasses import dataclass

class Outputs(Generic[A]):
  def __init__(self):
    self.lone : List[A] = []
    self.iterables : List[Iterable[A]] = []


