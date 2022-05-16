from typing import TypedDict, Type, Dict, TypeVar, Any, Callable, Set, Optional, Union, Mapping, Iterable, Generic, Sequence, Iterator, Tuple, List
from typing import get_origin, get_args

import sys
from functools import wraps

from ..lib.util.types_protocols import Table, AnyKey

from .imports import Import, I


class InvalidRowException(Exception):
  pass

class BadGeometryException(InvalidRowException):
  pass


from abc import ABC, abstractmethod
from typing import cast

V = TypeVar('V')
E = TypeVar('E')

class ExpectedBase(Generic[V, E], ABC):
  @abstractmethod
  def __init__(self):
    self.maybe_value : Optional[V] = None
    self.maybe_error : Optional[E] = None
    self.has_error = False

  def __bool__(self) -> bool:
    return not self.has_error

  def error(self) -> E:
    raise Exception("No error exists.")


class UnexpectedTemplate(ExpectedBase[V, E]):
  def __init__(self, error : E):
    super().__init__()
    self.maybe_error = error
    self.has_error = True

  def error(self) -> E:
    return cast(E, self.maybe_error)

Unexpected = UnexpectedTemplate[V, str]


class ExpectedTemplate(ExpectedBase[V, E]):
  def __init__(self, value : V):
    super().__init__()
    self.maybe_value = value

  def __call__(self) -> V:
    return cast(V, self.maybe_value)

Expected = ExpectedTemplate[V, str]


T = TypeVar('T')
K = TypeVar('K', bound=AnyKey)

class UnsetFutureException(Exception):
  pass

class Future(Generic[T, K]):
  def __init__(self, v : T):
    self.to_insert = v
    self.result : Optional[K] = None
    self.was_set = False

  def set(self, result : K) -> None:
    self.result = result
    self.was_set = True

  def get(self) -> Optional[K]:
    if not self.was_set:
      raise UnsetFutureException()
    if isinstance(self.result, Deferred):
      return self.result()
    return self.result


class FutureRequired(Future[T, K]):
  def get(self) -> K:
    if self.result is None:
      raise InvalidRowException
    return cast(K, super().get())


# TODO: Deferred is a type of Future
class Deferred(Generic[T]):
  def __init__(self, fn : Callable[[], T]):
    self.fn = fn
    self.maybe_result : Optional[T] = None

  def __call__(self) -> T:
    r = self.maybe_result
    if r is not None:
      return r
    r = self.fn()
    self.maybe_result = r
    return r


LoneArg = Union[T, Deferred[T]]
IterableArg = Union[Iterable[T], Iterable[Deferred[T]]]
Arg = Union[LoneArg[T], IterableArg[T]]

def isDeferred(value : LoneArg[T]):
  if isinstance(value, Deferred):
    return True
  return False



A = TypeVar('A', bound=Arg)

from dataclasses import dataclass

class Outputs(Generic[A]):
  def __init__(self):
    self.lone : List[A] = []
    self.iterables : List[Iterable[A]] = []


TypeToOutputs = Dict[Type[Table], Outputs[Table]]
TypeToDeferred = Dict[Type[Table], Outputs[Deferred[Table]]]


S = TypeVar('S', bound=Table)

class WriteFn():
  def __init__(self):
    self.type_to_outputs : TypeToOutputs = {}
    self.type_to_deferred : TypeToDeferred = {}

  def __call__(self, t : Type[S], *args : Arg[S]):
    is_iterable = False
    maybe_first : Optional[LoneArg] = None
    for arg in args:
      try:
        it = iter(cast(IterableArg, arg))
        is_iterable = True
        try:
          maybe_first = cast(Deferred[S], next(it))
        except StopIteration:
          return
      except TypeError:
        lone_arg = cast(LoneArg[S], arg)
        is_deferred = isDeferred(lone_arg)

      first = cast(LoneArg[S], maybe_first)
      is_deferred = isDeferred(first)

      if is_deferred:
        if t not in self.type_to_deferred:
          self.type_to_deferred[t] = Outputs()
        deferred = self.type_to_deferred[t]
        if maybe_first is not None:
          deferred_first = cast(Deferred[Table], maybe_first)
          deferred.lone.append(deferred_first)
        deferred_list = cast(Iterable[Deferred[Table]], arg if is_iterable else [arg])
        deferred.iterables.append(deferred_list)
      else:
        if t not in self.type_to_outputs:
          self.type_to_outputs[t] = Outputs()
        outputs = self.type_to_outputs[t]
        if maybe_first is not None:
          outputs_first = cast(Table, maybe_first)
          outputs.lone.append(outputs_first)
        output_list = cast(Iterable[Table], arg if is_iterable else [arg])
        outputs.iterables.append(output_list)




TypeToIterator = Dict[Type[Import], Iterator[Import]]
#TypeToIterator = Dict[Type, Iterator]
GetTypeIterator = Callable[[Type[Import]], Iterator[Import] ]
#GetTypeIterator = Callable[[Type], Iterator]
GetIteratorTypes = Callable[[],  Set[Type[Import]]]
#GetIteratorTypes = Callable[[],  Set[Type]]

MatchFn = Callable[[I], bool]

from functools import lru_cache as memo

class NextFn(object):
  def __init__(self, type_to_iterator : TypeToIterator):
    self.type_to_iterator = type_to_iterator

  # TODO: Memo messes with dataclass generics
  #       It might work in Python v3.10
  #@memo(maxsize=1)
  def __call__(self, typ : Type[I]) -> I:
    return cast(I, next(self.type_to_iterator[typ]))


from functools import cache

class FindFn(object):
  def __init__(self, get_type_iterator : GetTypeIterator):
    self.get_type_iterator = get_type_iterator

  #@memo(maxsize=2048)
  def __call__(self,
               other_type : Type[I],
               match_fn : MatchFn[I]
              ) -> Optional[I]:
    it = self.get_type_iterator(other_type)
    other_it : Iterator[I] = cast(Iterator[I], it)
    for row in other_it:
      if match_fn(row):
        return cast(Optional[I], row)
    return cast(Optional[I], None)



# TODO: How to get Type for deferred

FourArgs = Callable[[NextFn, FindFn, WriteFn, Any], None]

ThreeArgs = Callable[[NextFn, FindFn, WriteFn], None]

#ImportTransformation = ThreeArgs
ImportTransformation = Union[ThreeArgs, FourArgs]


@dataclass
class Plan():
  get_type_iterator : GetTypeIterator
  steps             : Sequence[ImportTransformation]
  args              : Mapping


def is_optional(field) -> bool:
  return (get_origin(field) is Union and
          type(None) in get_args(field)
         )
