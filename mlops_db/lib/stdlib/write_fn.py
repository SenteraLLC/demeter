from typing import Union, Iterable, List, TypeVar, Dict, Type, Optional, Any, Awaitable, Coroutine, Sequence, Tuple, Iterator, Set
from typing import cast

from collections import OrderedDict

from ..util.types_protocols import Table

from .future import T, R

from .outputs import LoneArg, Arg, IterableArg

from asyncio import Task
from asyncio import create_task

import inspect


# These are nested lists to minimize job queueing
#   The user can memo their insert functions and create indices
TypeToOutputs = Dict[Type, List[List[T]]]
TypeToDeferred = Dict[Type[T],
                      List[List[Task]],
                     ]
TypeToDeferredToTask = Dict[Type[T], Dict[Awaitable[T], Task[T]]]


def isDeferred(value : LoneArg):
  if inspect.isawaitable(value):
    return True
  return False


class WriteFn():
  def __init__(self) -> None:
    #self.type_to_outputs : TypeToOutputs = OrderedDict()
    self.type_to_deferred_to_task : TypeToDeferredToTask = {}

    self.latest_type_to_deferred : TypeToDeferred = OrderedDict()
    self.latest_type_to_outputs : TypeToOutputs = OrderedDict()


  def _addToLatest(self, latest : TypeToDeferred, existing : TypeToDeferred):
    if len(latest) <= 0:
      return existing
    for existing_typ, existing_deferred in existing.items():
      try:
        latest[existing_typ].extend(existing_deferred)
      except KeyError:
        latest[existing_typ] = existing_deferred
    return latest


  def getDeferred(self, existing : Optional[TypeToDeferred] = None) -> TypeToDeferred:
    latest = self.latest_type_to_deferred
    if existing is not None:
      latest = self._addToLatest(latest, existing)
    self.latest_type_to_deferred = OrderedDict()
    return latest


  def getOutputs(self) -> TypeToDeferred:
    latest = self.latest_type_to_outputs
    self.latest_type_to_outputs = OrderedDict()
    return latest


  def queueDeferred(self,
                    t : Type[T],
                    arg : Iterable[Awaitable[T]],
                   ):
    #if t not in self._type_to_deferred:
    #  self._type_to_deferred[t] = []
    if t not in self.latest_type_to_deferred:
      self.latest_type_to_deferred[t] = []

    if t not in self.type_to_deferred_to_task:
      self.type_to_deferred_to_task[t] = OrderedDict()

    existing_tasks = self.type_to_deferred_to_task[t]

    deferred_list = cast(List[Coroutine], arg)
    task_list : List[Task[T]] = []
    for d in deferred_list:
      if d not in existing_tasks:
        task = create_task(d)
        self.type_to_deferred_to_task[t][d] = task
        task_list.append(task)

    #self._type_to_deferred[t].append(task_list)
    self.latest_type_to_deferred[t].append(task_list)


  def queueOutputs(self,
                   t : Type[T],
                   arg : IterableArg[T],
                  ) -> None:
    if t not in self.latest_type_to_outputs:
      self.latest_type_to_outputs[t] = [[]]

    outputs = self.latest_type_to_outputs[t]
    outputs_list = cast(List[T], arg)
    outputs.append(outputs_list)


  def _isDeferred(self, it : Iterator[T]) -> Tuple[bool, Optional[LoneArg[T]]]:
    # We need to 'reinsert' this first item if it's a generator
    maybe_first : Optional[LoneArg[T]] = None
    is_deferred = False
    try:
      maybe_first = next(it)
      first = maybe_first
      # Assume homogenous
      is_deferred = isDeferred(first)
    except StopIteration:
      pass
    return is_deferred, maybe_first

  # TODO: Changes hash for deferred, will mess some stuff up
  def _fixGenerator(self, it : Iterator[T], first : T) -> Iterable[T]:
    def reattach_first():
      yield first
      for t in it:
        yield t
    return reattach_first()


  def _toIterableArg(self, arg : Arg[T]) -> Tuple[bool, IterableArg[T]]:
    is_deferred = False
    maybe_output : Optional[IterableArg[T]] = None
    try:
      maybe_it = cast(IterableArg[T], arg)
      it = cast(Iterator[T], iter(maybe_it))
      maybe_output = maybe_it

      is_deferred, maybe_first = self._isDeferred(it)

      is_generator = inspect.isgenerator(arg)
      if is_generator and maybe_first is not None:
        first = cast(T, maybe_first)
        maybe_output = self._fixGenerator(it, first)
    except TypeError:
      lone_arg = cast(LoneArg[T], arg)
      is_deferred = isDeferred(lone_arg)
      maybe_output = cast(IterableArg[T], [lone_arg])

    output = cast(IterableArg[T], maybe_output)

    return is_deferred, output


  def __call__(self, typ : Type[T], *args : Arg[T]) -> Sequence[Task[T]]:
    out : Sequence[Task[T]] = []
    for arg in args:
      is_deferred, output = self._toIterableArg(arg)

      if is_deferred:
        output = cast(Iterable[Awaitable[T]], output)
        self.queueDeferred(typ, output)
      else:
        self.queueOutputs(typ, output)
    return out

