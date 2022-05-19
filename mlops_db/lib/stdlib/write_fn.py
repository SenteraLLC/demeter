from typing import Union, Iterable, List, Generic, TypeVar, Dict, Type, Optional
from typing import cast

from collections import OrderedDict

from ..util.types_protocols import Table

from .future import Deferred, T
from .outputs import Outputs, LoneArg, Arg, IterableArg


TypeToOutputs = Dict[Type[Table], Outputs[Table]]
TypeToDeferred = Dict[Type[Table],
                      Outputs[Deferred[Table]],
                     ]

S = TypeVar('S', bound=Table)


def isDeferred(value : LoneArg[T]):
  if isinstance(value, Deferred):
    return True
  return False


class WriteFn():
  def __init__(self):
    self.type_to_outputs : TypeToOutputs = OrderedDict()
    self.type_to_deferred : TypeToDeferred = OrderedDict()

  def __call__(self, t : Type[S], *args : Arg[S]):
    is_iterable = False
    maybe_lone_arg : Optional[LoneArg] = None
    for arg in args:
      try:
        it = iter(cast(IterableArg, arg))
        is_iterable = True
        try:
          maybe_lone_arg = cast(Deferred[S], next(it))
        except StopIteration:
          return
      except TypeError:
        maybe_lone_arg = cast(LoneArg[S], arg)

      lone_arg = cast(LoneArg[S], maybe_lone_arg)
      is_deferred = isDeferred(lone_arg)

      if is_deferred:
        if t not in self.type_to_deferred:
          self.type_to_deferred[t] = Outputs()
        deferred = self.type_to_deferred[t]
        if maybe_lone_arg is not None:
          deferred_lone_arg = cast(Deferred[Table], maybe_lone_arg)
          deferred.lone.append(deferred_lone_arg)
        deferred_list = cast(Iterable[Deferred[Table]], arg if is_iterable else [arg])
        deferred.iterables.append(deferred_list)
      else:
        if t not in self.type_to_outputs:
          self.type_to_outputs[t] = Outputs()
        outputs = self.type_to_outputs[t]
        if maybe_lone_arg is not None:
          outputs_first = cast(Table, maybe_lone_arg)
          outputs.lone.append(outputs_first)
        output_list = cast(Iterable[Table], arg if is_iterable else [arg])
        outputs.iterables.append(output_list)


