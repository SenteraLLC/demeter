from typing import Type, Any, Mapping, Union, Awaitable, Optional, Set, List, Coroutine, Tuple, AsyncGenerator
from typing import cast

from .future import T, R

from ..util.api_protocols import ReturnId, ReturnKey
#from ..lib.util.exceptions impo
from .write_fn import WriteFn, TypeToOutputs
from .exceptions import NotNullViolationException
from collections import OrderedDict as Dict

import asyncio
from asyncio import Task

# TODO: Memo

import inspect

TypeToTaskToDeferred = Dict[Type[T], Dict[Task[T], Any]]

class GetFn():
  def __init__(self,
               type_to_results : Mapping[Type[T], Mapping[T, Any]],
               type_to_task_to_deferred : TypeToTaskToDeferred[T],
               write_fn        : WriteFn,
              ) -> None:
    self.type_to_results = type_to_results
    self.write_fn = write_fn
    self.type_to_task_to_deferred = type_to_task_to_deferred
    # TODO: Verify that sources have insert functions


  async def _getDeferredSource(self, typ : Type[T], source : Awaitable[T]) -> T:
    self.write_fn.queueDeferred(typ, [source])

    maybe_output : Optional[T] = None
    while maybe_output is None:
      type_to_deferred_to_task = self.write_fn.type_to_deferred_to_task
      # TODO: This could be made much more efficient
      if (
          (deferred_to_task := type_to_deferred_to_task.get(typ)) and
          (task := deferred_to_task.get(source)) and
          (task_to_deferred := self.type_to_task_to_deferred.get(typ)) and
          (deferred := task_to_deferred.get(task))
         ):
            maybe_output = deferred
      else:
        await asyncio.sleep(1)
    return maybe_output


  async def _getSource(self,
                      typ : Type[T],
                      source : Union[T, Awaitable[T]],
                     ) -> Tuple[Type[T], T]:
    maybe_resolved_source : Optional[T] = None

    if inspect.isawaitable(source):
      source = cast(Awaitable[T], source)
      maybe_resolved_source = await self._getDeferredSource(typ, source)
    else:
      maybe_resolved_source = cast(T, source)

    resolved_source = cast(T, maybe_resolved_source)
    source_type = cast(Type[T], type(resolved_source))

    return source_type, resolved_source


  async def _waitForResults(self,
                            source_type : Type[T],
                            resolved_source : T,
                           ) -> R:
    maybe_result : Optional[R] = None

    while maybe_result is None:
      maybe_results = self.type_to_results.get(source_type)
      if not ((results := maybe_results) and
              (maybe_result := results.get(resolved_source))
             ):
        await asyncio.sleep(1)

    result = maybe_result
    return cast(R, result)


  # TODO: Doesn't type-check between typ and source
  async def __call__(self,
                     typ : Type[T],
                     source : Union[T, Awaitable[T]],
                     default : Optional[T] = None,
                    ) -> Optional[R]:
    if source is None:
      if default is None:
        raise NotNullViolationException()
      else:
        return default
    source_type, resolved_source = await self._getSource(typ, source)
    # TODO: Warning if this type wasn't ever written
    #         Missing dependency
    results : R = await self._waitForResults(typ, resolved_source)
    return results

