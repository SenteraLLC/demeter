from typing import Dict, Type, Mapping, Union, Any, List
from typing import cast

from collections import OrderedDict
import asyncio

from .import_plan import ImportPlan, FiveArgs
from .find_fn import FindFn
from .write_fn import WriteFn, TypeToOutputs, TypeToDeferred, TypeToDeferredToTask
from .get_fn import GetFn, TypeToTaskToDeferred
from .exceptions import NotNullViolationException, BadGeometryException

from ..lib.util.types_protocols import Table, AnyKey


# TODO: Delete, temporary
import sys

TypeToOutputToResult = Dict[Type, Dict[Table, AnyKey]]

from ..lib.util.api_protocols import ReturnId, ReturnKey

TypeToInsert = Mapping[Type, Union[ReturnId, ReturnKey]]

def insertOutputs(cursor : Any,
                  type_to_outputs : TypeToOutputs,
                  type_to_output_to_result : TypeToOutputToResult,
                  type_to_insert_fn : TypeToInsert,
                 ) -> None:
  # Can at least start writing outputs
  for typ, outputs in type_to_outputs.items():
    try:
      type_to_output_to_result[typ]
    except KeyError:
      type_to_output_to_result[typ] = {}

    insert_fn = type_to_insert_fn[typ]
    # TODO: Mass inserts
    for output_group in outputs:
      for o in output_group:
        result = insert_fn(cursor, o)
        type_to_output_to_result[typ][o] = result


def getDeferred(cursor : Any,
                type_to_deferred : TypeToDeferred,
                type_to_output_to_result : TypeToOutputToResult,
                type_to_task_to_deferred : TypeToTaskToDeferred,
                type_to_insert_fn : TypeToInsert,
               ) -> TypeToDeferred:
  out : TypeToDeferred = {}
  for typ, deferred_groups in type_to_deferred.items():
    try:
      type_to_output_to_result[typ]
    except KeyError:
      type_to_output_to_result[typ] = {}

    try:
      type_to_task_to_deferred[typ]
    except KeyError:
      type_to_task_to_deferred[typ] = OrderedDict()

    insert_fn = type_to_insert_fn[typ]
    still_deferred : List[asyncio.Task] = []
    for deferred_group in deferred_groups:
      for task in deferred_group:
        if task.done():
          try:
            o = task.result()
            result = insert_fn(cursor, o)
            type_to_output_to_result[typ][o] = result
            type_to_task_to_deferred[typ][task] = o
          except NotNullViolationException as e:
            sys.stderr.write(f"Not null violation: {e}\n")
          except BadGeometryException as e:
            sys.stderr.write(f"Bad geo: {e}\n")
        else:
          still_deferred.append(task)

    if len(still_deferred):
      out[typ] = [still_deferred]
    else:
      s = set(type_to_deferred.keys()) - {typ}
  return out


async def executePlan(cursor : Any,
                      plan : ImportPlan,
                      type_to_insert_fn : TypeToInsert,
                      buffer_size = 1000,
                     ):
  get_type_iterator = plan.get_type_iterator
  find_fn = FindFn(get_type_iterator)

  for next_typ, fn in plan.steps:
    type_to_output_to_result : TypeToOutputToResult = {}
    type_to_task_to_deferred : TypeToTaskToDeferred = OrderedDict()

    it = get_type_iterator(next_typ)

    write_fn = WriteFn()
    KeepGoing = True
    while KeepGoing:

      for i in range(0, buffer_size):
        fn = cast(FiveArgs, fn)
        get_fn = GetFn(type_to_output_to_result, type_to_task_to_deferred, write_fn)

        try:
          fn(it, find_fn, write_fn, get_fn, plan.args)
          sys.stdout.flush()
        except StopIteration:
          KeepGoing = False
          break
        except NotNullViolationException as e:
          sys.stderr.write(f"Outputs Null\n")
        except BadGeometryException as e:
          sys.stderr.write(f"Outputs Bad Geo\n")
      else:
        type_to_outputs = write_fn.getOutputs()
        type_to_deferred : TypeToDeferred = write_fn.getDeferred()

        while len(type_to_deferred) or len(type_to_outputs):
          insertOutputs(cursor, type_to_outputs, type_to_output_to_result, type_to_insert_fn)
          type_to_deferred = getDeferred(cursor, type_to_deferred, type_to_output_to_result, type_to_task_to_deferred, type_to_insert_fn)
          await asyncio.sleep(0.1)
          type_to_outputs = write_fn.getOutputs()
          await asyncio.sleep(0.1)



