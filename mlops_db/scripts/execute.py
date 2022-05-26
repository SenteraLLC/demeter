from typing import Dict, Type, Mapping, Union, Any, List
from typing import cast

from collections import OrderedDict
import asyncio

from ..lib.stdlib.import_plan import ImportPlan, FiveArgs
from ..lib.stdlib.find_fn import FindFn
from ..lib.stdlib.write_fn import WriteFn, TypeToOutputs, TypeToDeferred
from ..lib.stdlib.get_fn import GetFn, TypeToTaskToDeferred
from ..lib.stdlib.next_fn import NextFn
from ..lib.stdlib.exceptions import NotNullViolationException, BadGeometryException
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
        print("REGULAR RESULT:\n",o,"\n",result)


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
        #print("CHECKING TASK: ",task)
        if task.done():
          #print("  DONE.")
          try:
            o = task.result()
            print("DEFERRED OUT: ",o)
            result = insert_fn(cursor, o)
            print("DEFERRED RESULT: ",typ,"\n",result,"\n",task)
            type_to_output_to_result[typ][o] = result
            type_to_task_to_deferred[typ][task] = o
            #print("ALL TTR: ",type_to_task_to_deferred)
          except StopIteration:
            print("DEFF Stopping iteration\n")
          except NotNullViolationException as e:
            #sys.stderr.write(f"Not null violation: {e}\n")
            print(f"DEFF Null\n ",e)
          except BadGeometryException as e:
            #sys.stderr.write(f"Bad geo: {e}\n")
            print(f"DEFF Geo\n")
          else:
            print(f"Success\n")
          #print("DEFERRED RESULT: ",o)
        else:
          #print("KEEP GOING.")
          still_deferred.append(task)

    if len(still_deferred):
      out[typ] = [still_deferred]
    else:
      print("Completed: ",typ)
      s = set(type_to_deferred.keys()) - {typ}
      print("   Remaining: ",s)
  return out




async def executePlan(cursor : Any,
                      plan : ImportPlan,
                      type_to_insert_fn : TypeToInsert,
                     ):
  get_type_iterator = plan.get_type_iterator
  find_fn = FindFn(get_type_iterator)

  for next_typ, fn in plan.steps:
    print("START STEP: ",fn)
    type_to_output_to_result : TypeToOutputToResult = {}
    type_to_task_to_deferred : TypeToTaskToDeferred = OrderedDict()

    next_fn = NextFn(get_type_iterator(next_typ))

    count = 0
    while True:
    #for i in range(0, 100):
      fn = cast(FiveArgs, fn)
      write_fn = WriteFn()
      get_fn = GetFn(type_to_output_to_result, type_to_task_to_deferred, write_fn)

      try:
        count += 1
        print("COUNT IS: ",count)
        fn(next_fn, find_fn, write_fn, get_fn, plan.args)
        sys.stdout.flush()
      except StopIteration:
        print("Done with step: ",fn)
        print("   Took: ",count)
        break
      except NotNullViolationException as e:
        print(f"Outputs Null\n")
      except BadGeometryException as e:
        print(f"Outputs Bad Geo\n")
      else:
        #print(f"Outputs Success\n")
        pass

      type_to_outputs= write_fn.type_to_outputs
      insertOutputs(cursor, type_to_outputs, type_to_output_to_result, type_to_insert_fn)

      type_to_deferred : TypeToDeferred = write_fn.getLatest()
      while len(type_to_deferred):
        type_to_deferred = getDeferred(cursor, type_to_deferred, type_to_output_to_result, type_to_task_to_deferred, type_to_insert_fn)
        await asyncio.sleep(0.1)
        type_to_deferred = write_fn.getLatest(type_to_deferred)




