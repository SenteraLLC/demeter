from typing import Optional, Union, Type, Any, Sequence, Iterator, Mapping, Dict, Callable, TextIO, List, Awaitable
from typing import cast

import psycopg2

import inspect
import sys

from collections import OrderedDict

# TODO: json_stream

from ..lib.core import types as demeter_types
from ..lib.core import api as demeter_api
from ..lib.local import types as local_types
from ..lib.local import api as local_api

from ..lib.stdlib.imports import I, WrapImport

from ..lib.stdlib.next_fn import NextFn, TypeToIterator
from ..lib.stdlib.write_fn import WriteFn, Task, TypeToDeferred
from ..lib.stdlib.find_fn import FindFn
from ..lib.stdlib.get_fn import GetFn, TypeToTaskToDeferred
from ..lib.stdlib.import_plan import FiveArgs, ImportPlan
# TODO: TO REMOVE
from ..lib.stdlib.future import UnsetFutureException, T
from ..lib.stdlib.exceptions import NotNullViolationException, BadGeometryException

from .imports import *

from .migrate_help import makeGrowers, makeGeomAndField, makeIrrigation, makeReviewHarvest, makeReviewQuality

from .migrate_help import MigrateArgs


# TODO: CONFIG
getPath = lambda n : f"./abi_models/contents/{n}.json"

SOURCE_TO_PATH : Mapping[Type[Import], str] = {
  Grower : getPath("Grower"),
  Country : getPath("Country"),
  Region : getPath("Region"),
  GrowerField : getPath("GrowerField"),
  Review : getPath("Review"),
  MeasureUnit : getPath("MeasureUnit"),
  IrrigationApplied : getPath("IrrigationApplied"),
  ReviewHarvest : getPath("ReviewHarvest"),
  ReviewQuality : getPath("ReviewQuality"),
}


from functools import lru_cache as memo
import json

#@memo(maxsize=4)
def getSource(s : Type[I]) -> Sequence[I]:
  p = SOURCE_TO_PATH[s]
  f = open(p)
  j = json.load(f)
  return j


# TODO: jsonstream
def makeJsonIterator(source_type : Type[I]) -> Iterator[I]:
  return (WrapImport(row) for row in getSource(source_type))

def getTypeToIterators() -> TypeToIterator:
#  GrowerIt = makeJsonIterator(Grower)
#  GrowerFieldIt = makeJsonIterator(GrowerField)
#  ReviewIt = makeJsonIterator(Review)
  type_to_iter : TypeToIterator = {
    T : makeJsonIterator(T) for T in SOURCE_TO_PATH.keys()
  }
#    Grower      : GrowerIt,
#    GrowerField : GrowerFieldIt,
#    Review      : ReviewIt,
#    Country     : makeJsonIterator(Country),
#    Region      : makeJsonIterator(Region),
#    MeasureUnit : makeJsonIterator(MeasureUnit),
#    IrriationApplied : makeJsonIterator(IrrigationApplied),
#    ReviewHarvest
  return type_to_iter

def getTypeIterator(typ : Type[Import]) -> Iterator[Import]:
  it = makeJsonIterator(typ)
  return it


from ..lib.util.api_protocols import ReturnId, ReturnKey

# TODO: Memoize these?
TABLE_TO_INSERT_FN : Mapping[Type, Union[ReturnId, ReturnKey]] = {
  demeter_types.Grower : demeter_api.insertOrGetGrower,
  demeter_types.Field : demeter_api.insertOrGetField,
  demeter_types.Geom : demeter_api.insertOrGetGeom,
  local_types.LocalType : local_api.insertOrGetLocalType,
  local_types.UnitType : local_api.insertOrGetUnitType,
  local_types.LocalValue : local_api.insertOrGetLocalValue,
  local_types.LocalValue : local_api.insertOrGetLocalValue,
}
#reveal_type(TABLE_TO_INSERT_FN[demeter_types.Grower])


from ..lib.util.types_protocols import Table, AnyKey

import asyncio




async def main() -> None:
  print("Start main.")
  # TODO: argparse
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host="localhost", dbname="postgres", options=options)

  # TODO: Should enforce this in library API
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  owner = demeter_types.Owner(owner="ABI")
  owner_id = demeter_api.insertOrGetOwner(cursor, owner)

  migrate_args = MigrateArgs(owner_id = owner_id, cursor = cursor)

  # TODO: Support indexes?
  plan = ImportPlan(
    # TODO: Joins?
    get_type_iterator = getTypeIterator,
    args = migrate_args,
    steps = [
      # TODO: Do we want growers without fields?
      #makeGrowers,
      # TODO: Do we want fields without any data?
      makeGeomAndField,
      #makeIrrigation,
      #makeReviewHarvest,
      #makeReviewQuality,
    ]
  )
  print("Plan setup.")

  primary_type_to_iterator = getTypeToIterators()

  find_fn = FindFn(getTypeIterator)
  for fn in plan.steps:
    print("START STEP: ",fn)
    type_to_results : Dict[Type, Dict[Table, AnyKey]] = {}
    type_to_task_to_deferred : TypeToTaskToDeferred = OrderedDict()

    # TODO: User shouldn't get this iterator
    next_fn = NextFn(primary_type_to_iterator)

    count = 0
    #while True:
    for i in range(0, 10):
      fn = cast(FiveArgs, fn)
      write_fn = WriteFn()
      get_fn = GetFn(type_to_results, type_to_task_to_deferred, write_fn)

      try:
        count += 1
        print("COUNT IS: ",count)
        fn(next_fn, find_fn, write_fn, get_fn, migrate_args)
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

    # Can at least start writing outputs
    for typ, outputs in write_fn.type_to_outputs.items():
      try:
        type_to_results[typ]
      except KeyError:
        type_to_results[typ] = {}

      insert_fn = TABLE_TO_INSERT_FN[typ]
      # TODO: Mass inserts
      for output_group in outputs:
        for o in output_group:
          result = insert_fn(cursor, o)
          type_to_results[typ][o] = result
          print("REGULAR RESULT:\n",o,"\n",result)

    def getDeferred(type_to_deferred : TypeToDeferred) -> TypeToDeferred:
      out : TypeToDeferred = {}
      for typ, deferred_groups in type_to_deferred.items():
        try:
          type_to_results[typ]
        except KeyError:
          type_to_results[typ] = {}

        try:
          type_to_task_to_deferred[typ]
        except KeyError:
          type_to_task_to_deferred[typ] = OrderedDict()

        insert_fn = TABLE_TO_INSERT_FN[typ]
        still_deferred : List[Task] = []
        for deferred_group in deferred_groups:
          for task in deferred_group:
            #print("CHECKING TASK: ",task)
            if task.done():
              #print("  DONE.")
              try:
                o = task.result()
                result = insert_fn(cursor, o)
                print("DEFERRED RESULT: ",typ,"\n",result,"\n",task)
                type_to_results[typ][o] = result
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

    type_to_still_deferred : TypeToDeferred = write_fn.getLatest()
    while len(type_to_still_deferred):
      type_to_still_deferred = getDeferred(type_to_still_deferred)
      await asyncio.sleep(1)
      type_to_still_deferred = write_fn.getLatest(type_to_still_deferred)


  sys.exit(1)


  # TODO: COMMIT
  # TODO: COMMIT


if __name__ == '__main__':
  asyncio.run(main())

