from typing import Optional, Union, Type, Any, Sequence, Iterator, Mapping, Dict, Callable, TextIO, List
from typing import cast

import psycopg2

import sys

# TODO: json_stream

from ..lib.core import types as demeter_types
from ..lib.core import api as demeter_api
from ..lib.local import types as local_types
from ..lib.local import api as local_api

from ..lib.stdlib.imports import I, WrapImport

from ..lib.stdlib.next_fn import NextFn, TypeToIterator
from ..lib.stdlib.write_fn import WriteFn
from ..lib.stdlib.find_fn import FindFn
from ..lib.stdlib.import_plan import FourArgs, ImportPlan
# TODO: TO REMOVE
from ..lib.stdlib.future import UnsetFutureException, Deferred, Future
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




psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

from ..lib.util.api_protocols import ReturnId, ReturnKey

# TODO: Memoize these?
TABLE_TO_INSERT_FN : Dict[Type, Union[ReturnId, ReturnKey]] = {
  demeter_types.Grower : demeter_api.insertOrGetGrower,
  demeter_types.Field : demeter_api.insertOrGetField,
  demeter_types.Geom : demeter_api.insertOrGetGeom,
  local_types.LocalType : local_api.insertOrGetLocalType,
  local_types.UnitType : local_api.insertOrGetUnitType,
  local_types.LocalValue : local_api.insertOrGetLocalValue,
  local_types.LocalValue : local_api.insertOrGetLocalValue,
}


from ..lib.util.types_protocols import Table, AnyKey

if __name__ == '__main__':

  # TODO: argparse
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host="localhost", dbname="postgres", options=options)

  # TODO: Should enforce this in library API
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  owner = demeter_types.Owner(owner="ABI")
  owner_id = demeter_api.insertOrGetOwner(cursor, owner)

  migrate_args = MigrateArgs(owner_id = owner_id, cursor = cursor)

  plan = ImportPlan(
    # TODO: Joins?
    get_type_iterator = getTypeIterator,
    args = migrate_args,
    steps = [
      #makeGrowers,
      #makeGeomAndField,
      makeIrrigation,
      makeReviewHarvest,
      makeReviewQuality,
    ]
  )

  primary_type_to_iterator = getTypeToIterators()

  type_to_deferred : Dict[Type, List[Deferred]] = {k : [] for k in TABLE_TO_INSERT_FN}
  for fn in plan.steps:
    print("\n\n\nFN: ",fn)
    fn = cast(FourArgs, fn)
    next_fn = NextFn(primary_type_to_iterator)
    find_fn = FindFn(getTypeIterator)
    write_fn = WriteFn()

    #while True:
    for i in range(0, 1000):
      try:
        fn(next_fn, find_fn, write_fn, migrate_args)
      except StopIteration:
        sys.stderr.write("Stopping iteration\n")
      except NotNullViolationException as e:
        #sys.stderr.write(f"Not null violation: {e}\n")
        sys.stderr.write(f"Null\n")
      except BadGeometryException as e:
        #sys.stderr.write(f"Bad geo: {e}\n")
        sys.stderr.write(f"Geo\n")
      else:
        sys.stderr.write(f"Success\n")

    for typ, outputs in write_fn.type_to_outputs.items():
      print("\nOutput: ",typ)
      insert_fn = TABLE_TO_INSERT_FN[typ]
      outputs.iterables.append(outputs.lone)
      # TODO: Mass inserts
      for it in outputs.iterables:
        for x in it:
          result = insert_fn(cursor, x)
          if isinstance(x, Future):
            x.set(result)
          #print("     Result: ",result)

    for typ, deferred in write_fn.type_to_deferred.items():
      print("Deferred: ",typ)
      insert_fn = TABLE_TO_INSERT_FN[typ]
      deferred.iterables.append(deferred.lone)
      deferred_further = type_to_deferred[typ]
      for dit in deferred.iterables:
        for d in dit:
          try:
            output = d()
          except UnsetFutureException:
            deferred_further.append(d)

  sys.exit(1)


  # TODO: COMMIT
  # TODO: COMMIT


