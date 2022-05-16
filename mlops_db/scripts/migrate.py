from typing import Optional, Union, Type, Any, Sequence, Iterator, Mapping, Dict, Callable, TextIO, List
from typing import cast

from ..lib.constants import NOW

import psycopg2

import sys

# TODO: json_stream
import json
from functools import lru_cache as memo

from .imports import *
from .plan import MatchFn, InvalidRowException, BadGeometryException, NextFn, FindFn, Plan, WriteFn, FourArgs, Outputs
from .plan import FutureRequired, Deferred, TypeToIterator, UnsetFutureException, TypeToDeferred

from ..lib.core import types as demeter_types
from ..lib.core import api as demeter_api
from ..lib.local import types as local_types
from ..lib.local import api as local_api


from .migrate_help import makeGrowers, makeGeomAndField, makeIrrigation
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
  GrowerIt = makeJsonIterator(Grower)

  GrowerFieldIt = makeJsonIterator(GrowerField)
  ReviewIt = makeJsonIterator(Review)


  type_to_iter : TypeToIterator = {
    Grower      : GrowerIt,
    GrowerField : GrowerFieldIt,
    Review      : ReviewIt,
    Country     : makeJsonIterator(Country),
    Region      : makeJsonIterator(Region),
    MeasureUnit : makeJsonIterator(MeasureUnit)

  }
  return type_to_iter

def getTypeIterator(typ : Type[Import]) -> Iterator[Import]:
  type_to_iterator = getTypeToIterators()
  it = type_to_iterator[typ]
  return it




psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

from ..lib.util.api_protocols import ReturnId, ReturnKey

# TODO: Memoize these?
TABLE_TO_INSERT_FN : Dict[Type, Union[ReturnId, ReturnKey]] = {
  demeter_types.Grower : demeter_api.insertOrGetGrower,
  #demeter_types.Field : demeter_api.insertOrGetField,
  demeter_types.Geom : demeter_api.insertOrGetGeom,
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

  plan = Plan(
    # TODO: Joins?
    get_type_iterator = getTypeIterator,
    args = migrate_args,
    steps = [
      makeGrowers,
      makeGeomAndField,
      makeIrrigation,
    ]
  )

  primary_type_to_iterator = getTypeToIterators()

  import sys

  type_to_deferred : Dict[Type, List[Deferred]] = {k : [] for k in TABLE_TO_INSERT_FN}
  for fn in plan.steps:
    fn = cast(FourArgs, fn)
    next_fn = NextFn(primary_type_to_iterator)
    find_fn = FindFn(getTypeIterator)
    write_fn = WriteFn()

    try:
      fn(next_fn, find_fn, write_fn, migrate_args)
    except StopIteration:
      sys.stderr.write("Stopping iteration")
      pass

    for typ, outputs in write_fn.type_to_outputs.items():
      insert_fn = TABLE_TO_INSERT_FN[typ]
      outputs.iterables.append(outputs.lone)
      # TODO: Mass insert
      for it in outputs.iterables:
        for x in it:
          result = insert_fn(cursor, x)
          sys.exit(1)
        sys.exit(1)
      sys.exit(1)

    for typ, deferred in write_fn.type_to_deferred.items():
      insert_fn = TABLE_TO_INSERT_FN[typ]
      deferred.iterables.append(deferred.lone)
      deferred_further = type_to_deferred[typ]
      for dit in deferred.iterables:
        for d in dit:
          try:
            output = d()
          except UnsetFutureException:
            deferred_further.append(d)



  # TODO: COMMIT
  # TODO: COMMIT


