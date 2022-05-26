from typing import Type, Sequence, Iterator, Mapping, Dict

import psycopg2
import asyncio
import json

# TODO: json_stream

from ..lib.core import types as demeter_types
from ..lib.core import api as demeter_api
from ..lib.local import types as local_types
from ..lib.local import api as local_api
from ..lib.util.api_protocols import ReturnId, ReturnKey

from ..importing.execute import TypeToInsert
from ..importing.execute import executePlan
from ..importing.imports import I, WrapImport
from ..importing.import_plan import ImportPlan

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



def getSource(s : Type[I]) -> Sequence[I]:
  p = SOURCE_TO_PATH[s]
  f = open(p)
  j = json.load(f)
  return j


# TODO: jsonstream
def makeJsonIterator(source_type : Type[I]) -> Iterator[I]:
  return (WrapImport(row) for row in getSource(source_type))

def getTypeIterator(typ : Type[I]) -> Iterator[I]:
  it = makeJsonIterator(typ)
  return it


TYPE_TO_INSERT_FN : TypeToInsert = {
  demeter_types.Grower : demeter_api.insertOrGetGrower,
  demeter_types.Field : demeter_api.insertOrGetField,
  demeter_types.Geom : demeter_api.insertOrGetGeom,
  local_types.LocalType : local_api.insertOrGetLocalType,
  local_types.UnitType : local_api.insertOrGetUnitType,
  local_types.LocalValue : local_api.insertOrGetLocalValue,
}


async def main() -> None:
  # TODO: argparse or config
  options = "-c search_path=test_mlops,public"
  connection = psycopg2.connect(host="localhost", dbname="postgres", options=options)

  # TODO: Should enforce this in library API
  cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  owner = demeter_types.Owner(owner="ABI")
  owner_id = demeter_api.insertOrGetOwner(cursor, owner)

  migrate_args = MigrateArgs(owner_id = owner_id, cursor = cursor)

  # TODO: Support indexes and maybe joins
  plan = ImportPlan(
    get_type_iterator = getTypeIterator,
    args = migrate_args,
    steps = [
      # TODO: Do we want growers or fields without data?
      #(Grower, makeGrowers),
      #(GrowerField, makeGeomAndField),
      (IrrigationApplied, makeIrrigation),
      (ReviewHarvest, makeReviewHarvest),
      (Review, makeReviewQuality),
    ]
  )


  await executePlan(cursor, plan, TYPE_TO_INSERT_FN)

  connection.commit()



if __name__ == '__main__':
  asyncio.run(main())

