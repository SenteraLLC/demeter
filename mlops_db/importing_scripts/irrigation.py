from typing import Iterator

import time

from ..lib.core import types as demeter_types
from ..lib.local import types as local_types
from ..lib.constants import NOW

from ..importing.find_fn import FindFn
from ..importing.write_fn import WriteFn
from ..importing.get_fn import GetFn

from .imports import *

from .util import MigrateArgs
from .util import NotNull, getGeomGrowerField, findGrower, deferMeasureUnitType

def makeIrrigation(irrigation_it : Iterator[IrrigationApplied],
                   find_fn  : FindFn,
                   write_fn : WriteFn,
                   get_fn   : GetFn,
                   args     : MigrateArgs,
                  ):
  last = time.time()

  irrigation_applied = next(irrigation_it)
  i = irrigation_applied

  irrigation_local_type = local_types.LocalType(
    type_name = "irrigation",
    type_category = "abi test data",
  )
  mk = "MeasureUnitId"
  measure_match = lambda m : m[mk] == i.AppliedOffMeasureUnitId
  maybe_measure_unit = find_fn(MeasureUnit, measure_match)
  measure_unit = NotNull(maybe_measure_unit)

  k = "IrrigationAppliedId"
  review_match = lambda r : r[k] == i.IrrigationAppliedId
  # TODO: 2nd Costly
  maybe_review = find_fn(Review, review_match)
  review = NotNull(maybe_review)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  # TODO: 3rd Costly
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, get_fn, owner_id, grower_field)
  geom, grower, deferred_field = ggf

  # TODO: 1st Costly
  grower = findGrower(find_fn, grower_field, owner_id)

  deferred_unit_type = deferMeasureUnitType(get_fn, measure_unit, irrigation_local_type)

  async def makeLocalValue() -> local_types.LocalValue:
    geom_id : int = NotNull(await get_fn(demeter_types.Geom, geom))
    # TODO: Warning function for missing field id?
    field_id : Optional[int] = await get_fn(demeter_types.Field, deferred_field)
    unit_id : int = NotNull(await get_fn(local_types.UnitType, deferred_unit_type))
    return local_types.LocalValue(
             geom_id        = geom_id,
             field_id       = field_id,
             unit_type_id   = unit_id,
             quantity       = i.TotalNumberIrrigation,
             local_group_id = None,
             acquired       = None,
             details        = None,
             last_updated   = NOW,
           )
  write_fn(demeter_types.Geom,     geom)
  write_fn(demeter_types.Grower,   grower)
  write_fn(demeter_types.Field,    deferred_field)
  write_fn(local_types.UnitType,   deferred_unit_type)
  write_fn(local_types.LocalType,  irrigation_local_type)
  write_fn(local_types.LocalValue, makeLocalValue())


