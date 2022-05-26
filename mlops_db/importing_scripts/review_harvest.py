from typing import Iterator

from ..lib.core import types as demeter_types
from ..lib.constants import NOW
from ..lib.local import types as local_types

from ..importing.find_fn import FindFn
from ..importing.write_fn import WriteFn
from ..importing.get_fn import GetFn

from .util import getGeomGrowerField, NotNull, deferMeasureUnitType
from .util import MigrateArgs

from .imports import *


def makeReviewHarvest(review_it : Iterator[ReviewHarvest],
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      get_fn   : GetFn,
                      args     : MigrateArgs,
                     ):
  owner_id = args["owner_id"]
  cursor = args["cursor"]

  review_harvest = next(review_it)

  k = "ReviewHarvestId"
  review_match = lambda r : r[k] == review_harvest.ReviewHarvestId
  maybe_review = find_fn(Review, review_match)
  review = NotNull(maybe_review)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, get_fn, owner_id, grower_field)
  geom, grower, deferred_field = ggf

  write_fn(demeter_types.Geom,     geom)
  write_fn(demeter_types.Grower,   grower)
  write_fn(demeter_types.Field, deferred_field)

  async def deferLocalValue(
              unit_type_id : int,
              quantity : float
             ) -> local_types.LocalValue:
    geom_id : int = NotNull(await get_fn(demeter_types.Geom, geom))
    field_id : int = NotNull(await get_fn(demeter_types.Field, deferred_field))
    return local_types.LocalValue(
             geom_id        = geom_id,
             field_id       = field_id,
             unit_type_id   = unit_type_id,
             quantity       = quantity,
             local_group_id = None,
             acquired       = None,
             details        = None,
             last_updated   = NOW,
           )
  mk = "MeasureUnitId"

  malt_unit_id = review_harvest.MaltBarleyYieldMeasureUnitId
  malt_yield = review_harvest.MaltBarleyYield
  if malt_unit_id is not None and malt_yield is not None:
    malt_local_type = local_types.LocalType(
      type_name = "malt barley yield",
      type_category = "abi test data",
    )

    measure_match = lambda m : m[mk] == malt_unit_id
    if (measure_unit := find_fn(MeasureUnit, measure_match)):
      deferred_unit_type = deferMeasureUnitType(get_fn, measure_unit, malt_local_type)

      async def deferMaltValue() -> local_types.LocalValue:
        unit_type : int = NotNull(await get_fn(local_types.UnitType, deferred_unit_type))
        return await deferLocalValue(unit_type, malt_yield)

      write_fn(local_types.LocalType, malt_local_type)
      write_fn(local_types.UnitType, deferred_unit_type)
      deferred_malt_value = deferMaltValue()
      write_fn(local_types.LocalValue, deferred_malt_value)


  agronomic_unit_id = review_harvest.AgronomicYieldMeasureUnitId
  agronomic_yield = review_harvest.AgronomicYield
  if agronomic_unit_id is not None and agronomic_yield is not None:
    agronomic_local_type = local_types.LocalType(
      type_name = "agronomic yield",
      type_category = "abi test data",
    )

    measure_match = lambda m : m[mk] == agronomic_unit_id
    if (measure_unit := find_fn(MeasureUnit, measure_match)):
      deferred_unit_type = deferMeasureUnitType(get_fn, measure_unit, agronomic_local_type)

      async def deferAgronomicValue() -> local_types.LocalValue:
        unit_type_id : int = NotNull(await get_fn(local_types.UnitType, deferred_unit_type))
        return await deferLocalValue(unit_type_id, agronomic_yield)

      write_fn(local_types.LocalType, agronomic_local_type)
      write_fn(local_types.UnitType, deferred_unit_type)
      deferred_agronomic_value = deferAgronomicValue()
      write_fn(local_types.LocalValue, deferred_agronomic_value)
      print("Write agro.")


