from typing import Iterator, Tuple, Sequence, Awaitable

import time

from ..lib.core import types as demeter_types
from ..lib.local import types as local_types
from ..lib.constants import NOW
from ..lib.util.details import Details

from ..importing.find_fn import FindFn
from ..importing.write_fn import WriteFn
from ..importing.get_fn import GetFn

from .imports import *

from .util import MigrateArgs
from .util import NotNull, getGeomGrowerField, findGrower, deferMeasureUnitType, deferUnitType


def makeReviewQuality(review_it  : Iterator[Review],
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      get_fn   : GetFn,
                      args     : MigrateArgs,
                     ):
  review = next(review_it)

  k = "ReviewQualityId"
  review_quality_match = lambda r : r[k] == review.ReviewQualityPostId
  maybe_review_quality = find_fn(ReviewQuality, review_quality_match)
  review_quality = NotNull(maybe_review_quality)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, get_fn, owner_id, grower_field)
  geom, grower, deferred_field = ggf

  async def deferLocalValueBase(unit_type_id : int,
                                quantity : float,
                                details : Optional[Details] = None,
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
             details        = details,
             last_updated   = NOW,
           )

  details = {
    "is_external_data" : review_quality.IsExternalData
  }

  Value = Tuple[local_types.LocalType,
                Optional[float],
                Optional[int],
                Optional[str]
               ]
  values : Sequence[Value] = [
    (local_types.LocalType(
       type_name = "test weight",
       type_category = "abi test data",
     ),
     review_quality.TestWeight,
     review_quality.TestWeightMeasureUnitId,
     None,
    ),
    (local_types.LocalType(
       type_name = "sprout",
       type_category = "abi test data",
     ),
     review_quality.Sprout,
     review_quality.SproutSecondsMeasureUnitId,
     None,
    ),
    (local_types.LocalType(
       type_name = "don",
       type_category = "abi test data",
     ),
     review_quality.DON,
     review_quality.DONMeasureUnitId,
     None,
    ),

    (local_types.LocalType(
       type_name = "thins",
       type_category = "abi test data",
     ),
     review_quality.Thins,
     None,
     review_quality.ThinsSieve,
    ),

    (local_types.LocalType(
       type_name = "plump",
       type_category = "abi test data",
     ),
     review_quality.Plump,
     None,
     review_quality.PlumpSieve,
    ),

    (local_types.LocalType(
       type_name = "protein",
       type_category = "abi test data",
     ),
     review_quality.Protein,
     None,
     "Unknown",
    ),

  ]

  for (local_type, maybe_quantity, maybe_measure_unit_id, maybe_unit) in values:
    if (quantity := maybe_quantity) is not None:

      maybe_u : Optional[Awaitable[local_types.UnitType]] = None
      if ( measure_unit_id := maybe_measure_unit_id ) is not None:
        measure_match = lambda m : m["MeasureUnitId"] == measure_unit_id
        maybe_measure_unit = find_fn(MeasureUnit, measure_match)
        if ( measure_unit := maybe_measure_unit) is not None:
          maybe_u = deferMeasureUnitType(get_fn, measure_unit, local_type)
        else:
          continue

      if (unit := maybe_unit) is not None:
        maybe_u = deferUnitType(get_fn, unit, local_type)

      if maybe_u is None:
        # TODO: Might want to add a common "UNKNOWN" type to demeter
        continue

      u = maybe_u
      async def deferLocalValue(quantity : float,
                                unit_type : Awaitable[local_types.UnitType],
                               ) -> local_types.LocalValue:
        unit_type_id : int = NotNull(await get_fn(local_types.UnitType, unit_type))
        b = await deferLocalValueBase(unit_type_id, quantity)
        return b

      write_fn(local_types.LocalType, local_type)
      write_fn(local_types.UnitType, u)
      l = deferLocalValue(quantity, u)
      write_fn(local_types.LocalValue, l)

  write_fn(demeter_types.Geom,   geom)
  write_fn(demeter_types.Grower, grower)
  write_fn(demeter_types.Field,  deferred_field)

