from typing import Optional, Type, Sequence, Iterator, TypedDict, Any, Tuple, Dict

import json

from ..lib.stdlib.imports import I
from ..lib.stdlib.exceptions import InvalidRowException, BadGeometryException, NotNullViolationException
from ..lib.stdlib.next_fn import NextFn
from ..lib.stdlib.find_fn import FindFn
from ..lib.stdlib.write_fn import WriteFn
from ..lib.stdlib.get_fn import GetFn
from ..lib.util.details import Details
from ..lib.util.types_protocols import TableEncoder

from .imports import *

from ..lib.core import types as demeter_types
from ..lib.local import types as local_types


from ..lib.constants import NOW

X = TypeVar('X')

def NotNull(v : Optional[X]) -> X:
  if v is None:
    raise NotNullViolationException()
  return v


class MigrateArgs(TypedDict):
  owner_id : int
  cursor   : Any


def getFarmName(grower : Grower) -> str:
  farm_name = grower.FarmName
  farm_city = grower.FarmCity or ""
  return "-testing-".join([farm_name, farm_city])


def getGeom(grower_field : GrowerField) -> Optional[demeter_types.Geom]:
  location_shape : str = grower_field.LocationShape
  shape = "Polygon"
  out : Optional[demeter_types.Coordinates] = None
  if location_shape is None:
    if ((latitude := grower_field.Latitude) and (longitude := grower_field.Longitude)):
      shape = "Point"
      p = demeter_types.Point = (latitude, longitude)
      out = p
    else:
      return None
  else:
    parsed_location = json.loads(location_shape)
    def getCoord(p : Dict[str, Any]) -> Tuple[float, float]:
      return (p["Latitude"], p["Longitude"])
    polygon : demeter_types.Polygon = tuple(getCoord(p) for p in parsed_location)
    multipolygon : demeter_types.MultiPolygon = (polygon, )
    out = multipolygon

  g = demeter_types.Geometry(
        type = shape,
        coordinates = out,
        crs = demeter_types.CRS(
                type = "name",
                properties = demeter_types.Properties({"name": "urn:ogc:def:crs:EPSG::4326"})
              )
      )
  geom = demeter_types.Geom(
           geom = g,
           container_geom_id = None,
           last_updated = NOW,
        )
  return geom


def makeGrower(input_grower : Grower,
               find_fn : FindFn,
               owner_id : int,
              ) -> demeter_types.Grower:
  country_match = lambda c : c.CountryId == input_grower.CountryId
  maybe_country = find_fn(Country, country_match)

  region_match = lambda c : c.RegionId == input_grower.RegionId
  maybe_region = find_fn(Region, region_match)

  g = demeter_types.Grower(
        owner_id    = owner_id,
        external_id = str(input_grower.GrowerId),
        farm        = getFarmName(input_grower),
        details     = Details({
          "country_name" : maybe_country.Name if maybe_country else None,
          "region_name"  : maybe_region.Name if maybe_region else None,
        }),
        last_updated = NOW,
      )
  return g


def makeGrowers(next_fn  : NextFn,
                find_fn  : FindFn,
                write_fn : WriteFn,
                get_fn   : GetFn,
                args     : MigrateArgs,
               ):
  input_grower = next_fn()
  owner_id = args["owner_id"]
  g = makeGrower(input_grower, find_fn, owner_id)
  gs : Sequence[demeter_types.Grower] = [] if g is None else [g]
  write_fn(demeter_types.Grower, gs)


from collections import Counter

def getMaxUnit(measure_unit : MeasureUnit) -> str:
  m = measure_unit
  unit_candidates : Sequence[str] = [m.Description, m.Identifier, m.Name]
  unit_counts = Counter(unit_candidates)

  unit = max(unit_counts, key=unit_counts.get) # type: ignore
  return unit



def getField(owner_id : int,
             geom_id  : int,
             grower_id : int,
             external_id : str,
            ) -> demeter_types.Field:
  f = demeter_types.Field(
        owner_id    = owner_id,
        geom_id     = geom_id,
        external_id = str(external_id),
        grower_id   = grower_id,
        sentera_id  = None,
        year        = None,
      )
  return f


def findGrower(find_fn : FindFn,
               grower_field : GrowerField,
               owner_id : int,
              ) -> demeter_types.Grower:
  match_grower = lambda g : g.GrowerId == grower_field.GrowerId
  maybe_input_grower = find_fn(Grower, match_grower)
  input_grower = NotNull(maybe_input_grower)
  grower = makeGrower(input_grower, find_fn, owner_id)
  return grower


def findField(find_fn : FindFn, grower_id : int) -> GrowerField:
  k = "GrowerId"
  grower_field_match = lambda gf : gf[k] == grower_id
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)
  return grower_field


A = TypeVar('A')
B = TypeVar('B')

from typing import Awaitable

GGF = Tuple[demeter_types.Geom,
            demeter_types.Grower,
            Awaitable[demeter_types.Field],
           ]
def getGeomGrowerField(find_fn : FindFn,
                       get_fn   : GetFn,
                       owner_id : int,
                       grower_field : GrowerField,
                      ) -> GGF:
  maybe_geom = getGeom(grower_field)
  if maybe_geom is None:
    raise BadGeometryException
  geom = maybe_geom

  grower = findGrower(find_fn, grower_field, owner_id)

  async def deferField() -> demeter_types.Field:
    geom_id : int = NotNull(await get_fn(demeter_types.Geom, geom))
    grower_id : int = NotNull(await get_fn(demeter_types.Grower, grower))

    #grower_field = findField(find_fn, grower_id)
    external_id : int = grower_field.GrowerFieldId

    return getField(owner_id,
                    geom_id   = geom_id,
                    grower_id = grower_id,
                    external_id = str(external_id),
                   )

  return (geom, grower, deferField())


def makeGeomAndField(next_fn  : NextFn,
                     find_fn  : FindFn,
                     write_fn : WriteFn,
                     get_fn   : GetFn,
                     args     : MigrateArgs,
                    ):
  grower_field = next_fn()

  ggf = getGeomGrowerField(find_fn, get_fn, args["owner_id"], grower_field)
  geom, grower, deferred_field = ggf

  write_fn(demeter_types.Geom, geom)
  write_fn(demeter_types.Field, deferred_field)
  write_fn(demeter_types.Grower, grower)


async def deferUnitType(get_fn     : GetFn,
                        unit       : str,
                        local_type : local_types.LocalType,
                       ) -> local_types.UnitType:
  local_type_id : int = NotNull(await get_fn(local_types.LocalType, local_type))
  u = local_types.UnitType(
        unit = unit,
        local_type_id = local_type_id,
      )
  return u


async def deferMeasureUnitType(get_fn     : GetFn,
                               measure_unit : MeasureUnit,
                               local_type : local_types.LocalType,
                              ) -> local_types.UnitType:
  unit = getMaxUnit(measure_unit)
  return await deferUnitType(get_fn, unit, local_type)

import time

def makeIrrigation(next_fn  : NextFn,
                   find_fn  : FindFn,
                   write_fn : WriteFn,
                   get_fn   : GetFn,
                   args     : MigrateArgs,
                  ):
  last = time.time()

  irrigation_applied = next_fn()
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


def makeReviewHarvest(next_fn  : NextFn,
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      get_fn   : GetFn,
                      args     : MigrateArgs,
                     ):
  owner_id = args["owner_id"]
  cursor = args["cursor"]

  review_harvest = next_fn()

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



def makeReviewQuality(next_fn  : NextFn,
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      get_fn   : GetFn,
                      args     : MigrateArgs,
                     ):
  review = next_fn()

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

