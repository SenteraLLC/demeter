from typing import Optional, Type, Sequence, Iterator, TypedDict, Any, Tuple, Dict

import json

from ..lib.stdlib.imports import I
from ..lib.stdlib.exceptions import InvalidRowException, BadGeometryException, NotNullViolationException
from ..lib.stdlib.next_fn import NextFn
from ..lib.stdlib.find_fn import FindFn
from ..lib.stdlib.write_fn import WriteFn
from ..lib.stdlib.future import Future, FutureRequired, Deferred

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
  if location_shape is None:
    if ((latitude := grower_field.Latitude) and (longitude := grower_field.Longitude)):
      shape = "Point"
      location_shape = f'[ {{ "Latitude": {latitude}, "Longitude": {longitude} }} ]'
    else:
      print("BAD: ",grower_field)
      return None


  parsed_location = json.loads(location_shape)
  postgis_format  = [(p["Latitude"], p["Longitude"]) for p in parsed_location]

  g = demeter_types.Geometry(
        type = shape,
        coordinates = [postgis_format],
        crs = demeter_types.CRS(
                type = "name",
                properties = {"name": "urn:ogc:def:crs:EPSG::4326"}
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
        details     = {
          "country_name" : maybe_country.Name if maybe_country else None,
          "region_name"  : maybe_region.Name if maybe_region else None,
        },
        last_updated = NOW,
      )
  return g


def makeGrowers(next_fn : NextFn,
                find_fn : FindFn,
                write_fn : WriteFn,
                args    : MigrateArgs,
               ):
  input_grower = next_fn(Grower)
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


def getUnitType(measure_unit : MeasureUnit, local_type_id : int) -> local_types.UnitType:
  unit = getMaxUnit(measure_unit)
  u = local_types.UnitType(
        unit = unit,
        local_type_id = local_type_id,
      )
  return u


def getFieldId(cursor : Any, grower_field : GrowerField, owner_id : int) -> int:
  raise Exception("NO FIELD ID, FIX")
#  external_id = grower_field.GrowerFieldId
#
#  stmt = """select field_id from field where owner_id = %(owner_id)s and external_id = %(external_id)s"""
#  args = {"owner_id": owner_id, "external_id": str(external_id)}
#  cursor.execute(stmt, args)
#  field_id = cursor.fetchone()["field_id"]
#
#  return field_id
#

def getField(next_fn  : NextFn,
             owner_id : int,
             geom_id  : int,
             grower_id : int,
            ) -> demeter_types.Field:
  grower_field = next_fn(GrowerField)
  external_id : int = grower_field.GrowerFieldId

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


A = TypeVar('A')
B = TypeVar('B')


GGF = Tuple[Future[demeter_types.Geom, int],
            Future[demeter_types.Grower, int],
            Deferred[demeter_types.Field],
           ]
def getGeomGrowerField(find_fn : FindFn,
                       next_fn : NextFn,
                       owner_id : int,
                       grower_field : GrowerField,
                      ) -> GGF:
  g = getGeom(grower_field)
  if g is None:
    raise BadGeometryException

  future_geom = FutureRequired[demeter_types.Geom, int](g)

  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower = FutureRequired[demeter_types.Grower, int](grower)

  df = Deferred(
        lambda : getField(next_fn,
                          owner_id,
                          geom_id   = future_geom.get(),
                          grower_id = future_grower.get()
                         )
        )
  return (future_geom, future_grower, df)


def makeGeomAndField(next_fn  : NextFn,
                     find_fn  : FindFn,
                     write_fn : WriteFn,
                     args     : MigrateArgs,
                    ):
  grower_field = next_fn(GrowerField)

  ggf = getGeomGrowerField(find_fn, next_fn, args["owner_id"], grower_field)
  future_geom, future_grower, df = ggf

  geom = future_geom.key
  grower = future_grower.key

  write_fn(demeter_types.Geom, future_geom)
  write_fn(demeter_types.Field, df)
  write_fn(demeter_types.Grower, grower)


def makeIrrigation(next_fn  : NextFn,
                   find_fn  : FindFn,
                   write_fn : WriteFn,
                   args     : MigrateArgs,
                  ):
  irrigation_applied = next_fn(IrrigationApplied)
  i = irrigation_applied

  irrigation_local_type = local_types.LocalType(
    type_name = "irrigation",
    type_category = "abi test data",
  )
  mk = "MeasureUnitId"
  measure_match = lambda m : m[mk] == i.AppliedOffMeasureUnitId
  maybe_measure_unit = find_fn(MeasureUnit, measure_match)
  measure_unit = NotNull(maybe_measure_unit)

  future_local_type_id = FutureRequired[local_types.LocalType, int](irrigation_local_type)

  unit_type = Deferred(
    lambda : getUnitType(measure_unit, future_local_type_id.get())
  )

  k = "IrrigationAppliedId"
  review_match = lambda r : r[k] == i.IrrigationAppliedId
  maybe_review = find_fn(Review, review_match)
  review = NotNull(maybe_review)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, next_fn, owner_id, grower_field)
  future_geom_id, future_grower_id, df = ggf

  future_geom_id = FutureRequired[demeter_types.Geom, int](future_geom_id.key)

  future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
  future_field_id = FutureRequired[Deferred[demeter_types.Field], int](df)

  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower_id = FutureRequired[demeter_types.Grower, int](grower)

  def makeLocalValue() -> local_types.LocalValue:
    geom_id = NotNull(future_geom_id.get())
    return local_types.LocalValue(
             geom_id        = geom_id,
             field_id       = future_field_id.get(),
             unit_type_id   = future_unit_type_id.get(),
             quantity       = i.TotalNumberIrrigation,
             local_group_id = None,
             acquired       = NOW,
             details        = {},
             last_updated   = NOW,
           )
  l = Deferred(makeLocalValue)

  write_fn(demeter_types.Geom,     future_geom_id)
  write_fn(demeter_types.Grower,   future_grower_id)
  write_fn(demeter_types.Field,    future_field_id)
  write_fn(local_types.UnitType,   unit_type)
  write_fn(local_types.LocalType,  irrigation_local_type)
  write_fn(local_types.LocalValue, l)


def makeReviewHarvest(next_fn  : NextFn,
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      args     : MigrateArgs,
                     ):
  owner_id = args["owner_id"]
  cursor = args["cursor"]

  review_harvest = next_fn(ReviewHarvest)

  k = "ReviewHarvestId"
  review_match = lambda r : r[k] == review_harvest.ReviewHarvestId
  maybe_review = find_fn(Review, review_match)
  review = NotNull(maybe_review)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, next_fn, owner_id, grower_field)
  future_geom_id, future_grower_id, df = ggf

  future_geom_id = FutureRequired[demeter_types.Geom, int](future_geom_id.key)

  future_field_id = FutureRequired[Deferred[demeter_types.Field], int](df)

  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower_id = FutureRequired[demeter_types.Grower, int](grower)

  def makeLocalValue(unit_type_id : int, quantity : float) -> local_types.LocalValue:
    return local_types.LocalValue(
             geom_id        = NotNull(future_geom_id.get()),
             field_id       = future_field_id.get(),
             unit_type_id   = unit_type_id,
             quantity       = quantity,
             local_group_id = None,
             acquired       = NOW,
             details        = {},
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
    maybe_measure_unit = find_fn(MeasureUnit, measure_match)
    measure_unit = NotNull(maybe_measure_unit)

    future_local_type_id = FutureRequired[local_types.LocalType, int](malt_local_type)

    unit_type = Deferred(
      lambda : getUnitType(measure_unit, future_local_type_id.get())
    )

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    l = Deferred(lambda : makeLocalValue(future_unit_type_id(), malt_yield))
    write_fn(local_types.LocalValue, l)


  agronomic_unit_id = review_harvest.AgronomicYieldMeasureUnitId
  agronomic_yield = review_harvest.AgronomicYield
  if agronomic_unit_id is not None and agronomic_yield is not None:
    agronomic_local_type = local_types.LocalType(
      type_name = "agronomic yield",
      type_category = "abi test data",
    )

    measure_match = lambda m : m[mk] == agronomic_unit_id
    maybe_measure_unit = find_fn(MeasureUnit, measure_match)
    measure_unit = NotNull(maybe_measure_unit)

    future_local_type_id = FutureRequired[local_types.LocalType, int](agronomic_local_type)

    unit_type = Deferred(
      lambda : getUnitType(measure_unit, future_local_type_id.get())
    )

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    l = Deferred(lambda : makeLocalValue(future_unit_type_id(), malt_yield))
    write_fn(local_types.LocalValue, l)

  write_fn(demeter_types.Geom,     future_geom_id)
  write_fn(demeter_types.Grower,   grower)



def makeReviewQuality(next_fn  : NextFn,
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      args     : MigrateArgs,
                     ):
  review = next_fn(Review)

  k = "ReviewQualityId"
  review_quality_match = lambda r : r[k] == review.ReviewQualityPreId
  maybe_review_quality = find_fn(ReviewQuality, review_quality_match)
  review_quality = NotNull(maybe_review_quality)

  k = "GrowerFieldId"
  grower_field_match = lambda gf : gf[k] == review.GrowerFieldId
  maybe_grower_field = find_fn(GrowerField, grower_field_match)
  grower_field = NotNull(maybe_grower_field)

  owner_id = args["owner_id"]
  ggf = getGeomGrowerField(find_fn, next_fn, owner_id, grower_field)
  future_geom_id, future_grower_id, df = ggf

  future_geom_id = FutureRequired[demeter_types.Geom, int](future_geom_id.key)

  future_field_id = FutureRequired[Deferred[demeter_types.Field], int](df)

  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower_id = FutureRequired[demeter_types.Grower, int](grower)

  def makeLocalValue(unit_type_id : int, quantity : float, details : Dict[str, Any] = {}) -> local_types.LocalValue:
    return local_types.LocalValue(
             geom_id        = NotNull(future_geom_id.get()),
             field_id       = future_field_id.get(),
             unit_type_id   = unit_type_id,
             quantity       = quantity,
             local_group_id = None,
             acquired       = NOW,
             details        = details,
             last_updated   = NOW,
           )


  def findUnitType(measure_unit_id : int,
                   local_type : local_types.LocalType,
                  ) -> Deferred[local_types.UnitType]:
    mk = "MeasureUnitId"
    measure_match = lambda m : m[mk] == measure_unit_id
    maybe_measure_unit = find_fn(MeasureUnit, measure_match)
    measure_unit = NotNull(maybe_measure_unit)

    future_local_type_id = FutureRequired[local_types.LocalType, int](local_type)

    unit_type = Deferred(
      lambda : getUnitType(measure_unit, future_local_type_id.get())
    )

    return unit_type


  def findLocalValue(local_type : local_types.LocalType,
                     measure_unit_id : int,
                     quantity : float,
                    ) -> Tuple[Deferred[local_types.UnitType],
                               Deferred[local_types.LocalValue]
                              ]:
    unit_type = findUnitType(measure_unit_id, local_type)

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    local_value = Deferred(lambda : makeLocalValue(future_unit_type_id(), quantity, details))
    return unit_type, local_value


  details = {
    "is_external_data" : review_quality.IsExternalData
  }

  Value = Tuple[local_types.LocalType, Optional[int], Optional[float]]
  values : Sequence[Value] = [
    (local_types.LocalType(
       type_name = "test weight",
       type_category = "abi test data",
     ),
     review_quality.TestWeightMeasureUnitId,
     review_quality.TestWeight,
    ),
    (local_types.LocalType(
       type_name = "sprout",
       type_category = "abi test data",
     ),
     review_quality.SproutSecondsMeasureUnitId,
     review_quality.Sprout,
    ),
    (local_types.LocalType(
       type_name = "don",
       type_category = "abi test data",
     ),
     review_quality.DONMeasureUnitId,
     review_quality.DON,
    ),
  ]

  for (local_type, maybe_measure_unit_id, maybe_quantity) in values:
    local_type
    measure_unit_id = NotNull(maybe_measure_unit_id)
    quantity = NotNull(maybe_quantity)

    u, l = findLocalValue(local_type, measure_unit_id, quantity)
    write_fn(local_types.UnitType, u)
    write_fn(local_types.LocalValue, l)


  def getLocalValue(local_type : local_types.LocalType,
                    unit : str,
                    quantity : float,
                   ) -> Tuple[Deferred[local_types.UnitType],
                              Deferred[local_types.LocalValue],
                             ]:
    future_local_type_id = FutureRequired[local_types.LocalType, int](local_type)

    unit_type = Deferred(
          lambda : local_types.UnitType(
                      unit = unit,
                      local_type_id = future_local_type_id.get(),
                    )
        )

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    l = Deferred(lambda : makeLocalValue(future_unit_type_id(), quantity, details))
    return u, l

  Value2 = Tuple[local_types.LocalType, Optional[str], Optional[float]]
  values2 : Sequence[Value2] = [
    (local_types.LocalType(
       type_name = "thins",
       type_category = "abi test data",
     ),
     review_quality.ThinsSieve,
     review_quality.Thins,
    ),

    (local_types.LocalType(
       type_name = "plump",
       type_category = "abi test data",
     ),
     review_quality.PlumpSieve,
     review_quality.Plump,
    ),

    (local_types.LocalType(
       type_name = "protein",
       type_category = "abi test data",
     ),
     "Unknown",
     review_quality.Protein,
    ),
  ]

  for (local_type, maybe_unit, maybe_quantity) in values2:
    try:
      unit = NotNull(maybe_unit)
      quatity = NotNull(maybe_quantity)
      u, l = getLocalValue(local_type, unit, quantity)
      write_fn(local_types.UnitType, u)
      write_fn(local_types.LocalValue, l)
    except NotNullViolationException:
      pass

  write_fn(demeter_types.Geom,   future_geom_id)
  write_fn(demeter_types.Grower, grower)
  write_fn(demeter_types.Field, future_field_id)

