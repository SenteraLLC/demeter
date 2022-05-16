from typing import Optional, Type, Sequence, Iterator, TypedDict, Any, Tuple

from .plan import I, InvalidRowException, FindFn, NextFn, WriteFn, BadGeometryException, FutureRequired, Deferred

from .imports import *

import json

from ..lib.core import types as demeter_types
from ..lib.local import types as local_types

from ..lib.constants import NOW


def NotNull(v : Optional[I]) -> I:
  if v is None:
    raise InvalidRowException()
  return v


class MigrateArgs(TypedDict):
  owner_id : int
  cursor   : Any


# Start Scripts

def getFarmName(grower : Grower) -> str:
  farm_name = grower.FarmName
  farm_city = grower.FarmCity or ""
  return "-testing-".join([farm_name, farm_city])


def getGeom(grower_field : GrowerField) -> Optional[demeter_types.Geometry]:
  location_shape : str = grower_field.LocationShape
  if location_shape is None:
    return None
  parsed_location = json.loads(location_shape)
  postgis_format  = [(p["Latitude"], p["Longitude"]) for p in parsed_location]

  geom = demeter_types.Geometry(
           type = "Polygon",
           coordinates = [postgis_format],
           crs = demeter_types.CRS(
                   type = "name",
                   properties = {"name": "urn:ogc:def:crs:EPSG::4326"}
                 )
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

# TODO: Move to constants file
TYPE_NAME = "irrigation"
TYPE_CATEGORY = "abi irrigation test data"
LOCAL_TYPE = local_types.LocalType(
  type_name = TYPE_NAME,
  type_category = TYPE_CATEGORY,
)


def getUnitType(measure_unit : MeasureUnit, local_type_id : int) -> local_types.UnitType:
  m = measure_unit
  unit_candidates : Sequence[str] = [m.Description, m.Identifier, m.Name]
  unit_counts = Counter(unit_candidates)

  unit = max(unit_counts, key=unit_counts.get) # type: ignore
  u = local_types.UnitType(
        unit = unit,
        local_type_id = local_type_id,
      )
  return u


def getFieldId(cursor : Any, grower_field : GrowerField, owner_id : int) -> int:
  external_id = grower_field.GrowerFieldId

  stmt = """select field_id from field where owner_id = %(owner_id)s and external_id = %(external_id)s"""
  args = [owner_id, external_id]
  results = cursor.execute(stmt, args)
  field_id = results.fetchone()["field_id"]

  return field_id


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

Future = Tuple[FutureRequired[I, B], I]

GGF = Tuple[Future[demeter_types.Geometry, int],
            Future[demeter_types.Grower, int],
            Deferred[demeter_types.Field],
           ]
def getGeomGrowerField(find_fn : FindFn,
                      next_fn : NextFn,
                      owner_id : int,
                      grower_field : GrowerField,
                     ) -> GGF:
  geom = getGeom(grower_field)
  if geom is None:
    raise BadGeometryException

  future_geom_id = FutureRequired[demeter_types.Geometry, int](geom)

  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower_id = FutureRequired[demeter_types.Grower, int](grower)

  df = Deferred(
        lambda : getField(next_fn,
                          owner_id,
                          geom_id   = future_geom_id.get(),
                          grower_id = future_grower_id.get()
                         )
        )
  return (future_geom_id, geom), (future_grower_id, grower), df


def makeGeomAndField(next_fn : NextFn,
                     find_fn : FindFn,
                     write_fn : WriteFn,
                     args    : MigrateArgs,
                    ):
  grower_field = next_fn(GrowerField)

  ggf = getGeomGrowerField(find_fn, next_fn, args["owner_id"], grower_field)
  (_, geom), (_, grower), df = ggf

  write_fn(demeter_types.Field, df)
  write_fn(demeter_types.Geom, geom)
  write_fn(demeter_types.Grower, grower)



def makeIrrigation(next_fn  : NextFn,
                   find_fn  : FindFn,
                   write_fn : WriteFn,
                   args     : MigrateArgs,
                  ):
  irrigation_applied = next_fn(IrrigationApplied)
  i = irrigation_applied

  mk = "MeasureUnitId"
  measure_match = lambda m : m[mk] == i.AppliedOffMeasureUnitId
  maybe_measure_unit = find_fn(MeasureUnit, measure_match)

  measure_unit = NotNull(maybe_measure_unit)

  future_local_type_id = FutureRequired[local_types.LocalType, int](LOCAL_TYPE)

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

  geom = getGeom(grower_field)
  if geom is None:
    raise BadGeometryException
  future_geom_id = FutureRequired[demeter_types.Geometry, int](geom)

  future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)

  owner_id = args["owner_id"]
  grower = findGrower(find_fn, grower_field, owner_id)
  future_grower_id = FutureRequired[demeter_types.Grower, int](grower)

  cursor = args["cursor"]
  field_id = getFieldId(cursor, grower_field, owner_id)

  def makeLocalValue() -> local_types.LocalValue:
    geom_id = future_geom_id.get()
    grower_id = future_grower_id.get()
    f = getField(next_fn, owner_id, geom_id, grower_id)
    return local_types.LocalValue(
             geom_id        = future_geom_id.get(),
             field_id       = field_id,
             unit_type_id   = future_unit_type_id.get(),
             quantity       = i.TotalNumberIrrigation,
             local_group_id = None,
             acquired       = NOW,
             details        = {},
             last_updated   = NOW,
           )
  l = Deferred(makeLocalValue)

  write_fn(demeter_types.Geom,     geom)
  write_fn(demeter_types.Grower,   grower)
  write_fn(local_types.UnitType,   unit_type)
  write_fn(local_types.LocalType,  LOCAL_TYPE)
  write_fn(local_types.LocalValue, l)


def makeReviewHarvest(next_fn  : NextFn,
                      find_fn  : FindFn,
                      write_fn : WriteFn,
                      args     : MigrateArgs,
                     ):
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
  (future_geom_id, geom), (future_grower_id, grower), df = ggf

  cursor = args["cursor"]
  field_id = getFieldId(cursor, grower_field, owner_id)


  def makeLocalValue(unit_type_id : int, quantity : float) -> local_types.LocalValue:
    geom_id = future_geom_id.get()
    grower_id = future_grower_id.get()
    f = getField(next_fn, owner_id, geom_id, grower_id)
    return local_types.LocalValue(
             geom_id        = future_geom_id.get(),
             field_id       = field_id,
             unit_type_id   = unit_type_id,
             quantity       = quantity,
             local_group_id = None,
             acquired       = NOW,
             details        = {},
             last_updated   = NOW,
           )


  from functools import partial

  mk = "MeasureUnitId"

  malt_unit_id = review_harvest.MaltBarleyYieldMeasureUnitId
  malt_yield = review_harvest.MaltBarleyYield
  if malt_unit_id is not None and malt_yield is not None:
    measure_match = lambda m : m[mk] == malt_unit_id
    maybe_measure_unit = find_fn(MeasureUnit, measure_match)
    measure_unit = NotNull(maybe_measure_unit)

    future_local_type_id = FutureRequired[local_types.LocalType, int](LOCAL_TYPE)

    unit_type = Deferred(
      lambda : getUnitType(measure_unit, future_local_type_id.get())
    )

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    l = Deferred(partial(makeLocalValue, future_unit_type_id, malt_yield))
    write_fn(local_types.LocalValue, l)


  agronomic_unit_id = review_harvest.AgronomicYieldMeasureUnitId
  agronomic_yield = review_harvest.AgronomicYield
  if agronomic_unit_id is not None and agronomic_yield is not None:
    measure_match = lambda m : m[mk] == agronomic_unit_id
    maybe_measure_unit = find_fn(MeasureUnit, measure_match)
    measure_unit = NotNull(maybe_measure_unit)

    future_local_type_id = FutureRequired[local_types.LocalType, int](LOCAL_TYPE)

    unit_type = Deferred(
      lambda : getUnitType(measure_unit, future_local_type_id.get())
    )

    future_unit_type_id = FutureRequired[Deferred[local_types.UnitType], int](unit_type)
    l = Deferred(partial(makeLocalValue, future_unit_type_id, malt_yield))
    write_fn(local_types.LocalValue, l)


