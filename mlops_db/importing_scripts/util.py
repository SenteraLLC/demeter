from typing import TypeVar, Optional, Tuple, Sequence, Dict, TypedDict

import json

from ..lib.core import types as demeter_types
from ..lib.local import types as local_types
from ..lib.constants import NOW
from ..lib.util.details import Details

from ..importing.exceptions import NotNullViolationException, BadGeometryException
from ..importing.find_fn import FindFn
from ..importing.get_fn import GetFn

from .imports import *

# TODO: Temporary
class MigrateArgs(TypedDict):
  owner_id : int
  cursor   : Any


X = TypeVar('X')

def NotNull(v : Optional[X]) -> X:
  if v is None:
    raise NotNullViolationException()
  return v


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

    external_id : int = grower_field.GrowerFieldId

    return getField(owner_id,
                    geom_id   = geom_id,
                    grower_id = grower_id,
                    external_id = str(external_id),
                   )

  return (geom, grower, deferField())


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


