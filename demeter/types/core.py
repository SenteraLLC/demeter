from typing import Optional, Union
from typing import Mapping
from typing import Literal
from typing import Sequence, Tuple
from datetime import date

from ..database.types_protocols import Table, Updateable, Detailed
from ..database.details import JsonRootObject, HashableJsonContainer

from dataclasses import InitVar
from dataclasses import dataclass

#@dataclass(frozen=True)
#class Properties(Details):
#  def __init__(self, name : str):
#    super().__init__({"name": name})
#
#  # TODO: Inheritance/hashing/dataclass is messy
#  #def __hash__(self) -> int:
#  #  return super().__hash__()

Properties = JsonRootObject

@dataclass(frozen=True)
class CRS(Table, HashableJsonContainer):
  type       : Literal["name"]
  properties : InitVar[Properties]


Point = Tuple[float, float]
Line = Tuple[Point, ...]
# Postgis only wants MultiPolygon, not Polygon
Polygon = Line
MultiPolygon = Tuple[Polygon, ...]
Coordinates = Union[Point, Line, MultiPolygon]


@dataclass(frozen=True)
class GeomImpl(Table):
  type        : str
  coordinates : Coordinates
  crs         : CRS

@dataclass(frozen=True)
class Geom(Updateable):
  container_geom_id : Optional[int]
  geom              : GeomImpl

@dataclass(frozen=True)
class InsertableGeom(Table):
  container_geom_id : Optional[int]
  geom              : str

@dataclass(frozen=True)
class Owner(Table):
  owner : str

@dataclass(frozen=True)
class Grower(Detailed):
  owner_id    : int
  farm        : str
  external_id : Optional[str]

@dataclass(frozen=True)
class Field(Table):
  owner_id    : int
  geom_id     : int
  year        : Optional[int]
  grower_id   : Optional[int]
  sentera_id  : Optional[str]
  external_id : Optional[str]

@dataclass(frozen=True)
class GeoSpatialKey(Table):
  geom_id  : int
  field_id : Optional[int]

@dataclass(frozen=True)
class TemporalKey(Table):
  start_date : date
  end_date   : date


AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]

