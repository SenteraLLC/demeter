from typing import Optional, Union
from typing import Literal
from typing import Tuple
from datetime import date, datetime

from ..database.types_protocols import Table, Updateable, Detailed
from ..database.details import HashablePair

from collections import OrderedDict
from dataclasses import InitVar
from dataclasses import dataclass, field


@dataclass(frozen=True)
class CRS:
  type       : Literal["name"]
  # TODO: What other properties are available?
  properties : HashablePair


Point = Tuple[float, float]
Line = Tuple[Point, ...]
# Postgis only wants MultiPolygon
_Polygon = Line
MultiPolygon = Tuple[_Polygon, ...]
Coordinates = Union[Point, Line, MultiPolygon]


@dataclass(frozen=True)
class GeomImpl(Table):
  type        : str
  coordinates : Coordinates
  crs_name    : InitVar[str]
  crs         : CRS = field(init=False)

  def __post_init__(self, crs_name : str):
    crs = CRS(type = "name",
              properties = HashablePair("name", crs_name),
             )
    object.__setattr__(self, 'crs', crs)


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
  external_id : Optional[str]
  # TODO: Internal id?
  sentera_id  : Optional[str]      = None
  created     : Optional[datetime] = None

@dataclass(frozen=True)
class GeoSpatialKey(Table):
  geom_id  : int
  field_id : Optional[int]

@dataclass(frozen=True)
class TemporalKey(Table):
  start_date : date
  end_date   : date


AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]

