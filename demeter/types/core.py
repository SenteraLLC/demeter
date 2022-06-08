from typing import Optional, Union
from typing import Literal, Mapping, Tuple
from datetime import date, datetime

from ..database.types_protocols import Table, Updateable, Detailed
from ..database.details import HashableJSON

import json
from dataclasses import InitVar
from dataclasses import dataclass, field, asdict


Point = Tuple[float, float]
Line = Tuple[Point, ...]
Polygon = Line
MultiPolygon = Tuple[Polygon, ...]
# Postgis won't accept a lone Polygon
Coordinates = Union[Point, Line, MultiPolygon]


@dataclass(frozen=True)
class CRS:
  type       : Literal["name"]
  # TODO: What other properties are available?
  properties : Mapping[Literal["name"], str]


@dataclass(frozen=True)
class GeomImpl:
  type        : str
  coordinates : Coordinates
  crs         : CRS


@dataclass(frozen=True)
class Geom(Updateable):
  crs_name          : InitVar[str]
  type              : InitVar[str]
  coordinates       : InitVar[Coordinates]
  geom              : str = field(init=False)

  container_geom_id : Optional[int]

  def __post_init__(self,
                    crs_name : str,
                    type : str,
                    coordinates : Coordinates
                   ):
    crs = CRS(type = "name",
              properties = {"name": crs_name},
             )
    impl = GeomImpl(
      type = type,
      coordinates = coordinates,
      crs = crs
    )
    geom = json.dumps(impl, default=asdict)
    object.__setattr__(self, 'geom', geom)


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

