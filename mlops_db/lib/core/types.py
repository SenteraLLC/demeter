from typing import Optional, Union, Mapping, Literal, Sequence, Tuple, Generator
from datetime import date

from ..util.types_protocols import Table, Updateable, Detailed
from ..util.details import Details

from dataclasses import dataclass
from abc import ABC

class Properties(Details):
  def __init__(self, properties : Mapping[Literal["name"], str]):
    super().__init__(properties) # type: ignore


@dataclass(frozen=True)
class CRS(Table):
  type       : Literal["name"]
  properties : Properties


Point = Tuple[float, float]
Line = Tuple[Point, ...]
# Postgis only wants MultiPolygon, not Polygon
Polygon = Line
MultiPolygon = Tuple[Polygon, ...]
Coordinates = Union[Point, Line, MultiPolygon]


@dataclass(frozen=True)
class Geometry(Table):
  type        : str
  coordinates : Coordinates
  crs         : CRS

@dataclass(frozen=True)
class Geom(Updateable):
  container_geom_id : Optional[int]
  geom              : Geometry

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

from typing import Protocol

@dataclass(frozen=True)
class Key(GeoSpatialKey, TemporalKey):
  geospatial_key_id : int
  temporal_key_id   : int

def foo(k : Key) -> int:
  x = k.geom_id
  return 1

KeyGenerator = Generator[Key, None, None]

AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]

