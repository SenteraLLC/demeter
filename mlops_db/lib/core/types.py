from typing import Optional, Union, Dict, Literal, List, Tuple, Generator, TypedDict
from datetime import date

from ..util.types_protocols import Table, Updateable, Detailed

from dataclasses import dataclass
from abc import ABC

class CRS(TypedDict):
  type       : Literal["name"]
  properties : Dict[Literal["name"], str]

TwoDimensionalPolygon = List[List[Tuple[float, float]]]
# TODO: Add other coordinate types
Coordinates = Union[TwoDimensionalPolygon]


class Geometry(TypedDict):
  type        : str
  coordinates : Coordinates
  crs         : CRS

@dataclass
class Geom(Updateable):
  container_geom_id : Optional[int]
  geom              : Geometry

@dataclass
class InsertableGeom(Table):
  container_geom_id : Optional[int]
  geom              : str

@dataclass
class Owner(Table):
  owner : str

@dataclass
class Grower(Detailed):
  owner_id    : int
  farm        : str
  external_id : Optional[str]

@dataclass
class Field(Table):
  owner_id    : int
  geom_id     : int
  year        : Optional[int]
  grower_id   : Optional[int]
  sentera_id  : Optional[str]
  external_id : Optional[str]

@dataclass
class GeoSpatialKey(Table):
  geom_id  : int
  field_id : Optional[int]

@dataclass
class TemporalKey(Table):
  start_date : date
  end_date   : date

from typing import Protocol

@dataclass
class Key(GeoSpatialKey, TemporalKey):
  geospatial_key_id : int
  temporal_key_id   : int

def foo(k : Key) -> int:
  x = k.geom_id
  return 1

KeyGenerator = Generator[Key, None, None]

AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]

