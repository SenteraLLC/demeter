from typing import TypedDict, Optional, Union, Dict, Literal, List, Tuple, Generator
from datetime import date

from ..util.types_protocols import Table, Updateable


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

class Geom(Updateable):
  container_geom_id : Optional[int]
  geom              : Geometry

class InsertableGeom(Table):
  container_geom_id : Optional[int]
  geom              : str

class Owner(Table):
  owner : str

class Grower(Table):
  farm  : str

class Field(Table):
  owner_id    : int
  geom_id     : int
  year        : Optional[int]
  grower_id   : Optional[int]
  sentera_id  : Optional[str]
  external_id : Optional[str]

class GeoSpatialKey(Table):
  geom_id  : int
  field_id : Optional[int]

class TemporalKey(Table):
  start_date : date
  end_date   : date

class Key(GeoSpatialKey, TemporalKey):
  geospatial_key_id : int
  temporal_key_id   : int

KeyGenerator = Generator[Key, None, None]

AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]
