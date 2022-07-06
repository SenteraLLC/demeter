from typing import Optional, Union, Generator
from typing import Literal, Mapping, Tuple
from typing import cast

import json
from datetime import date, datetime
from dataclasses import InitVar
from dataclasses import dataclass, field, asdict

from ...db import Table, Detailed
from ...db import TableId as TableId


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
class Geom(Table):
  crs_name          : InitVar[str]
  type              : InitVar[str]
  coordinates       : InitVar[Coordinates]
  geom              : str = field(init=False)

  container_geom_id : Optional[TableId]

  def __post_init__(self,
                    crs_name : str,
                    type : str,
                    coordinates : Coordinates
                   ) -> None:
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

#reveal_type(Detailed)
@dataclass(frozen=True)
class Grower(Detailed):
  owner_id    : TableId
  farm        : str
  external_id : Optional[str]
#reveal_type(Grower)

@dataclass(frozen=True)
class Field(Table):
  owner_id    : TableId
  geom_id     : TableId
  year        : Optional[int]
  grower_id   : Optional[TableId]
  external_id : Optional[str]
  sentera_id  : Optional[str]      = None
  created     : Optional[datetime] = None

@dataclass(frozen=True)
class GeoSpatialKey(Table):
  geom_id  : TableId
  field_id : Optional[TableId]

@dataclass(frozen=True)
class TemporalKey(Table):
  start_date : date
  end_date   : date

@dataclass(frozen=True)
class _KeyIds(Table):
  geospatial_key_id : TableId
  temporal_key_id   : TableId

@dataclass(frozen=True, order=True)
class Key(_KeyIds, GeoSpatialKey, TemporalKey):
  pass

KeyGenerator = Generator[Key, None, None]

AnyDataTable = Union[Geom, Owner, Grower, Field, GeoSpatialKey, TemporalKey]

