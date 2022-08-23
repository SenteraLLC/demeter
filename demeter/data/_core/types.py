from typing import Optional, Union, Generator
from typing import Literal, Mapping, Tuple
from typing import cast

import json
from datetime import date, datetime
from dataclasses import InitVar
from dataclasses import dataclass, field, asdict

from ... import db

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
class Geom(db.Table):
  crs_name          : InitVar[str]
  type              : InitVar[str]
  coordinates       : InitVar[Coordinates]
  geom              : str = field(init=False)

  container_geom_id : Optional[db.TableId]

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
class Owner(db.Table):
  owner : str

from ...db import Detailed
@dataclass(frozen=True)
class Grower(Detailed):
  owner_id    : db.TableId
  farm        : str
  external_id : Optional[str]

@dataclass(frozen=True)
class Field(db.Table):
  owner_id    : db.TableId
  geom_id     : db.TableId
  year        : Optional[int]
  grower_id   : Optional[db.TableId]
  external_id : Optional[str]
  sentera_id  : Optional[str]      = None
  created     : Optional[datetime] = None

@dataclass(frozen=True)
class GeoSpatialKey(db.Table):
  geom_id  : db.TableId
  field_id : Optional[db.TableId]

@dataclass(frozen=True)
class TemporalKey(db.Table):
  start_date : date
  end_date   : date

@dataclass(frozen=True)
class KeyIds(db.Table):
  geospatial_key_id : db.TableId
  temporal_key_id   : db.TableId

@dataclass(frozen=True, order=True)
class Key(KeyIds, GeoSpatialKey, TemporalKey):
  pass

@dataclass(frozen=True)
class CropType(db.TypeTable, db.Detailed):
  species     : str
  cultivar    : Optional[str]
  parent_id_1 : Optional[db.TableId]
  parent_id_2 : Optional[db.TableId]

@dataclass(frozen=True)
class CropStage(db.TypeTable):
  crop_stage : str

@dataclass(frozen=True)
class PlantHarvestKey(db.TableKey):
  field_id      : db.TableId
  crop_type_id  : db.TableId
  geom_id       : db.TableId

@dataclass(frozen=True)
class PlantingKey(PlantHarvestKey):
  pass

@dataclass(frozen=True)
class HarvestKey(PlantHarvestKey):
  pass

@dataclass(frozen=True)
class _PlantHarvest(db.Detailed):
  completed : Optional[date]

@dataclass(frozen=True)
class Planting(PlantingKey, _PlantHarvest):
  pass

@dataclass(frozen=True)
class Harvest(HarvestKey, _PlantHarvest):
  pass

@dataclass(frozen=True)
class CropProgressKey(db.TableKey):
  field_id         : db.TableId
  crop_type_id     : db.TableId
  planting_geom_id : db.TableId
  crop_stage_id    : db.TableId
  geom_id          : Optional[db.TableId]

@dataclass(frozen=True)
class CropProgress(CropProgressKey):
  day             : Optional[date]
#  x               : InitVar[int]

@dataclass(frozen=True)
class ReportType(db.TypeTable):
  report : str


