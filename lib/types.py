from datetime import datetime, date
from typing import TypedDict, Literal, Dict, List, Any, Optional, Tuple, Union, Type

import json
import jsonschema

from enum import Enum

### Table Protocols

class Table(TypedDict):
  pass

class Updateable(Table):
  last_updated : Optional[datetime]

Details = Optional[Dict[str, Any]]

class Detailed(Updateable):
  details : Details


### Type Tables

class TypeTable(Table):
  pass

class UnitType(TypeTable):
  unit          : str
  local_type_id : int

class LocalType(TypeTable):
  type_name      : str
  type_category  : Optional[str]

# TODO: NHA Rate?
# TODO: Cultivar vs Variety?
class CropType(TypeTable):
  species     : str
  cultivar    : Optional[str]
  #parent_id_1 : Optional[int]
  #parent_id_2 : Optional[int]

class CropStage(TypeTable):
  crop_stage : str

class ReportType(TypeTable):
  report : str

class LocalGroup(TypeTable):
  group_name     : str
  group_category : Optional[str]


# Data Tables

class CRS(TypedDict):
  type       : Literal["name"]
  properties : Dict[Literal["name"], str]

TwoDimensionalPolygon = List[List[Tuple[float, float]]]
# TODO: Add other types to a union
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
  owner_id  : int
  year      : int
  geom_id   : int
  grower_id : Optional[int]


class LocalValue(Detailed):
  geom_id        : int
  field_id       : Optional[int]
  unit_type_id   : int
  quantity       : float
  local_group_id : Optional[int]


class LocalParameter(Table):
  local_value_id : int
  acquired       : date



class Key(TypedDict):
  pass

class PlantHarvestKey(Key):
  field_id      : int
  crop_type_id  : int
  geom_id       : int


class PlantingKey(PlantHarvestKey):
  pass


class HarvestKey(PlantHarvestKey):
  pass

AnyKey = Union[PlantingKey, HarvestKey]

class PlantHarvest(Detailed):
  completed : Optional[date]

class Planting(PlantingKey, PlantHarvest):
  pass

class Harvest(HarvestKey, PlantHarvest):
  pass

class CropProgressKey(Key):
  field_id         : int
  crop_type_id     : int
  planting_geom_id : int
  geom_id          : Optional[int]
  crop_stage_id    : int

class CropProgress(CropProgressKey):
  day             : Optional[date]


# HTTP Type

class HTTPVerb(Enum):
  GET    = 1
  PUT    = 2
  POST   = 3
  DELETE = 4


class RequestBodySchema(object):
  def __init__(self : Any, schema : Any):
    jsonschema.Draft7Validator.check_schema(schema)
    self.schema = schema


class HTTPType(TypedDict):
  type_name           : str
  verb                : HTTPVerb
  uri                 : str
  # TODO: Required vs optional?
  uri_parameters      : Optional[List[str]]
  request_body_schema : Optional[RequestBodySchema]


type_table_lookup = {
  UnitType   : "unit_type",
  LocalType  : "local_type",
  CropType   : "crop_type",
  CropStage  : "crop_stage",
  ReportType : "report_type",
  LocalGroup : "local_group",
  HTTPType   : "http_type",
}

AnyTypeTable = Union[UnitType, LocalType, CropType, CropStage, ReportType, HTTPType]


data_table_lookup = {
  Geom            : "geom",
  Owner           : "owner",
  Grower          : "grower",
  Field           : "field",
  LocalValue      : "local_value",
  LocalParameter  : "local_parameter",
}

AnyDataTable = Union[Geom, Owner, Grower, Field, LocalValue, LocalParameter, LocalGroup]

id_table_lookup = type_table_lookup.copy()
id_table_lookup.update(data_table_lookup)
AnyIdTable = Union[AnyTypeTable, AnyDataTable]

key_table_lookup = {
  Planting     : ("planting", PlantingKey),
  Harvest      : ("harvest",  HarvestKey),
  CropProgress : ("crop_progress", CropProgressKey),
}
AnyKeyTable = Union[Planting, Harvest, CropProgress]


# TODO: More complex selections
# Full field queries with planted info
# As planted objects with nested stages and crops etc

# Model Run Tables

class SampleSet(Table):
  geom_id    : int
  created    : datetime
  start_date : date
  end_date   : date


class SampleSetRecord(Table):
  sample_set_id : int
  sample_id     : int


class Model(Detailed):
  model_name    : str
  major         : int
  minor         : int
  patch         : Optional[int]
  created       : datetime
  sample_set_id : int


class ModelFeature(Table):
  model_id        : int
  feature_type_id : int


class Report(Detailed):
  sample_id      : int
  report_type_id : int
  created        : datetime
