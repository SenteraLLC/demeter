from datetime import datetime, date
from typing import TypedDict, Literal, Dict, List, Any, Optional, Tuple, Union, Type, Generator, Set, Sequence

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
  owner_id    : int
  year        : int
  geom_id     : int
  grower_id   : Optional[int]
  sentera_id  : Optional[str]
  external_id : Optional[str]


class LocalValue(Detailed):
  geom_id        : int
  field_id       : Optional[int]
  unit_type_id   : int
  quantity       : float
  local_group_id : Optional[int]
  acquired       : date


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


class TableKey(TypedDict):
  pass

class PlantHarvestKey(TableKey):
  field_id      : int
  crop_type_id  : int
  geom_id       : int

class PlantingKey(PlantHarvestKey):
  pass


class HarvestKey(PlantHarvestKey):
  pass

class PlantHarvest(Detailed):
  completed : Optional[date]

class Planting(PlantingKey, PlantHarvest):
  pass

class Harvest(HarvestKey, PlantHarvest):
  pass

class CropProgressKey(TableKey):
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

def stringToHTTPVerb(s : str):
  return HTTPVerb[s.upper()]

class RequestBodySchema(object):
  def __init__(self : Any, schema : Any):
    jsonschema.Draft7Validator.check_schema(schema)
    self.schema = schema

class HTTPType(TypeTable):
  type_name           : str
  verb                : HTTPVerb
  uri                 : str
  # TODO: Required vs optional?
  uri_parameters      : Optional[List[str]]
  request_body_schema : Optional[RequestBodySchema]



# TODO: Type Versions?
class S3Type(TypeTable):
  type_name : str

class S3TypeDataFrame(Table):
  driver       : str
  has_geometry : bool

S3SubType = Union[S3TypeDataFrame]

class TaggedS3SubType(TypedDict):
  tag   : Type[S3SubType]
  value : S3SubType

s3_subtype_table_lookup = {
  S3TypeDataFrame : "s3_type_dataframe",
}


class S3Output(Table):
  function_id : int
  s3_type_id  : int


class S3Object(Table):
  key         : str
  bucket_name : str
  s3_type_id  : int


class S3ObjectKey(Table):
  s3_object_id      : int
  geospatial_key_id : int
  temporal_key_id   : int


# Function Signature Tables

class FunctionType(TypeTable):
  function_type_name    : str
  function_subtype_name : Optional[str]

class Function(Detailed):
  function_name    : str
  major            : int
  #minor            : int
  function_type_id : int
  created          : datetime

class FunctionId(Table):
  function_id : int

class Parameter(FunctionId):
  pass

class LocalParameter(Parameter):
  local_type_id       : int

class HTTPParameter(Parameter, Detailed):
  http_type_id        : int

class S3InputParameter(Parameter, Detailed):
  s3_type_id              : int

class S3OutputParameter(Parameter, Detailed):
  s3_output_parameter_name : str
  s3_type_id               : int

class KeywordType(Enum):
  STRING  = 1
  INTEGER = 2
  FLOAT   = 3
  JSON    = 4

class Keyword(TypedDict):
  keyword_name : str
  keyword_type : KeywordType

class KeywordParameter(Keyword, Parameter):
  pass


S3TypeSignature = Tuple[S3Type, Optional[S3TypeDataFrame]]

class FunctionSignature(TypedDict):
  name           : str
  major          : int
  local_inputs   : List[LocalType]
  keyword_inputs : List[Keyword]
  s3_inputs      : List[S3TypeSignature]
  http_inputs    : List[HTTPType]
  s3_outputs     : List[S3TypeSignature]

class Execution(Table):
  function_id  : int

class ExecutionKey(TypedDict):
  execution_id      : int
  geospatial_key_id : int
  temporal_key_id   : int

class Argument(FunctionId):
  execution_id      : int

class LocalArgument(Argument):
  local_type_id : int
  number_of_observations : int

class HTTPArgument(Argument):
  http_type_id : int
  number_of_observations : int

class S3InputArgument(Argument):
  s3_type_id   : int
  s3_object_id : int

class S3OutputArgument(Argument):
  s3_output_parameter_name : str
  s3_type_id               : int
  s3_object_id             : int

class KeywordArgument(Argument):
  keyword_name : str
  value_number : Optional[float]
  value_string : Optional[str]

class ExecutionArguments(TypedDict):
  local : List[LocalArgument]
  keyword : List[KeywordArgument]
  http : List[HTTPArgument]
  s3 : List[S3InputArgument]
  keys : List[Key]

ExecutionOutputs = Dict[Literal['s3'], List[S3OutputArgument]]

class ExecutionSummary(TypedDict):
  inputs  : ExecutionArguments
  outputs : ExecutionOutputs
  function_id         : int
  execution_id        : int


AnyTypeTable = Union[UnitType, LocalType, CropType, CropStage, ReportType, LocalGroup, HTTPType, S3Type, FunctionType]

type_table_lookup = {
  UnitType   : "unit_type",
  LocalType  : "local_type",
  CropType   : "crop_type",
  CropStage  : "crop_stage",
  ReportType : "report_type",
  LocalGroup : "local_group",
  HTTPType   : "http_type",
  S3Type     : "s3_type",
  S3TypeDataFrame : "s3_type_dataframe",
  FunctionType    : "function_type",
}


AnyDataTable = Union[Geom, Owner, Grower, Field, LocalValue, GeoSpatialKey, TemporalKey, S3Output, S3Object, Function, Execution]

data_table_lookup = {
  Geom              : "geom",
  Owner             : "owner",
  Grower            : "grower",
  Field             : "field",
  LocalValue        : "local_value",
  GeoSpatialKey     : "geospatial_key",
  TemporalKey       : "temporal_key",
  S3Output          : "s3_output",
  S3Object          : "s3_object",
  Function          : "function",
  Execution         : "execution",
}


AnyIdTable = Union[AnyTypeTable, AnyDataTable, S3SubType]
id_table_lookup = type_table_lookup.copy()
id_table_lookup.update(data_table_lookup)


key_table_lookup = {
  Planting     : ("planting", PlantingKey),
  Harvest      : ("harvest",  HarvestKey),
  CropProgress : ("crop_progress", CropProgressKey),
  S3ObjectKey  : ("s3_object_key", S3ObjectKey),
  S3TypeDataFrame : ("s3_type_dataframe", S3TypeDataFrame),
  LocalParameter : ("local_parameter", LocalParameter),
  HTTPParameter     : ("http_parameter", HTTPParameter),
  S3InputParameter  : ("s3_input_parameter", S3InputParameter),
  S3OutputParameter : ("s3_output_parameter", S3OutputParameter),
  KeywordParameter : ("keyword_parameter", KeywordParameter),
  ExecutionKey     : ("execution_key", ExecutionKey),
  LocalArgument    : ("local_argument", LocalArgument),
  HTTPArgument     : ("http_argument", HTTPArgument),
  KeywordArgument  : ("keyword_argument", KeywordArgument),
  S3InputArgument  : ("s3_input_argument", S3InputArgument),
  S3OutputArgument : ("s3_output_argument", S3OutputArgument),
}
AnyKeyTable = Union[Planting, Harvest, CropProgress, S3ObjectKey, LocalParameter, HTTPParameter, S3InputParameter, S3OutputParameter]



# TODO: More complex selections
# Full field queries with planted info
# As planted objects with nested stages and crops etc

# Model Run Tables

#class SampleSet(Table):
#  geom_id    : int
#  created    : datetime
#  start_date : date
#  end_date   : date
#
#
#class SampleSetRecord(Table):
#  sample_set_id : int
#  sample_id     : int


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



