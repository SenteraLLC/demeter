from typing import TypedDict, Optional, Union, Dict, Literal, List, Tuple, Generator
from datetime import date

from ..util.types_protocols import Detailed, TypeTable, TableKey


class LocalValue(Detailed):
  geom_id        : int
  field_id       : Optional[int]
  unit_type_id   : int
  quantity       : float
  local_group_id : Optional[int]
  acquired       : date

AnyDataTable = Union[LocalValue]

class LocalType(TypeTable):
  type_name      : str
  type_category  : Optional[str]

class UnitType(TypeTable):
  unit          : str
  local_type_id : int

class ReportType(TypeTable):
  report : str

class LocalGroup(TypeTable):
  group_name     : str
  group_category : Optional[str]

class CropType(TypeTable, Detailed):
  species     : str
  cultivar    : Optional[str]
  parent_id_1 : Optional[int]
  parent_id_2 : Optional[int]

class CropStage(TypeTable):
  crop_stage : str

AnyTypeTable = Union[UnitType, CropType, CropStage, ReportType, LocalGroup]


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

AnyKeyTable = Union[Planting, Harvest, CropProgress]


