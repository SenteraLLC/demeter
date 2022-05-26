from typing import TypedDict, Optional, Union, Dict, Literal, Tuple, Generator
from datetime import date

from ..database.types_protocols import Detailed, TypeTable, TableKey

from dataclasses import dataclass


@dataclass(frozen=True)
class LocalValue(Detailed):
  geom_id        : int
  field_id       : Optional[int]
  unit_type_id   : int
  quantity       : float
  local_group_id : Optional[int]
  acquired       : Optional[date]

AnyDataTable = Union[LocalValue]

@dataclass(frozen=True)
class LocalType(TypeTable):
  type_name      : str
  type_category  : Optional[str]

@dataclass(frozen=True)
class UnitType(TypeTable):
  unit          : str
  local_type_id : int

@dataclass(frozen=True)
class ReportType(TypeTable):
  report : str

@dataclass(frozen=True)
class LocalGroup(TypeTable):
  group_name     : str
  group_category : Optional[str]

@dataclass(frozen=True)
class CropType(TypeTable, Detailed):
  species     : str
  cultivar    : Optional[str]
  parent_id_1 : Optional[int]
  parent_id_2 : Optional[int]

@dataclass(frozen=True)
class CropStage(TypeTable):
  crop_stage : str

AnyTypeTable = Union[UnitType, CropType, CropStage, ReportType, LocalGroup]


class PlantHarvestKey(TableKey):
  field_id      : int
  crop_type_id  : int
  geom_id       : int

@dataclass(frozen=True)
class PlantingKey(PlantHarvestKey):
  pass

@dataclass(frozen=True)
class HarvestKey(PlantHarvestKey):
  pass

@dataclass(frozen=True)
class PlantHarvest(Detailed):
  completed : Optional[date]

@dataclass(frozen=True)
class Planting(PlantingKey, PlantHarvest):
  pass

@dataclass(frozen=True)
class Harvest(HarvestKey, PlantHarvest):
  pass

@dataclass(frozen=True)
class CropProgressKey(TableKey):
  field_id         : int
  crop_type_id     : int
  planting_geom_id : int
  geom_id          : Optional[int]
  crop_stage_id    : int

@dataclass(frozen=True)
class CropProgress(CropProgressKey):
  day             : Optional[date]

AnyKeyTable = Union[Planting, Harvest, CropProgress]


