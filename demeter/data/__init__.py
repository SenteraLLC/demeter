from typing import Union

from . import local
from . import core

AnyTypeTable = Union[local.types.AnyTypeTable]

AnyDataTable = Union[local.types.AnyDataTable, core.types.AnyDataTable, ]

AnyIdTable = Union[AnyTypeTable, AnyDataTable, ]

AnyKeyTable = Union[local.types.AnyKeyTable, ]

AnyTable = Union[AnyTypeTable, AnyDataTable, AnyIdTable, AnyKeyTable, ]



from .core import *
from .core.types import *

from .local import *
from .local.types import *

__all__ = [
  'local',
  'core',

  # core
  'getMaybeFieldId',
  'getMaybeOwnerId',
  'getMaybeGrowerId',
  'getMaybeGeoSpatialKeyId',
  'getMaybeTemporalKeyId',

  'getField',
  'getOwner',
  'getGeom',

  'insertField',
  'insertOwner',
  'insertGrower',
  'insertGeoSpatialKey',
  'insertTemporalKey',

  'insertOrGetGeoSpatialKey',
  'insertOrGetTemporalKey',
  'insertOrGetOwner',
  'insertOrGetGrower',
  'insertOrGetField',

  'getMaybeDuplicateGeom',
  'insertOrGetGeom',

  # core types
  'Point',
  'Line',
  'Polygon',
  'MultiPolygon',
  'Coordinates',
  'Geom',
  'Owner',
  'Grower',
  'Field',
  'GeoSpatialKey',
  'TemporalKey',
  'Key',

  # local
  'getMaybeLocalValue',
  'getMaybeUnitTypeId',
  'getMaybeLocalTypeId',
  'getMaybeLocalValueId',
  'getMaybeCropTypeId',
  'getMaybeCropStageId',
  'getMaybeReportTypeId',
  'getMaybeLocalGroupId',

  'getLocalType',

  'insertLocalValue',
  'insertUnitType',
  'insertLocalType',
  'insertCropType',
  'insertCropStage',
  'insertReportType',
  'insertLocalGroup',

  'insertPlanting',
  'insertHarvest',
  'insertCropProgress',

  'insertOrGetUnitType',
  'insertOrGetLocalType',
  'insertOrGetLocalValue',
  'insertOrGetCropType',
  'insertOrGetCropStage',
  'insertOrGetLocalGroup',

  'getMaybeDuplicateGeom',
  'insertOrGetGeom',

  # local types
  'LocalValue',

  'LocalType',
  'UnitType',
  'ReportType',
  'LocalGroup',
  'CropType',
  'CropStage',

  'PlantHarvestKey',
  'PlantingKey',
  'HarvestKey',
  'CropProgressKey',

  'Planting',
  'Harvest',
  'CropProgress',

]

